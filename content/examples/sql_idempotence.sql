/*
   ==========================================================================
   CRAFT PATTERN : L'IDEMPOTENCE & L'INCRÉMENTAL (BigQuery)
   ==========================================================================
   Définition de l'Idempotence : Exécuter un pipeline 1 fois ou 100 fois
   doit produire STRICTEMENT le même état final dans la base de données.
   Aucun doublon. Aucune donnée manquante.
   --------------------------------------------------------------------------
*/

{{
    config(
        materialized='incremental',
        /*
           1. LA STRATÉGIE INCRÉMENTALE (Le Secret BigQuery)
           ----------------------------------------------------------------------
           Par défaut, dbt utilise 'merge' (qui fait un UPDATE sur la unique_key).
           Sur des milliards de lignes, un MERGE consomme énormément de CPU.

           ✅ CRAFT PATTERN : 'insert_overwrite'
           Sur BigQuery, l'insert_overwrite est magique. Au lieu de chercher les 
           lignes une par une, dbt va écraser (REPLACE) physiquement les 
           partitions entières concernées par le run. C'est l'idempotence pure,
           extrêmement rapide et peu coûteuse.
        */
        incremental_strategy='insert_overwrite',
        partition_by={
            "field": "dateCommande",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by=['magasinId'],
        tags=['craft_example']
    )
}}

WITH sourceCommandes AS (
    SELECT
        commandeId,
        magasinId,
        dateCommande,
        montantTotal
    FROM {{ ref('stg_commandes') }}

    /*
       2. LE BLOC IS_INCREMENTAL() & LE "LOOKBACK WINDOW"
       --------------------------------------------------------------------------
       ❌ ANTI-PATTERN : WHERE dateCommande > (select max(date) from this)
       Si un run échoue à 23h59, ou qu'un système amont a du retard ("late 
       arriving facts"), vous allez rater des commandes.

       ✅ CRAFT PATTERN : La fenêtre de chevauchement (Lookback Window)
       On recalcule toujours les X derniers jours. Grâce à 'insert_overwrite',
       les partitions des 3 derniers jours seront proprement remplacées,
       réglant silencieusement le problème des données retardataires sans 
       créer de doublons.
    */
    {% if is_incremental() %}
        -- On utilise les variables de dbt pour éviter une sous-requête MAX() coûteuse
        -- _dbt_max_partition est une variable dbt générée automatiquement lors d'un insert_overwrite
        WHERE dateCommande >= DATE_SUB(DATE '{{ run_started_at.strftime("%Y-%m-%d") }}', INTERVAL 3 DAY)
    {% endif %}
),

commandesDedoublonnees AS (
    /*
       3. GESTION DES DOUBLONS À LA SOURCE
       --------------------------------------------------------------------------
       L'insert_overwrite protège des doublons entre deux runs dbt.
       MAIS il ne protège pas des doublons présents dans la donnée source du jour !

       ✅ CRAFT PATTERN : Le QUALIFY défensif
       On s'assure qu'une même commande ne sorte qu'une seule fois dans le run.
       On prend la version la plus récente si le système source a bégayé.
    */
    SELECT *
    FROM sourceCommandes
    -- S'il y a 2 lignes pour la même commandeId, on ne garde que la dernière reçue
    -- (Ici on suppose l'existence d'une colonne technique _syncedAt)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY commandeId ORDER BY dateCommande DESC) = 1
)

SELECT
    commandeId,
    magasinId,
    dateCommande,
    montantTotal
FROM commandesDedoublonnees

/*
   ==========================================================================
   CRAFT PATTERN (VANGUARD) : LE MICROBATCH (dbt 1.9+)
   ==========================================================================
   Depuis dbt core 1.9, la matérialisation 'microbatch' rend obsolète la 
   gestion manuelle du bloc 'is_incremental()' pour les flux chronologiques.
   C'est l'évolution ultime du Data Craftsmanship vers la simplicité extrême.
   --------------------------------------------------------------------------

{{
    config(
        materialized='microbatch',
        -- Le moteur dbt découpe automatiquement les runs en lots journaliers
        batch_size='day',
        -- Le système filtre nativement la source et cible la bonne partition,
        -- sans aucune clause temporelle dans notre SQL.
        event_time='dateCommande',
        begin=DATE '2024-01-01',
        cluster_by=['magasinId'],
        tags=['craft_example']
    )
}}

SELECT
    commandeId,
    magasinId,
    dateCommande,
    montantTotal
FROM {{ ref('stg_commandes') }}
-- 💡 QUALIFY ROW_NUMBER() reste pertinent ici pour gérer les doublons intra-batch !

-- ✅ RÉSULTAT CRAFT : Idempotence Native & Pureté du Code
-- Le code redevient aussi simple qu'une vue logique, la complexité physique 
-- est entièrement déléguée au moteur dbt qui gère les REPLACE par partition.
*/
