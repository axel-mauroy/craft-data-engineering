/*
   ==========================================================================
   CRAFT PATTERN : SQL MASTERY & LISIBILITÉ
   ==========================================================================
   Le cœur du Data Craftsmanship réside dans la maîtrise du SQL. Un codeur 
   SQL junior se contente d'obtenir le bon résultat. Un Data Craftsman écrit du 
   SQL en ayant une image mentale de la façon dont le moteur (BigQuery, Snowflake) 
   va physiquement allouer la mémoire et lire les disques.
*/

{{
    config(
        materialized='table',
        partition_by={
            "field": "dateCommande",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by=['clientId'],
        tags=['craft_example']
    )
}}

/*
   1. LE CONTRAT PHYSIQUE (Matérialisation & Stockage)
   --------------------------------------------------------------------------
   Un modèle dbt n'est jamais terminé sans définir son empreinte physique.
   Le Craftsmanship impose de gérer l'I/O et le coût de requêtage.
   
   View        -> Légère, toujours fraîche. Idéale pour le staging.
   Table       -> Coût fixe à l'écriture, lecture très rapide.
   Partition   -> Divise la donnée par bloc temporel (ex: par jour).
                  Filtre drastiquement le volume scanné.
   Clustering  -> Trie la donnée à l'intérieur de chaque partition.
                  Accélère les jointures et les agrégations (ex: clientId).
*/

/*
   2. LA FORME (Lisibilité & CTEs)
   --------------------------------------------------------------------------
   Le SQL devient vite illisible. Le Craftsman utilise les CTEs (WITH) non 
   pas comme de simples sous-requêtes, mais comme des paragraphes logiques. 
   Chaque CTE doit avoir une responsabilité unique et un nom explicite en camelCase.
*/

WITH
clientsActifs AS (
    -- Responsabilité : Identifier les clients actifs et ramener leurs axes d'analyse
    SELECT 
        clientId,
        segmentFidelite -- ✅ CRAFT : On ramène un vrai attribut métier pour justifier la jointure
    FROM {{ ref('stg_clients') }}
    /*
       3. NULL AWARENESS
       --------------------------------------------------------------------------
       ❌ ANTI-PATTERN : WHERE statut != 'ANNULE'
       Exclut les ANNULE mais exclut silencieusement les NULL - comportement
       contre-intuitif garanti en production.

       ✅ CRAFT PATTERN : Expliciter le traitement du NULL
    */
    WHERE (statut != 'ANNULE' OR statut IS NULL)
),

/*
   4. LE FOND (Plan d'exécution & I/O)
   --------------------------------------------------------------------------
   Règle d'or : On filtre le plus tôt possible dans la première CTE pour 
   réduire la taille des données en mémoire avant le premier JOIN. 
   Pensez toujours I/O (Entrées/Sorties) et CPU.
*/

commandesFiltrees AS (
    SELECT
        commandeId,
        clientId,
        montant,
        dateCommande,
        /*
           5. WINDOW FUNCTIONS & QUALIFY (L'élégance BigQuery / Snowflake)
           --------------------------------------------------------------------------
           ✅ CRAFT PATTERN : Window function
           Calcul du rang de la commande. Utile en aval pour filtrer la première
           ou la dernière commande d'un client sans repasser par des sous-requêtes.
        */
        ROW_NUMBER() OVER (
            PARTITION BY clientId 
            ORDER BY dateCommande DESC
        ) AS rangCommande -- ✅ Typage camelCase respecté
    FROM {{ ref('stg_commandes') }}
    /*
       6. PRÉDICATS SARGables (Search ARGument ABLE) & TYPAGE
       ----------------------------------------------------------------------
       ❌ ANTI-PATTERN : WHERE EXTRACT(YEAR FROM dateCommande) = 2024
       Force un Full Scan. Par ailleurs, `dateCommande >= '2024-01-01'` 
       utilise un cast implicite (dangereux sur BigQuery).

       ✅ CRAFT PATTERN : Filtre sur colonne brute avec typage explicite
       Garantit l'utilisation du partitionnement et des types robustes.
    */
    WHERE dateCommande >= DATE '2024-01-01'
      AND dateCommande < DATE '2025-01-01'
    /*
       💡 Note Expert BigQuery : Si ce modèle devait STRICTEMENT n'exposer 
       que la dernière commande par client, c'est ici qu'on ajouterait :
       -- QUALIFY rangCommande = 1
    */
)

-- Assemblage Final (Le "Paragraph" principal)
SELECT
    /*
       7. CONTRAT DE DONNÉES & EXPLICITÉ
       ----------------------------------------------------------------------
       ❌ ANTI-PATTERN : SELECT * ou SELECT c.clientId, cmd.*
       Ceci crée une dépendance fragile. L'ajout d'une colonne en amont peut
       casser le modèle en aval (notamment dans les vues ou les UNION).

       ✅ CRAFT PATTERN : Contrat de données strict
       On nomme explicitement chaque colonne sélectionnée, et on préfixe
       toujours avec l'alias de la table pour éviter toute ambiguïté.
    */
    cmd.commandeId,
    cmd.dateCommande,
    cmd.montant,
    cmd.rangCommande,   -- ✅ Justifie le coût du ROW_NUMBER()
    c.clientId,
    c.segmentFidelite   -- ✅ Justifie le LEFT JOIN
FROM commandesFiltrees cmd
/*
   8. CONSCIENCE DU FAN-OUT (JOINTURES)
   --------------------------------------------------------------------------
   ❌ ANTI-PATTERN : Joindre sans comprendre la cardinalité.
   Un LEFT JOIN n'est pas inoffensif : s'il y a des doublons à droite, 
   il va multiplier les lignes à gauche (Fan-out) et fausser les KPIs.
   Ex: `FROM clients LEFT JOIN commandes` va dissimuler une relation 1:N.

   ✅ CRAFT PATTERN : La table de gauche dicte le grain (Commandes)
   Enrichissement avec la dimension (Relation N:1). Aucun risque de Fan-out.
   Si on voulait une dimension Client (1:N), il faudrait agréger les commandes.
*/
LEFT JOIN clientsActifs c
    ON cmd.clientId = c.clientId

/*
   9. UNION ALL vs UNION (PERFORMANCE)
   --------------------------------------------------------------------------
   ❌ ANTI-PATTERN : Utiliser UNION par défaut
   UNION force le moteur à faire un DISTINCT global caché (Tri + Dédoublonnage),
   ce qui détruit les performances sur des gros volumes.

   ✅ CRAFT PATTERN : Utiliser UNION ALL
   Si les requêtes sont distinctes par nature, ou si les doublons sont gérés,
   on utilise UNION ALL pour concaténer les blocs sans pénalité CPU.
*/
-- UNION ALL
-- SELECT ...
