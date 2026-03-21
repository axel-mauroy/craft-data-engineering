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
        tags=['craft_example']
    )
}}

/*
   9. MATÉRIALISATION INTENTIONNELLE
   --------------------------------------------------------------------------
   View        -> Légère, toujours fraîche, coûteuse à chaque lecture.
                  Idéale pour le staging.
   Table       -> Coût fixe à l'écriture, lecture rapide.
                  Idéale pour les marts consommés fréquemment.
   Incremental -> Pour les volumes > 10M lignes avec une clé temporelle claire.
                  Complexité accrue - à justifier explicitement.
*/

/*
   1. LA FORME (Lisibilité & CTEs)
   --------------------------------------------------------------------------
   Le SQL devient vite illisible. Le Craftsman utilise les CTEs (WITH) non 
   pas comme de simples sous-requêtes, mais comme des paragraphes logiques. 
   Chaque CTE doit avoir une responsabilité unique et un nom explicite en camelCase.
*/

WITH
clientsActifs AS (
    -- Responsabilité : Identifier les clients actifs sur l'année en cours
    SELECT 
        clientId,
        -- ...
    FROM {{ ref('stg_clients') }}
    /*
       8. NULL AWARENESS
       --------------------------------------------------------------------------
       ❌ ANTI-PATTERN : WHERE statut != 'ANNULE'
       Exclut les ANNULE mais exclut silencieusement les NULL - comportement
       contre-intuitif garanti en production.

       ✅ CRAFT PATTERN : Expliciter le traitement du NULL
    */
    WHERE (statut != 'ANNULE' OR statut IS NULL)
),

/*
   2. LE FOND (Plan d'exécution & I/O)
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
           7. WINDOW FUNCTIONS (Éviter la sous-requête corrélée)
           --------------------------------------------------------------------------
           ❌ ANTI-PATTERN : Sous-requête corrélée pour trouver la dernière commande
           Exécutée ligne par ligne - O(n²) sur les gros volumes.

           ✅ CRAFT PATTERN : Window function
           Un seul scan, le moteur calcule le rang en parallèle par partition.
        */
        ROW_NUMBER() OVER (
            PARTITION BY clientId 
            ORDER BY dateCommande DESC
        ) AS rang_commande
    FROM {{ ref('stg_commandes') }}
    /*
       3. PRÉDICATS SARGables (Search ARGument ABLE) & TYPAGE
       ----------------------------------------------------------------------
       ❌ ANTI-PATTERN : WHERE EXTRACT(YEAR FROM dateCommande) = 2024
       Force un Full Scan. Par ailleurs, `dateCommande >= '2024-01-01'` 
       utilise un cast implicite (dangereux sur BigQuery).

       ✅ CRAFT PATTERN : Filtre sur colonne brute avec typage explicite
       Garantit l'utilisation du partitionnement et des types robustes.
    */
    WHERE dateCommande >= DATE '2024-01-01'
      AND dateCommande < DATE '2025-01-01'
)

-- Assemblage Final (Le "Paragraph" principal)
SELECT
    /*
       4. CONTRAT DE DONNÉES & EXPLICITÉ
       ----------------------------------------------------------------------
       ❌ ANTI-PATTERN : SELECT * ou SELECT c.clientId, cmd.*
       Ceci crée une dépendance fragile. L'ajout d'une colonne en amont peut
       casser le modèle en aval (notamment dans les vues ou les UNION).

       ✅ CRAFT PATTERN : Contrat de données strict
       On nomme explicitement chaque colonne sélectionnée, et on préfixe
       toujours avec l'alias de la table pour éviter toute ambiguïté.
    */
    c.clientId,
    cmd.commandeId,
    cmd.montant,
    cmd.dateCommande
FROM commandesFiltrees cmd
/*
   5. CONSCIENCE DU FAN-OUT (JOINTURES)
   --------------------------------------------------------------------------
   ❌ ANTI-PATTERN : Joindre sans comprendre la cardinalité.
   Un LEFT JOIN n'est pas inoffensif : s'il y a des doublons à droite, 
   il va multiplier les lignes à gauche (Fan-out) et fausser les KPIs.
   Ex: `FROM clients LEFT JOIN commandes` va dissimuler une relation 1:N.

   ✅ CRAFT PATTERN : La table de gauche dicte le grain
   Ici, le modèle est une table de faits (Commandes). On part donc des commandes 
   et on enrichit avec le client (Relation N:1). Aucun risque de Fan-out.
   Si on voulait une dimension Client (1:N), il faudrait agréger les commandes.
*/
LEFT JOIN clientsActifs c
    ON cmd.clientId = c.clientId

/*
   6. UNION ALL vs UNION (PERFORMANCE)
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
