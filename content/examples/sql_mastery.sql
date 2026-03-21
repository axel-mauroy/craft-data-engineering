/*
   ==========================================================================
   CRAFT PATTERN : SQL MASTERY & LISIBILITÉ
   ==========================================================================
   Le cœur du Data Craftsmanship réside dans la maîtrise du SQL. Un codeur 
   SQL junior se contente d'obtenir le bon résultat. Un Data Craftsman écrit du 
   SQL en ayant une image mentale de la façon dont le moteur (BigQuery, Snowflake) 
   va physiquement allouer la mémoire et lire les disques.
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
    WHERE statut = 'ACTIF'
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
        dateCommande
    FROM {{ ref('stg_commandes') }}
    /*
       3. PRÉDICATS SARGables (Search ARGument ABLE)
       ----------------------------------------------------------------------
       ❌ ANTI-PATTERN : WHERE EXTRACT(YEAR FROM dateCommande) = 2024
       Ceci force le moteur à appliquer la fonction sur chaque ligne, 
       ce qui invalide l'utilisation de l'index ou de la partition (Full Scan).

       ✅ CRAFT PATTERN : Filtre sur la colonne brute
       Ceci permet d'utiliser le partitionnement ou clustering natif.
    */
    WHERE dateCommande >= '2024-01-01'
      AND dateCommande < '2025-01-01'
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
FROM clientsActifs c
/*
   5. CONSCIENCE DU FAN-OUT (JOINTURES)
   --------------------------------------------------------------------------
   ❌ ANTI-PATTERN : Joindre sans comprendre la cardinalité.
   Un LEFT JOIN n'est pas inoffensif : s'il y a des doublons à droite, 
   il va multiplier les lignes à gauche (Fan-out) et fausser les KPIs.

   ✅ CRAFT PATTERN : Garantir l'unicité
   Le Craftsman s'assure toujours que la clé de jointure de la table de droite
   est unique (1:1 ou N:1). Si c'est 1:N, on agrège avant de joindre.
*/
LEFT JOIN commandesFiltrees cmd
    ON c.clientId = cmd.clientId

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
