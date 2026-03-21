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
    c.clientId,
    cmd.montant
FROM clientsActifs c
INNER JOIN commandesFiltrees cmd
    ON c.clientId = cmd.clientId
