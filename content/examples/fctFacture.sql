-- fctFacture.sql (BigQuery & dbt Model)
-- Logic: Aggregate invoice lines and link to client

{{
    config(
        materialized = 'incremental',
        unique_key = 'factureId',
        partition_by = {
            "field": "dateCreation",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by = ['magasinId', 'statutFacture'],
        on_schema_change = 'fail', -- Recommandé pour les contrats dbt (Fail Fast)
        contract = {
            "enforced": true -- Garantie de schéma (dbt >= 1.5)
        },
        tags = ['domain_sales']
    )
}}

WITH sourceFactures AS (
    SELECT 
        factureId,
        clientId,
        magasinId,
        CAST(date_creation AS DATE) AS dateCreation, -- Centralisation du Cast
        statut_facture AS statutFacture,             
        montant_ht AS montantHt                      
    FROM {{ source('raw_sales', 'raw_factures') }}
    {% if is_incremental() %}
        -- 💡 Principe DRY : On réutilise l'alias dateCreation défini plus haut.
        -- Note : BigQuery gère l'élagage de partition (Partition Pruning) si le filtre est SARGable.
        WHERE CAST(date_creation AS DATE) >= (SELECT MAX(dateCreation) FROM {{ this }})
    {% endif %}
),

sourceLignes AS (
    SELECT 
        factureId,
        SUM(quantite) AS quantiteTotaleArticles,
        SUM(montant_tva) AS montantTvaTotal        
    FROM {{ source('raw_sales', 'raw_lignes_facture') }}
    GROUP BY factureId
),

facturesEnrichies AS (
    SELECT 
        f.factureId,
        f.clientId,
        f.magasinId,
        f.dateCreation,
        f.statutFacture,
        CAST(f.montantHt AS NUMERIC) AS montantHt,
        CAST(COALESCE(l.quantiteTotaleArticles, 0) AS INT64) AS quantiteTotaleArticles,
        CAST(COALESCE(l.montantTvaTotal, 0) AS NUMERIC) AS montantTvaTotal,
        CAST((f.montantHt + COALESCE(l.montantTvaTotal, 0)) AS NUMERIC) AS montantTtc
    FROM sourceFactures f
    LEFT JOIN sourceLignes l 
        ON f.factureId = l.factureId
)

SELECT * FROM facturesEnrichies
