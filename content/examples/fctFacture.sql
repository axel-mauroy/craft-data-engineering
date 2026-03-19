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
        tags = ['domain_sales']
    )
}}

WITH sourceFactures AS (
    SELECT 
        factureId,
        clientId,
        magasinId,
        CAST(date_creation AS DATE) AS dateCreation, 
        statut_facture AS statutFacture,             
        montant_ht AS montantHt                      
    FROM {{ source('raw_sales', 'raw_factures') }}
    {% if is_incremental() %}
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
        f.montantHt,
        COALESCE(l.quantiteTotaleArticles, 0) AS quantiteTotaleArticles,
        COALESCE(l.montantTvaTotal, 0) AS montantTvaTotal,
        (f.montantHt + COALESCE(l.montantTvaTotal, 0)) AS montantTtc
    FROM sourceFactures f
    LEFT JOIN sourceLignes l 
        ON f.factureId = l.factureId
)

SELECT * FROM facturesEnrichies
