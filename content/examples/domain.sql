/* 
   ==========================================================================
   ARCHITECTURE PATTERN : ISOLATION DU DOMAINE (Agnosticisme Technique)
   ==========================================================================
   Ce document illustre comment séparer la logique métier pure de l'infrastructure
   BigQuery/dbt pour garantir la testabilité et la robustesse de vos pipelines.
*/

-- ❌ L'ANTI-PATTERN : Logique couplée au moteur technique
-- Pourquoi c'est fragile : La règle métier est noyée dans l'implémentation SQL.
-- Impossible à tester unitairement sans charger la base de données.

SELECT 
    f.factureId,
    c.clientId,
    -- ⚠️ RÈGLE MÉTIER NOYÉE : Si le seuil change, on doit modifier le modèle SQL
    CASE 
        WHEN c.statutCarteMaison = 'PREMIUM' AND f.montantTotal > 1000 THEN f.montantTotal * 0.90
        WHEN c.statutCarteMaison = 'STANDARD' AND f.montantTotal > 500 THEN f.montantTotal * 0.95
        ELSE f.montantTotal 
    END AS montantApresRemise
FROM `lm-data-prd.prd_sales_domain.factures` f
LEFT JOIN `lm-data-prd.prd_customer_domain.clients` c ON f.clientId = c.clientId
WHERE DATE(f.dateCreation) = CURRENT_DATE(); -- Contrainte d'infrastructure


/* 
   --------------------------------------------------------------------------
   ✅ LA VISION "CRAFT" : L'isolation du Domaine
   --------------------------------------------------------------------------
   Le principe "Domain First" : la règle métier doit pouvoir être testée en 
   isolation complète (millisecondes) avant même d'écrire une ligne de SQL.
*/

-- Étape 1 : Le Domaine Pur (Conceptuel)
-- En Python, on l'isolerait dans une fonction pure sans aucune dépendance.

/*
def calculer_remise_fidelite(statut_carte: str, montant_total: float) -> float:
    # Agnostique : ne sait pas s'il tourne sur Spark, dbt ou BigQuery.
    if statut_carte == 'PREMIUM' and montant_total > 1000:
        return montant_total * 0.90
    if statut_carte == 'STANDARD' and montant_total > 500:
        return montant_total * 0.95
    return montant_total
*/


-- Étape 2 : Implémentation dbt Expert (Macro isolée)
-- On encapsule la règle métier dans une Macro Jinja agnostique.
-- Fichier: macros/domain_sales/calculer_remise.sql

{% macro calculer_remise(statutCarte, montant) %}
    CASE 
        WHEN {{ statutCarte }} = 'PREMIUM' AND {{ montant }} > 1000 THEN {{ montant }} * 0.90
        WHEN {{ statutCarte }} = 'STANDARD' AND {{ montant }} > 500 THEN {{ montant }} * 0.95
        ELSE {{ montant }}
    END
{% endmacro %}


-- Étape 3 : Le Test Unitaire (Indépendant de l'infra)
-- Avec dbt >= 1.8, nous testons la logique métier sans cluster BigQuery.
-- Fichier: models/sales/sales.yml

/*
unit_tests:
  - name: test_regle_remise_premium
    model: fctFacturesEnrichies
    given:
      - input: ref('stg_factures')
        rows:
          - {factureId: 1, montantTotal: 1500}
      - input: ref('stg_clients')
        rows:
          - {clientId: 'C1', statutCarteMaison: 'PREMIUM'}
    expect:
      rows:
        - {factureId: 1, montantApresRemise: 1350}
*/


-- Étape 4 : L'Infrastructure (Le modèle d'assemblage)
-- Le modèle dbt se contente de lier les ports et adapters.
-- Fichier: models/sales/marts/fctFacturesEnrichies.sql

SELECT 
    f.factureId,
    c.clientId,
    -- 🚀 Appel au domaine pur : la transition est indolore
    {{ calculer_remise('c.statutCarteMaison', 'f.montantTotal') }} AS montantApresRemise
FROM {{ ref('stg_factures') }} f
LEFT JOIN {{ ref('stg_clients') }} c ON f.clientId = c.clientId;