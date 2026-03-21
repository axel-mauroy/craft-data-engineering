/* 
   ==========================================================================
   ARCHITECTURE PATTERN : ISOLATION DE LA LOGIQUE ANALYTIQUE (READ DOMAIN)
   ==========================================================================
   Ce document illustre l'isolation d'une règle de reporting / KPI analytique
   (ex: reconstitution d'une remise pour analyse BI).
   
   ⚠️ IMPORTANT : Les décisions métier transactionnelles (ex: le prix RÉEL 
   facturé au client) doivent vivre dans le service applicatif amont. 
   dbt n'est ici qu'un "Adapter de lecture" isolant la logique de calcul analytique.
*/

-- Correspondance avec l'Architecture Hexagonale (Mise à jour) :
-- | Couche  | Rôle                      | Outil dbt / Craft            |
-- |---------|---------------------------|------------------------------|
-- | Domain  | KPI / Règle d'entreprise  | dbt Semantic Layer (YAML)    |
-- | Port    | Interface / Composant DRY | Macro Jinja                  |
-- | Adapter | Assemblage infrastructure | Modèle SQL (Marts)           |
-- | Test    | Vérification du contrat   | dbt Unit Test / Data Test    |


-- ❌ L'ANTI-PATTERN : Logique couplée au moteur technique
-- Pourquoi c'est fragile : 
-- 1. Couplage Métier : La règle de remise est noyée dans le SQL.
-- 2. Couplage Infra : Dépendance aux fonctions système (CURRENT_DATE).
-- 3. Testabilité : Impossible à tester sans charger la base de données.

SELECT 
    f.factureId,
    c.clientId,
    CASE 
        WHEN c.statutCarteMaison = 'PREMIUM' AND f.montantTotal > 1000 THEN f.montantTotal * 0.90
        WHEN c.statutCarteMaison = 'STANDARD' AND f.montantTotal > 500 THEN f.montantTotal * 0.95
        ELSE f.montantTotal 
    END AS montantApresRemise
FROM `lm-data-prd.prd_sales_domain.factures` f
LEFT JOIN `lm-data-prd.prd_customer_domain.clients` c ON f.clientId = c.clientId
WHERE DATE(f.dateCreation) = CURRENT_DATE(); -- ⚠️ Couplage temporel d'infrastructure


/* 
   --------------------------------------------------------------------------
   ✅ LA VISION "CRAFT" : Isolation de la Logique Analytique
   --------------------------------------------------------------------------
*/

-- Étape 1 : Le composant réutilisable (Macro Jinja isolée)
-- On encapsule la logique pure de transformation à la ligne dans une Macro pour rester DRY, 
-- mais sans en abuser pour conserver la lisibilité du SQL.
-- Fichier: macros/domain_sales/calculer_remise.sql

{% macro calculer_remise(colonne_statut, colonne_montant) %}
    CASE 
        WHEN COALESCE({{ colonne_statut }}, 'SANS_CARTE') = 'PREMIUM' AND COALESCE({{ colonne_montant }}, 0) > 1000 
            THEN {{ colonne_montant }} * 0.90
        WHEN COALESCE({{ colonne_statut }}, 'SANS_CARTE') = 'STANDARD' AND COALESCE({{ colonne_montant }}, 0) > 500 
            THEN {{ colonne_montant }} * 0.95
        ELSE COALESCE({{ colonne_montant }}, 0)
    END
{% endmacro %}


-- Étape 2 : Le Modèle (Adapter) 
-- Le SQL ne fait qu'assembler les briques.
-- Fichier: models/marts/finance/fct_factures_remises.sql

SELECT 
    f.factureId,
    c.clientId,
    {{ calculer_remise('c.statutCarteMaison', 'f.montantTotal') }} AS montantApresRemise
FROM {{ ref('stg_factures') }} f
LEFT JOIN {{ ref('stg_clients') }} c ON f.clientId = c.clientId;


-- Étape 3 : Le Domaine Analytique Pur (dbt Semantic Layer)
-- L'isolation ultime du KPI ne se fait plus en SQL, mais via MetricFlow (Semantic Layer).
-- C'est ici que vit la "Règle de Reporting" pour garantir une cohérence parfaite dans tous les outils BI.
-- Fichier: models/marts/finance/metrics.yml
/*
metrics:
  - name: montant_total_remise
    description: "Reconstitution analytique du montant total après l'application des remises fidélité."
    type: sum
    measure: montantApresRemise
    # Le Semantic Layer gère ensuite dynamiquement le code, le cache et les jointures pour les outils BI.
*/


-- Étape 4 : Les Contrats et les Tests (La Garantie de l'Architecture)
-- Pour aller au bout de l'Architecture Hexagonale, il faut distinguer :
-- 1. Le "Model Contract" : garantit l'interface technique (le schéma attendu par les consommateurs).
-- 2. Le "Unit Test" : garantit la logique métier (le Port) de manière isolée sans accès aux données réelles.
-- 3. Le "Data Test" : garantit l'intégrité de la donnée en production (ex: pas de doublons post-jointure).
-- Fichier: models/marts/finance/finance.yml
/*
models:
  - name: fct_factures_remises
    config:
      materialized: table # 💡 Requis pour appliquer les contraintes de contrat
      contract:
        enforced: true # 🚀 GARANTIE DU PORT : Le modèle échouera si le schéma ne respecte pas ce contrat strict.
    columns:
      - name: factureId
        data_type: integer
        constraints:
          - type: not_null
      - name: clientId
        data_type: string
      - name: montantApresRemise
        data_type: float

unit_tests:
  - name: test_regles_remise
    model: fct_factures_remises
    given:
      - input: ref('stg_factures')
        rows:
          - {factureId: 1, clientId: 'C1', montantTotal: 1500}
      - input: ref('stg_clients')
        rows:
          - {clientId: 'C1', statutCarteMaison: 'PREMIUM'}
    expect:
      rows:
        - {factureId: 1, montantApresRemise: 1350}
*/


/*
   ==========================================================================
   💡 EN RÉSUMÉ :
   Gardez l'idée de la Macro Jinja comme équivalent d'une fonction (Port) 
   pour modulariser les calculs répétitifs au niveau de la ligne, mais introduisez 
   le dbt Semantic Layer comme l'outil moderne (le vrai "Domain" analytique) pour 
   définir, isoler et gouverner vos KPIs et métriques d'entreprise. 
   
   Sublimez votre approche avec les Model Contracts (contract: {enforced: true}) 
   pour sceller l'interface technique du Port de l'architecture hexagonale.
   N'oubliez pas non plus d'insister sur les Unit Tests (pour valider la 
   règle métier "à vide") et les Data Tests (pour garantir l'intégrité en production) !
   ==========================================================================
*/