/* 
   ==========================================================================
   ARCHITECTURE PATTERN : ISOLATION DE LA LOGIQUE ANALYTIQUE
   ==========================================================================
   Ce document illustre la séparation de la logique métier (Domain) de 
   l'infrastructure (Adapter). Nous utilisons ici l'Architecture Hexagonale.
*/

-- Correspondance avec l'Architecture Hexagonale :
-- | Couche  | Rôle                      | Outil dbt / Craft            |
-- |---------|---------------------------|------------------------------|
-- | Domain  | Règle pure, testable      | Python model / Conceptuel    |
-- | Port    | Interface déclarée        | Macro Jinja (Contrat)        |
-- | Adapter | Assemblage infrastructure | Modèle SQL fct*.sql          |
-- | Test    | Vérification du contrat   | dbt Unit Test (Component)    |


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
   ✅ LA VISION "CRAFT" : L'isolation de la Logique Analytique
   --------------------------------------------------------------------------
*/

-- Étape 1 : Le Domaine Pur (Logique Métier Pure)
-- L'idéal théorique est une fonction pure (Python) sans aucune dépendance.
/*
def calculer_remise_fidelite(statut_carte: str, montant_total: float) -> float:
    # Agnostique : ne sait rien de BigQuery ou SQL.
    if statut_carte == 'PREMIUM' and montant_total > 1000:
        return montant_total * 0.90
    if statut_carte == 'STANDARD' and montant_total > 500:
        return montant_total * 0.95
    return montant_total
*/


-- Étape 2 : Le Port (Macro Jinja isolée)
-- On encapsule la règle analytique dans une Macro.
-- 💡 Note : C'est une isolation partielle (pragmatique). dbt reste ici un "adapter de lecture" 
-- qui isole la logique de transformation du reste du pipeline.
-- Fichier: macros/domain_sales/calculer_remise.sql

{% macro calculer_remise(colonne_statut, colonne_montant) %}
    CASE 
        /* Utilisation de COALESCE pour garantir un typage robuste et éviter la propagation des NULLs */
        WHEN COALESCE({{ colonne_statut }}, 'SANS_CARTE') = 'PREMIUM' AND COALESCE({{ colonne_montant }}, 0) > 1000 
            THEN {{ colonne_montant }} * 0.90
        WHEN COALESCE({{ colonne_statut }}, 'SANS_CARTE') = 'STANDARD' AND COALESCE({{ colonne_montant }}, 0) > 500 
            THEN {{ colonne_montant }} * 0.95
        ELSE COALESCE({{ colonne_montant }}, 0)
    END
{% endmacro %}


-- Étape 3 : Le Test de Composant (Vérification de l'assemblage)
-- dbt Unit Test permet de vérifier le contrat du Port utilisé par l'Adapter.
-- Fichier: models/sales/sales.yml
/*
unit_tests:
  - name: test_regles_remise
    model: fctFacturesEnrichies  # On teste l'assemblage (Component Test)
    given:
      - input: ref('stg_factures')
        rows:
          - {factureId: 1, clientId: 'C1', montantTotal: 1500}
          - {factureId: 2, clientId: 'C2', montantTotal: 800}
          - {factureId: 3, clientId: 'C1', montantTotal: 200}
          - {factureId: 4, clientId: 'C_INCONNU', montantTotal: 2000}
      - input: ref('stg_clients')
        rows:
          - {clientId: 'C1', statutCarteMaison: 'PREMIUM'}
          - {clientId: 'C2', statutCarteMaison: 'STANDARD'}
    expect:
      rows:
        - {factureId: 1, montantApresRemise: 1350}  -- PREMIUM > 1000 : -10%
        - {factureId: 2, montantApresRemise: 760}   -- STANDARD > 500 : -5%
        - {factureId: 3, montantApresRemise: 200}   -- < seuil : pas de remise
        - {factureId: 4, montantApresRemise: 2000}  -- client inconnu (NULL) : pas de remise
*/


-- Étape 4 : L'Adapter (Modèle SQL d'assemblage)
-- Le modèle se contente de lier la logique du domaine aux sources de données.
-- Fichier: models/sales/marts/fctFacturesEnrichies.sql

SELECT 
    f.factureId,
    c.clientId,
    -- 🚀 Appel au domaine via le Port : la logique métier est isolée.
    {{ calculer_remise('c.statutCarteMaison', 'f.montantTotal') }} AS montantApresRemise
FROM {{ ref('stg_factures') }} f
LEFT JOIN {{ ref('stg_clients') }} c ON f.clientId = c.clientId;