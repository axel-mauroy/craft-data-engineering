# CRAFT PATTERN : DATA CONTRACTS (La Garantie Data Mesh)
**Outils : dbt (>= 1.10) x Collibra x BigQuery**

Dans un Data Mesh, un modèle final (Mart) n'est pas juste une table, c'est un "Data Product". Il doit offrir des garanties strictes à ses consommateurs. Un Data Craftsman ne casse jamais ses contrats en production.

---

## 1. LE CONTRAT DÉCLARATIF (Le fichier .yml)

C'est le pont entre l'ingénierie (dbt) et la gouvernance (Collibra).  
*Fichier : `models/sales/marts/sales_contracts.yml`*

```yaml
models:
  - name: fctFacturesEnrichies
    description: "Modèle central des factures, exposé aux autres domaines (Comptabilité)."
    
    # ✅ CRAFT PATTERN : Activation du contrat
    # Si le SQL génère une table qui ne respecte pas EXACTEMENT ces types,
    # dbt refusera de compiler (Erreur bloquante en CI/CD).
    config:
      contract:
        enforced: true
      
      # Pont avec Collibra : Les métadonnées remontent via le manifest.json
      meta:
        collibra_domain: "Sales & Commerce"
        data_steward: "jean.dupont@leroymerlin.fr"
        data_product_tier: "Tier 1 (Critique)"

    columns:
      - name: factureId
        data_type: string    # Contrat technique
        description: "Identifiant unique de la facture."
        constraints:
          - type: not_null   # Le contrat interdit l'insertion de NULL
          - type: primary_key # Non-enforced sur BigQuery car il n'enforce pas les PKs au niveau moteur (ce sera donc garanti via test unique), mais son Query Optimizer l'utilise pour accélérer les jointures !
        meta:
          collibra_concept_id: "C-98765" # Lien direct vers la définition métier

      - name: clientId
        data_type: string
        description: "Identifiant du client."
        meta:
          pii: true          # Collibra saura qu'il faut masquer ce champ
          
      - name: montantTtc
        data_type: numeric
        description: "Montant total Toutes Taxes Comprises de la facture."
        constraints:
          - type: not_null
          # ⚠️ Les contraintes sémantiques (ex: CHECK > 0) ne sont pas supportées
          # par le DDL BigQuery. Elles sont donc déléguées aux tests dbt (data_tests).
```

---

## 2. L'IMPLÉMENTATION SQL (L'Adapter sous contrat)

Le fichier SQL doit s'aligner parfaitement sur le YAML.  
*Fichier : `models/sales/marts/fctFacturesEnrichies.sql`*

```sql
{{
    config(
        materialized='incremental',
        unique_key='factureId',
        -- Le contrat est défini dans le YAML, mais appliqué ici par dbt
    )
}}

WITH sourceFactures AS (
    SELECT
        facture_id,
        client_id,
        montant_ht
    FROM {{ ref('stg_factures') }}
)

SELECT
    /*
       3. RESPECT DU TYPAGE EXPLICITE (Sécuriser le contrat)
       ----------------------------------------------------------------------
       ❌ ANTI-PATTERN : SELECT id_facture AS factureId
       Si la source change de type (ex: INT64 vers STRING), BigQuery pourrait 
       laisser passer, mais le contrat dbt plantera car il attend un STRING.

       ✅ CRAFT PATTERN : Cast explicite (ou SAFE_CAST quand le moteur le permet)
       Le Craftsman s'assure que la sortie correspond exactement au contrat
       déclaré, et gère les erreurs de typage silencieusement ou via Elementary.
    */
    SAFE_CAST(facture_id AS STRING) AS factureId,
    
    SAFE_CAST(client_id AS STRING) AS clientId,
    
    /*
       ✅ CRAFT PATTERN : Règle métier isolée (Hexagonal)
       La logique métier (ex: TVA) est encapsulée dans une macro dbt pure, hors
       de l'assemblage SQL.
       Puisque BigQuery ne supporte pas les contraintes CHECK dans son DDL,
       la garantie sémantique (montantTtc > 0) sera assurée par un test dbt 
       (ex: dbt-expectations) exécuté juste après le build.
    */
    {{ calculer_montant_ttc('montant_ht') }} AS montantTtc

FROM sourceFactures
```

---

## 3. LA GESTION DES VERSIONS (Évolution sans casse)

Que faire si le métier demande de changer le grain du modèle ou de supprimer une colonne vitale ?

❌ **ANTI-PATTERN** : Modifier la table existante et prévenir sur Slack.  
Cela va casser les requêtes des autres domaines (Data Mesh Fail).

✅ **CRAFT PATTERN** : Le Model Versioning (dbt >= 1.5)  
On crée la v2 de notre contrat. La v1 continue de tourner en production pendant une "fenêtre de dépréciation" pour laisser le temps aux consommateurs de migrer. Collibra affichera la v1 comme "Deprecated".

---

## Takeaways

1. **Shift-Left de la Qualité** : Sans contrat, on découvre qu'une colonne a changé de type quand le tableau de bord plante en production le lundi matin (Observabilité a posteriori). Avec le `contract: enforced: true`, le développeur est bloqué sur sa machine au moment où il tape `dbt run`. Il ne peut même pas pousser son code.
2. **Gouvernance as Code (Collibra)** : Finis les dictionnaires de données Excel obsolètes. Les tags `meta:` dans le YAML permettent à l'intégration logicielle (ou à un pipeline CI/CD personnalisé) de pousser les descriptions dbt directement dans Collibra. La documentation technique est la documentation officielle.
3. **Versioning Stratégique** : Le versioning des modèles est crucial pour maintenir la compatibilité avec les consommateurs tout en faisant évoluer les données. Collibra peut afficher la v1 comme "Deprecated" pendant que la v2 est déployée, permettant une transition en douceur.
