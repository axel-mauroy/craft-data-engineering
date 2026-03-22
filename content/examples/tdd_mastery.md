# CRAFT PATTERN : Le cycle "Red, Green, Refactor" appliqué au SQL

**Pourquoi j'écris mes tests dbt avant mon SQL**

Le TDD en ingénierie de la donnée n'est pas une option, c'est un bouclier. Il force le Data Engineer à penser aux cas limites (valeurs manquantes, doublons) **avant** d'écrire la moindre ligne de transformation.

Le cycle se divise en 3 étapes strictes :

1. 🔴 **RED** : Écrire le test qui échoue.
2. 🟢 **GREEN** : Écrire le SQL minimal pour faire passer le test.
3. 🔵 **REFACTOR** : Optimiser le SQL pour les performances BigQuery (I/O) sans casser le test.

---

## ÉTAPE 1 : 🔴 RED (Penser le Contrat et les Cas Limites)

Avant de toucher au SQL, nous définissons notre modèle et ses règles de gestion dans le YAML.

**Exemple métier :** Nous devons calculer la `margeNette` d'une ligne de facture.  
**Règle métier :** `margeNette = montantVente - coutRevient - remise`.

*Fichier : `models/sales/marts/marge_tests.yml`*

```yaml
models:
  - name: fctLignesMarge
    description: "Calcul de la marge nette par ligne de facture."
    # 1. Le Contrat Physique
    config:
      contract:
        enforced: true
    columns:
      - name: ligneId
        data_type: string
      - name: dateTransaction
        data_type: date
      - name: magasinId
        data_type: string
      - name: margeNette
        data_type: numeric(16, 2)

    # 2. Le Test Unitaire dbt (Le coeur du TDD)
    # Note : le bloc `expect` peut ignorer les colonnes non testées (dateTransaction, magasinId).
    # Les Unit Tests dbt 1.8+ ne vérifient que les colonnes explicitement déclarées dans `expect`.
    unit_tests:
      - name: test_calcul_marge_nette_edge_cases
        description: "Vérifie le calcul de la marge, y compris avec des données sales (NULL)."
        model: fctLignesMarge
        given:
          - input: ref('stg_lignes_facture')
            rows:
              # Cas 1 : Nominal (100 - 40 - 10 = 50)
              - {ligneId: "L1", montantVente: 100, coutRevient: 40, remise: 10}
              # Cas 2 : Edge Case - Pas de remise (NULL). La marge doit quand même se calculer.
              - {ligneId: "L2", montantVente: 100, coutRevient: 50, remise: null}
              # Cas 3 : Edge Case - Coût de revient inconnu (NULL). La marge doit être 0 par sécurité.
              - {ligneId: "L3", montantVente: 100, coutRevient: null, remise: 5}
        
        expect:
          rows:
            - {ligneId: "L1", margeNette: 50}
            - {ligneId: "L2", margeNette: 50}
            - {ligneId: "L3", margeNette: 0} # Sécurité métier exigée
```

Pour initier la phase "RED", nous créons un fichier SQL "bouchon" (dummy) qui respecte la forme mais pas la logique. 
*Fichier : `models/sales/marts/fctLignesMarge.sql`*

```sql
-- BOUCHON TDD (Phase RED) : On renvoie 0 partout pour faire échouer le test métier.
SELECT 
    ligneId,
    0 AS margeNette
FROM {{ ref('stg_lignes_facture') }}
```

Nous lançons ensuite la commande isolant les tests unitaires : `dbt test --select fctLignesMarge test_type:unit`
Le test va échouer proprement en affichant la différence entre nos attentes (50) et le résultat actuel (0). C'est le but : la cible est verrouillée, la phase GREEN peut commencer.

---

## ÉTAPE 2 : 🟢 GREEN (L'Implémentation Naïve)

Maintenant, nous écrivons le code SQL **strict minimum** pour satisfaire le test métier. À ce stade, le Data Craftsman ne se soucie pas encore des optimisations BigQuery (partitions, clusters). L'objectif est de **traduire la logique**.

*Fichier : `models/sales/marts/fctLignesMarge.sql`*

```sql
{{ config(materialized='table') }}

WITH lignes AS (
    SELECT * FROM {{ ref('stg_lignes_facture') }}
)

SELECT
    ligneId,
    dateTransaction,
    magasinId,
    /*
       ✅ CRAFT PATTERN : La logique est dictée par les tests (TDD)
       Grâce à notre test sur les lignes L2 et L3, nous avons été FORCÉS 
       d'utiliser des COALESCE et des CASE WHEN pour gérer les valeurs NULL.
    */
    SAFE_CAST(
        CASE 
            WHEN coutRevient IS NULL THEN 0
            ELSE montantVente - coutRevient - COALESCE(remise, 0)
        END 
    AS NUMERIC(16, 2)) AS margeNette

FROM lignes
```

Nous relançons notre test isolé en mémoire : `dbt test --select fctLignesMarge test_type:unit`. Le test passe au **VERT**. La logique métier est validée et garantie.

---

## ÉTAPE 3 : 🔵 REFACTOR (L'Optimisation Architecturale)

C'est ici que l'Architecte de Données entre en jeu. Le modèle fait le bon calcul, mais sur les 3 milliards de lignes de Leroy Merlin, un `materialized='table'` sans partitionnement va ruiner notre budget BigQuery.

Nous allons refactoriser le code pour le rendre **incrémental et optimisé**. La beauté du TDD, c'est que nous pouvons faire ces modifications d'infrastructure **l'esprit libre** : notre test unitaire est là pour garantir que nous ne casserons pas le calcul de la marge !

*Fichier : `models/sales/marts/fctLignesMarge.sql` (Version Finale)*

```sql
{{ 
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        on_schema_change='fail', -- OBLIGATOIRE pour les modèles incrémentaux sous contrat
        partition_by={
            "field": "dateTransaction",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by=['magasinId']
    ) 
}}

WITH lignes AS (
    SELECT 
        ligneId,
        magasinId,
        dateTransaction,
        montantVente,
        coutRevient,
        remise
    FROM {{ ref('stg_lignes_facture') }}
    {% if is_incremental() %}
        -- ⚠️ Cette fenêtre de 3 jours est un choix architectural lié au SLA d'ingestion.
        -- Les unit tests ne couvrent PAS ce filtre (is_incremental() = false en test).
        -- Validez cette fenêtre via un test d'intégration séparé (ex: Elementary volume_anomalies).
        WHERE dateTransaction >= DATE_SUB(DATE '{{ run_started_at.strftime("%Y-%m-%d") }}', INTERVAL 3 DAY)
    {% endif %}
)

SELECT
    ligneId,
    dateTransaction,
    magasinId,
    SAFE_CAST(
        CASE 
            WHEN coutRevient IS NULL THEN 0
            ELSE montantVente - coutRevient - COALESCE(remise, 0)
        END 
    AS NUMERIC(16, 2)) AS margeNette
FROM lignes
```

**Le test final (Validation globale)** : Plutôt que de relancer uniquement le test unitaire, le Craftsman lance maintenant la commande suprême pour déployer son travail : `dbt build --select fctLignesMarge`. Cette commande va :
- Lancer le test unitaire (Vérification de la logique pure).
- Valider le contrat DDL (Vérification de la forme).
- Matérialiser la table incrémentale de manière optimisée.
- Lancer les Data Tests (ex: `not_null` sur la table physique).

C'est **VERT** sur toute la ligne. Le code est désormais prêt pour la production : il est fonctionnellement exact, physiquement optimisé et formellement gouverné.

---

## 🎯 Takeaways du Data TDD

1. **La documentation par l'exemple :** Les tests unitaires YAML deviennent la meilleure documentation pour le métier. Un Data Analyst peut lire le YAML et comprendre exactement comment la marge réagit face à une remise `NULL`.
2. **Confiance lors du Refactoring :** Quand vous devrez passer ce modèle de BigQuery à un autre moteur, ou changer la clé de partitionnement, vous aurez la certitude absolue de ne pas altérer les KPIs financiers.
3. **Gain de temps de calcul (FinOps) :** Déboguer une règle de gestion directement sur BigQuery en faisant des requêtes `SELECT` sur des pétaoctets coûte très cher. Les Unit Tests dbt tournent en mémoire, localement, et sont **gratuits**.
