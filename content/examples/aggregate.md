# CRAFT PATTERN : Ne livrez jamais une Facture incomplète

**Le principe d'Aggregate appliqué à dbt**

Avez-vous déjà dû déboguer un rapport financier où le montant TTC d'une facture ne correspondait pas à la somme du HT et de la TVA ? 

Ce bug classique n'est pas une erreur de calcul, c'est une **erreur d'architecture**. Il survient quand la logique métier est fragmentée entre trop de domaines. Appliquer le concept d'**Aggregate** permet de supprimer ces incohérences à la racine.

---

## 1. Qu'est-ce qu'un Aggregate et un Invariant ?

Dans le **Domain-Driven Design (DDD)**, un **Aggregate** est un ensemble d'entités liées qui ne peuvent exister de manière cohérente qu'ensemble (Facture, Lignes de facture, Taxes).

Un **Invariant** est une règle métier qui doit être vraie **en permanence** au sein de cet Aggregate. 
*   **Exemple Invariant** : `margeNette = montantVente - coutRevient - remise`. Si cette équation est fausse à un instant T, votre donnée est corrompue.

---

## 2. L'Exposition de l'Aggregate (Marts vs Intermediate)

En Data Engineering, l'Aggregate est l'**unité atomique de livraison**. Vos modèles d'exposition (couche `marts/`) ne doivent jamais livrer de "brouillons" ou d'états transitoires.

❌ **Anti-Pattern (Fuite de Domaine)** : Vous livrez une table `fctFactures` sans la TVA, et demandez à l'équipe Comptabilité de la calculer de leur côté. Vous venez de briser l'Aggregate.

✅ **Craft Pattern (Encapsulation par Assemblage)** : Vous utilisez la modularité de dbt pour décomposer la complexité, mais vous "refermez" l'Aggregate dans le modèle final.

*Fichier : `models/sales/marts/fctFactures.sql`*
```sql
{{ config(materialized='table') }}

WITH base_factures AS (
    SELECT * FROM {{ ref('int_factures_calculs_de_base') }}
),
taxes AS (
    SELECT * FROM {{ ref('int_factures_calculs_tva') }}
),
final AS (
    SELECT 
        f.*,
        t.montantTva,
        -- L'invariant est finalisé ici, dans le Data Product exposé
        f.montantHt + t.montantTva AS montantTtc
    FROM base_factures f
    LEFT JOIN taxes t USING (factureId)
)
SELECT * FROM final
```

> [!IMPORTANT]
> **Modularité vs Monolithe** : Traiter l'Aggregate comme une unité atomique ne signifie pas écrire un SQL géant. Utilisez des modèles intermédiaires (`int_`) **privés** au sein de votre domaine, mais livrez un Mart complet et verrouillé.

---

## 3. Pourquoi l'Aggregate atomique est-il vital ?

### 3.1. Éviter le couplage fort (SRP)
Si la logique métier est éparpillée entre plusieurs domaines, toute modification d'une taxe vous obligera à traquer des modèles avals que vous ne maîtrisez pas. En encapsulant l'Aggregate, vous respectez le **Single Responsibility Principle**.

### 3.2. Garantir les Data Contracts
Un **contrat de modèle dbt** (exposé dans un catalogue comme Collibra ou DataHub) est une promesse faite à l'organisation. Si vous exposez un Mart incomplet, la promesse est rompue. Le consommateur aval va utiliser vos montants partiels en toute confiance, faussant les résultats financiers globaux.

```yaml
# Le contrat YAML qui protège l'Aggregate
models:
  - name: fctFactures
    config:
      contract:
        enforced: true
    columns:
      - name: montantTtc
        data_type: numeric(16, 2)
        description: "Montant TTC = HT + TVA. Invariant garanti par l'Aggregate."
```

### 3.3. Automatiser l'Observabilité (Elementary)
L'observabilité moderne repose sur des tests d'intégrité de niveau ligne. Un test de cohérence `column_a + column_b = column_c` devient impossible à automatiser si ces colonnes vivent dans deux domaines différents. En refermant l'Aggregate, vous permettez une détection immédiate de toute anomalie de calcul via vos tests dbt.

---

## 🔗 Le pont avec le TDD

L'Aggregate et le TDD sont les deux faces de la même médaille. Le test unitaire `test_calcul_marge_nette_edge_cases` défini dans notre [guide TDD (tdd_mastery.md)](https://github.com/axel-mauroy/craft-data-engineering/blob/main/content/examples/tdd_mastery.md) est précisément le **garde-fou** qui protège l'invariant de cet Aggregate. Sans ce test, vous n'avez aucune preuve que votre Aggregate est cohérent. Avec, vous pouvez refactoriser l'esprit libre.

---

## 🎯 Takeaway

Un modèle de la couche **Marts** est un engagement. Soit il encapsule toutes les règles de l'entité et respecte ses invariants (et passe au vert), soit il échoue (*Fail Fast*). **La donnée ne doit jamais transiter d'un domaine à un autre dans un état de validation partiel.**