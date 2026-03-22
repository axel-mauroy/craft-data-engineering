# CRAFT PATTERN : Entity vs Value Object en Data Engineering

> Maîtriser le DDD tactique pour construire des modèles de données sémantiquement riches.

---

## 1. L'Entity (Entité) : L'Identité avant tout

Dans le **Domain-Driven Design (DDD)**, une **Entity** est un objet métier qui se définit par son **identité unique** et constante dans le temps, peu importe l'évolution de ses attributs.

### Caractéristiques principales :
*   **L'identité prime** : Deux objets avec les mêmes attributs (nom, adresse) restent distincts s'ils ont des IDs différents (ex: deux clients homonymes).
*   **La mutabilité** : Une Entity a un cycle de vie. Si un `Customer` change d'e-mail, l'objet est mis à jour, mais c'est toujours la même entité physique.
*   **Identifiant technique** : Elle possède toujours une clé primaire (`customer_id`, `product_id`).

---

## 2. Le Value Object (Objet de Valeur) : L'Égalité par la Valeur

À l'inverse, un **Value Object** n'a pas d'identité propre. Il se définit uniquement par l'ensemble de ses valeurs.

### Caractéristiques principales :
*   **Immutabilité** : On ne modifie pas un Value Object, on le remplace. 
*   **Égalité par le contenu** : "10 USD" est égal à un autre "10 USD". Si vous changez la devise, vous créez un nouveau concept.
*   **Exemples courants** : Une adresse postale, un montant monétaire, une couleur, un code ISO de pays.

> **La question clé :** "Est-ce que modifier les attributs crée un nouveau concept ?"  
> Si oui, c'est un **Value Object** (ex: changer la rue change l'adresse). Si non, c'est une **Entity** (ex: changer l'adresse du client ne change pas l'identité du client).

---

## 3. Implémentation concrète dans dbt

Traduire ces concepts dans le monde de la Data permet de structurer vos pipelines de manière plus robuste :

### 3.1. Modélisation dans la couche Marts
Les **Entities** métier (Clients, Produits, Commandes) constituent l'aboutissement de votre pipeline. Elles se matérialisent généralement sous forme de tables de dimension (`dim_customers.sql`).

### 3.2. Garantie de l'Identité (Tests & Contrats)
*   **Data Tests** : Un modèle d'entité **doit** avoir des tests `unique` et `not_null` sur sa clé primaire. 
*   **Model Contracts** : Utilisez les nouveaux contrats dbt pour déclarer formellement la contrainte `primary_key` dans le YAML.

### 3.3. Gestion des mutations via dbt Snapshots (SCD2)
C'est ici que la mutabilité de l'Entity s'exprime. Pour traquer l'évolution historique d'une entité sans perdre son identité, utilisez les **dbt Snapshots**. Ils implémentent automatiquement une "Slowly Changing Dimension" (SCD Type 2), générant des colonnes de validité (`dbt_valid_from`, `dbt_valid_to`) à chaque changement d'attribut.

---

## 🎯 Takeaway

En Data Engineering, une **Entity** métier se matérialise dans la couche **Marts**. 

Son identité est blindée par des tests d'unicité et des **Model Contracts**, sa cohérence dicte les frontières du modèle (Aggregates), et ses mutations temporelles sont capturées élégamment par les **dbt Snapshots**.