# CRAFT PATTERN : Le Repository (Référentiel) en Data Engineering

> Faire le pont entre la logique métier pure et l'infrastructure technique (bases de données).

---

## 1. Le concept théorique (DDD pur)

Un **Repository** est un patron de conception (*design pattern*) qui abstrait l'accès aux données.

Du point de vue de votre domaine métier, le Repository se comporte comme une simple **collection d'objets en mémoire** (on peut y ajouter, retirer ou chercher des éléments). Il cache totalement les détails techniques sous-jacents (SQL, requêtes HTTP, appels système).

> **La règle d'or :** Un Repository interagit exclusivement avec des **Aggregate Roots** (les racines d'agrégats), et jamais directement avec les entités internes qui composent l'agrégat.

---

## 2. L'application en Data Engineering & dbt

Dans une architecture Data moderne, ce concept correspond exactement à l'**Inversion de Dépendance** abordée à la Page 6 (Clean Code) avec l'exemple `solid_dip.py`.

Voici comment le puzzle s'assemble :
*   **dbt est le moteur physique** : Il s'occupe de la transformation et matérialise vos entités dans la couche **Marts** (ex: `fctFacturesEnrichies`). Pour le logiciel, cette table dbt est le *Read Model*.
*   **Le Repository est l'Interface (Le Port)** : Dans votre code Python (application, API, orchestrateur), le domaine définit une interface abstraite (ex: `FactureRepository`). Elle dit **quoi faire** (`get_factures_by_date()`), mais pas **comment**.
*   **L'Adapter est l'implémentation (L'Infrastructure)** : C'est ici que l'on écrit le code technique qui va requêter dbt. Exemple : `BigQueryFactureRepository` (SQL vers BigQuery) ou `DuckDBFactureRepository` (pour les tests locaux).

---

## 3. Pourquoi c'est un "Craft Pattern" indispensable ?

### 🚀 Testabilité absolue
En utilisant un Repository, vous pouvez tester votre logique métier Python en millisecondes en injectant un **Mock Repository** ou une base locale en mémoire (DuckDB), sans jamais toucher à BigQuery ni payer de coûts de Cloud.

### 🛡️ Agnosticisme technologique
Votre domaine métier n'est pas "pollué" par des librairies spécifiques (`google-cloud-bigquery`). Si demain vous migrez vers Snowflake, votre logique métier ne change pas d'une seule ligne : vous codez simplement un nouveau `SnowflakeFactureRepository`.

### 🔗 Protection du pipeline
Le Repository vous force à respecter le **contrat dbt**. Il lit les données en respectant le schéma et les types imposés par votre contrat, garantissant une isolation parfaite entre le stockage et l'usage.

---

## 🎯 Takeaway

Les modèles dbt (**Marts**) préparent l'**Aggregate Root** dans la base de données, et le **Repository** est l'abstraction Python qui vient le lire pour l'injecter proprement dans la logique métier. 

C'est l'alliance parfaite entre le Data Engineering et le Software Engineering !