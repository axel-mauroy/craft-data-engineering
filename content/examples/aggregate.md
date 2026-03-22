# CRAFT PATTERN : L'Aggregate comme Unité Atomique dans dbt

> Appliquer les principes du Domain-Driven Design (DDD) pour garantir l'intégrité métier de vos modèles de données.

---

## 1. Qu'est-ce qu'un Aggregate et un Invariant ?

Dans le **Domain-Driven Design (DDD)**, un **Aggregate** (Agrégat) est une grappe d'objets métier qui doivent être traités comme une seule unité cohérente. 

Un **Invariant** est une règle métier absolue qui doit toujours rester vraie à l'intérieur de cet Aggregate, quel que soit son état. 

*   **Exemple Aggregate Facture** : Ses invariants mathématiques stricts seraient :
    *   `margeNette = montantVente - coutRevient - remise` (comme vu en TDD).
    *   `TTC = HT + TVA`.

---

## 2. Ne jamais "couper" ses invariants entre deux modèles dbt

En Data Engineering, affirmer que l'Aggregate est l'**unité atomique** signifie qu'un modèle dbt final (un Data Product ou un Read Model) doit toujours exposer une entité métier dans un état **100% valide et cohérent**.

❌ **Anti-Pattern** : Calculer une partie de la facture dans un modèle dbt `A`, l'exposer, puis laisser un modèle dbt `B` en aval calculer la `margeNette` ou corriger les `NULL`.

---

## 3. Pourquoi est-ce mal ? (Risques Architecturaux)

### 3.1. Violation du SRP (Single Responsibility Principle)
Le principe de responsabilité unique stipule qu'un modèle dbt ne doit avoir qu'une seule raison de changer. Si la logique définissant une "Facture valide" est éparpillée, vous créez un **couplage fort** et risquez d'oublier de modifier un modèle lors d'une mise à jour de règle métier.

### 3.2. Rupture des Contrats de Modèle (Model Contracts)
Un **dbt model contract** garantit la forme et les types. Si vous coupez un invariant, vous exposez potentiellement un modèle "techniquement" correct mais "métier-invalide". Un domaine consommateur (ex: la Comptabilité) pourrait se brancher sur ce "brouillon" et fausser ses rapports financiers.

### 3.3. Impossibilité de garantir la Qualité des Données
Les tests de cohérence (ex: `test_montant_coherence` ou `dbt-expectations`) ne peuvent fonctionner que si le modèle traite l'Aggregate dans sa totalité. Si l'invariant est coupé, le test de cohérence à l'échelle de la ligne devient impossible à réaliser de manière fiable.

---

## 🎯 Takeaway

Traiter l'**Aggregate** comme l'unité atomique dans dbt signifie que vos modèles de la couche **Marts** (interfaces publiques) ne doivent jamais livrer de "brouillons". 

Soit le modèle calcule **toutes les règles métier** et respecte **tous les invariants** (et passe au vert), soit il échoue (*Fail Fast*). La donnée ne doit jamais transiter d'un domaine à un autre dans un état de validation partiel.