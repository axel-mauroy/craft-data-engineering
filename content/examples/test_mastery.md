/*
   ==========================================================================
   CRAFT PATTERN : LA PYRAMIDE DE TESTS DATA (dbt >= 1.10 x Elementary)
   ==========================================================================
   Tester la donnée coûte cher en temps de calcul (I/O BigQuery) et en temps 
   humain (faux positifs). Le Craftsman maîtrise la Pyramide de Tests pour 
   valider sa logique métier en millisecondes, et ses données en continu.
*/

# ------------------------------------------------------------------------------
# NIVEAU 1 : LE TEST UNITAIRE (La Base de la Pyramide)
# Objectif : Tester la logique SQL pure, SANS lire les tables BigQuery (Zéro I/O).
# Outil : dbt Unit Tests (Natifs depuis la v1.8+)
# Fichier : models/sales/marts/sales_unit_tests.yml
# ------------------------------------------------------------------------------

unit_tests:
  - name: test_logique_calcul_remise_fidelite
    description: "Vérifie que la macro de remise applique bien -10% pour les PREMIUM"
    model: fctFacturesEnrichies # Le modèle cible
    # ✅ CRAFT PATTERN : Mocking des données en mémoire
    # dbt va créer des CTEs virtuelles avec ces données exactes, sans requêter la base.
    given:
      - input: ref('stg_factures')
        rows:
          - {factureId: "F1", clientId: "C1", montantHt: 1000}
          - {factureId: "F2", clientId: "C2", montantHt: 500}
      - input: ref('stg_clients')
        rows:
          - {clientId: "C1", statutCarteMaison: "PREMIUM"}
          - {clientId: "C2", statutCarteMaison: "STANDARD"}
    # L'attente absolue du métier (Le Contrat Logique)
    expect:
      rows:
        - {factureId: "F1", montantApresRemise: 900} # 1000 - 10%
        - {factureId: "F2", montantApresRemise: 500} # Pas de remise sous 1000


# ------------------------------------------------------------------------------
# NIVEAU 2 : LE TEST D'INTÉGRATION ET DE CONTRAT (Le Milieu de la Pyramide)
# Objectif : Vérifier que la donnée physique respecte les règles relationnelles.
# Outil : dbt Data Tests & dbt_utils
# Fichier : models/sales/marts/sales_contracts.yml
# ------------------------------------------------------------------------------

models:
  - name: fctFacturesEnrichies
    columns:
      - name: factureId
        tests:
          - unique
          - not_null
      
      - name: clientId
        tests:
          # ✅ CRAFT PATTERN : Intégrité Référentielle (Foreign Key)
          # Vérifie que chaque facture est rattachée à un client qui existe VRAIMENT 
          # dans le domaine Client. Fail-fast bloquant si désynchronisation.
          - relationships:
              to: ref('dimClient')
              field: clientId

      - name: statutFacture
        tests:
          # Évite les valeurs inattendues liées à un changement silencieux de l'ERP source
          - accepted_values:
              values: ['CREEE', 'PAYEE', 'ANNULEE', 'REMBOURSEE']


# ------------------------------------------------------------------------------
# NIVEAU 3 : LE TEST MÉTIER SUR MESURE (L'Expertise du Domaine)
# Objectif : Définir une règle de gestion complexe réutilisable.
# Outil : Macro dbt "Generic Test"
# Fichier : macros/tests/test_montant_coherence.sql
# ------------------------------------------------------------------------------

/*
    -- ❌ L'ANTI-PATTERN : Écrire un test manuel jetable ou faire confiance à la source.
    -- ✅ CRAFT PATTERN : Créer un test générique réutilisable sur tous les modèles.
*/
{% test test_montant_coherence(model, column_name, column_tva, column_ttc) %}
    
    WITH validation AS (
        SELECT 
            {{ column_name }} AS montantHt,
            {{ column_tva }} AS montantTva,
            {{ column_ttc }} AS montantTtc
        FROM {{ model }}
    )
    -- Le test échoue s'il renvoie des lignes. On cherche donc les anomalies.
    SELECT *
    FROM validation
    -- Règle métier absolue : TTC doit toujours être rigoureusement égal à HT + TVA
    WHERE ROUND(montantTtc, 2) != ROUND(montantHt + montantTva, 2)

{% endtest %}

# Application du test dans le YAML :
#      - name: montantHt
#        tests:
#          - test_montant_coherence:
#              column_tva: montantTva
#              column_ttc: montantTtc


# ------------------------------------------------------------------------------
# NIVEAU 4 : L'OBSERVABILITÉ STATISTIQUE (Le Sommet de la Pyramide)
# Objectif : Détecter les dérives progressives (Data Drifts) qui passent les tests stricts.
# Outil : Elementary Data (Monitoring ML)
# Fichier : models/sales/marts/sales_contracts.yml
# ------------------------------------------------------------------------------

models:
  - name: fctFacturesEnrichies
    tests:
      # ✅ CRAFT PATTERN : Détection d'anomalie de volume
      # Un bug source peut diviser notre volume d'ingestion par 2.
      # Les tests `not_null` passeront, mais le métier perdra de l'argent.
      - elementary.volume_anomalies:
          tags: ["alert_stewards"]
          time_bucket:
            period: day
            count: 1
    
    columns:
      - name: montantApresRemise
        tests:
          # Détecte si la moyenne ou l'écart-type des remises explose soudainement
          - elementary.column_anomalies:
              column_anomalies:
                - mean
                - stddev