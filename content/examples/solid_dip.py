"""
==========================================================================
CRAFT PATTERN : DEPENDENCY INVERSION PRINCIPLE (DIP)
==========================================================================
Le DIP stipule que les modules de haut niveau (Domaine) ne doivent pas 
dépendre des modules de bas niveau (Infrastructure). Les deux doivent 
dépendre d'abstractions (Ports).
--------------------------------------------------------------------------
"""

# --- ÉTAPE 0 : LE PROBLÈME (ANTI-PATTERN) ---
# ❌ Le pipeline dépend directement de l'implémentation BigQuery.
# Impossible de tester sans cluster, impossible de changer de moteur sans tout réécrire.

# from google.cloud import bigquery
# def charger_factures(date: str) -> list[dict]:
#     client = bigquery.Client()
#     # ❌ ANTI-PATTERN : Injection SQL et plan d'exécution non mis en cache
#     query = f"SELECT * FROM `prd.sales.fctFactures` WHERE date = '{date}'"
#     return list(client.query(query))


# --- ÉTAPE 1 : LE PORT (Abstraction) ---
from abc import ABC, abstractmethod
from dataclasses import dataclass

@dataclass
class Facture:
    facture_id: str
    montant_ttc: float

class FactureRepository(ABC):
    """
    Port de lecture — indépendant de tout moteur.
    Le domaine dépend de cette interface, jamais de l'infrastructure.
    """
    @abstractmethod
    def get_factures_by_date(self, date: str) -> list[Facture]:
        pass


# --- ÉTAPE 2 : L'ADAPTER BIGQUERY (Implémentation Prod) ---
# Seul cet adapter connaît l'existence de BigQuery et du modèle dbt.

class BigQueryFactureRepository(FactureRepository):
    def __init__(self):
        # self._client = bigquery.Client()
        pass

    def get_factures_by_date(self, date: str) -> list[Facture]:
        # ✅ CRAFT PATTERN : Utilisation des paramètres typés (Query Parameters)
        # Cela évite l'injection SQL et permet au moteur d'optimiser le cache.
        query = """
            SELECT factureId, montantTtc 
            FROM `prd-sales.prd_sales_domain.fctFacturesEnrichies`
            WHERE dateCommande = @date_commande
        """
        # Simulation du job_config BigQuery
        print(f"📡 Appel BigQuery (Paramétré) pour le @date_commande='{date}'")
        
        # En production, on ferait :
        # job_config = bigquery.QueryJobConfig(
        #     query_parameters=[bigquery.ScalarQueryParameter("date_commande", "DATE", date)]
        # )
        # return [Facture(...) for row in self._client.query(query, job_config=job_config)]
        
        return [Facture(facture_id="FAC-001", montant_ttc=120.0)]


# --- ÉTAPE 3 : L'ADAPTER DUCKDB (Implémentation Test) ---
# Permet de tester le domaine en millisecondes, sans coût et sans réseau.

class DuckDBFactureRepository(FactureRepository):
    def __init__(self, db_path: str = ":memory:"):
        import duckdb
        self._conn = duckdb.connect(db_path)
        # Setup table de test simulée
        self._conn.execute("CREATE TABLE fctFacturesEnrichies (factureId TEXT, montantTtc DOUBLE, dateCommande DATE)")
        self._conn.execute("INSERT INTO fctFacturesEnrichies VALUES ('TEST-001', 99.9, '2024-01-15')")

    def get_factures_by_date(self, date: str) -> list[Facture]:
        print(f"🦆 Appel DuckDB Local (Moteur de test) pour le {date}")
        results = self._conn.execute(
            "SELECT factureId, montantTtc FROM fctFacturesEnrichies WHERE dateCommande = ?", 
            [date]
        ).fetchall()
        return [Facture(facture_id=r[0], montant_ttc=r[1]) for r in results]


# --- ÉTAPE 4 : LE SERVICE (DOMAINE) ---
# Le service ne connaît que le Port. Il est pur et testable.

class FactureService:
    def __init__(self, repository: FactureRepository):
        self._repository = repository

    def calculer_chiffre_affaires(self, date: str) -> float:
        factures = self._repository.get_factures_by_date(date)
        return sum(f.montant_ttc for f in factures)


# --- ÉTAPE 5 : ASSEMBLAGE (Injection de Dépendance) ---
if __name__ == "__main__":
    # En PROD : On injecte l'Adapter BigQuery
    repo_prod = BigQueryFactureRepository()
    service_prod = FactureService(repository=repo_prod)
    print(f"Résultat Prod: {service_prod.calculer_chiffre_affaires('2024-01-15')}\n")

    # En TEST : On injecte l'Adapter DuckDB
    repo_test = DuckDBFactureRepository()
    service_test = FactureService(repository=repo_test)
    print(f"Résultat Test: {service_test.calculer_chiffre_affaires('2024-01-15')}")

    # ✅ RÉSULTAT CRAFT : 
    # Le service (Domaine) n'a pas changé d'une ligne. 
    # dbt a fourni le contrat physique (fctFacturesEnrichies), 
    # le DIP a fourni la flexibilité architecturale.
