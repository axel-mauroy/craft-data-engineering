# ==============================================================================
# CRAFT PATTERN : INVERSION DE DÉPENDANCE (D.I.P) EN DATA ENGINEERING
# ==============================================================================

from abc import ABC, abstractmethod
from dataclasses import dataclass
from google.cloud import bigquery
import duckdb
import os

# ------------------------------------------------------------------------------
# 1. LE DOMAINE ET LE PORT (Agnostiques)
# ------------------------------------------------------------------------------

@dataclass
class Facture:
    facture_id: str
    montant_ttc: float

class FactureRepository(ABC):
    """
    Port de lecture — indépendant de tout moteur.
    Le domaine dépend de cette interface, jamais de BigQuery ou Snowflake.
    """
    @abstractmethod
    def get_factures_by_date(self, date: str) -> list[Facture]:
        pass

class FactureService:
    """
    Le domaine dépend du Port (abstraction), jamais de l'Adapter (implémentation).
    C'est le DIP : les deux dépendent de FactureRepository, pas l'un de l'autre.
    """
    def __init__(self, repository: FactureRepository):
        self._repository = repository

    def calculer_chiffre_affaires(self, date: str) -> float:
        factures = self._repository.get_factures_by_date(date)
        return sum(f.montant_ttc for f in factures)

# ------------------------------------------------------------------------------
# 2. LES ADAPTERS (Implémentations spécifiques)
# ------------------------------------------------------------------------------

# ❌ L'ANTI-PATTERN (Ce qu'il ne faut pas faire)
def charger_factures_bad_practice(date: str) -> list[dict]:
    # ❌ Double anti-pattern : Injection SQL ET SELECT * (contrat fragile)
    query = f"SELECT * FROM `prd-sales.prd_sales_domain.fctFacturesEnrichies` WHERE dateCommande = '{date}'"
    ...

class BigQueryFactureRepository(FactureRepository):
    """
    Adapter BigQuery — implémente le Port.
    C'est le seul endroit où BigQuery est mentionné.
    """
    def __init__(self, project: str, dataset: str):
        self._project = project
        self._dataset = dataset
        # self._client = bigquery.Client(project=project)  # ✅ Décommentez en production
        pass

    def get_factures_by_date(self, date: str) -> list[Facture]:
        # ✅ CRAFT PATTERN : Injection de la configuration (IaC)
        # Le nom du projet et du dataset ne sont JAMAIS hardcodés.
        query = f"""
            SELECT factureId, montantTtc 
            FROM `{self._project}.{self._dataset}.fctFacturesEnrichies`
            WHERE dateCommande = @date_commande
        """
        # ✅ CRAFT PATTERN : Utilisation des Query Parameters pour éviter l'injection SQL
        # et permettre au moteur BigQuery de mettre le plan d'exécution en cache.
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("date_commande", "DATE", date)
            ]
        )
        
        # return [
        #     Facture(facture_id=row.factureId, montant_ttc=float(row.montantTtc))
        #     for row in self._client.query(query, job_config=job_config)
        # ]
        return [] # Mock pour l'exemple

class DuckDBFactureRepository(FactureRepository):
    """
    Adapter de test — même Port, moteur local et léger.
    Permet de tester le domaine en millisecondes dans la CI/CD.
    """
    def __init__(self, db_path: str = ":memory:"):
        self._conn = duckdb.connect(db_path)

    def get_factures_by_date(self, date: str) -> list[Facture]:
        # Implémentation DuckDB (sécurisée avec des paramètres)
        # return [Facture(...) for row in self._conn.execute("...", [date]).fetchall()]
        return [Facture(facture_id="F-MOCK-1", montant_ttc=150.0)]

# ------------------------------------------------------------------------------
# 3. L'INJECTION (L'Assemblage au démarrage de l'application ou du DAG)
# ------------------------------------------------------------------------------

def main():
    # L'infrastructure est définie par l'environnement (IaC / Variables d'environnement)
    env = os.getenv("ENV", "DEV")
    
    # Choix de l'Adapter (Le seul endroit où l'on prend une décision technique)
    if env == "PRD":
        repository = BigQueryFactureRepository(
            project="lm-data-prd", 
            dataset="prd_sales_domain"
        )
    else:
        # En DEV ou en Test, on utilise la base locale ultra-rapide
        repository = DuckDBFactureRepository()

    # Injection du Repository dans le Service Métier
    service = FactureService(repository)

    # Exécution de la logique métier (Le Domaine ne sait pas si c'est BigQuery ou DuckDB !)
    chiffre_affaires = service.calculer_chiffre_affaires("2026-03-22")
    print(f"Chiffre d'affaires : {chiffre_affaires} €")

if __name__ == "__main__":
    main()