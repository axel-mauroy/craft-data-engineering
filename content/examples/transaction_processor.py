❌ Le Code "Cauchemar" (Haute Complexité & Obscurité)
Ce code cumule les tares : variables globales, index magiques, logique métier dispersée et dépendances cachées.

import csv

# 1. Global state: High cognitive load. 
# Who modifies this? When? We don't know.
DATA_STORE = {}
# 2. Obscurity: What is 'c'? (It's a conversion rate, but nothing says so)
C = 1.12 

def p_data(f):
    """
    Process file. 
    Symptoms: Change amplification, Obscurity, Unknown Unknowns.
    """
    with open(f, 'r') as r:
        for l in csv.reader(r):
            # 3. Index dependency: If a column is added to the CSV, 
            # this breaks silently or calculates wrong values (Unknown Unknown).
            # l[2] is 'amount', l[5] is 'currency_code', l[0] is 'client_id'
            if l[5] == 'USD':
                v = float(l[2]) * C
            else:
                v = float(l[2])
            
            # 4. Hidden side effect: Modifying global state.
            if l[0] not in DATA_STORE:
                DATA_STORE[l[0]] = 0
            DATA_STORE[l[0]] += v
            
            # 5. Hardcoded business logic: 
            # If the threshold changes, we must find every file doing this.
            if v > 10000:
                print(f"ALERT for {l[0]}") 

# Problem: If tomorrow 'l[2]' becomes 'net_amount' and 'l[3]' is 'tax',
# this function will continue to run but will produce WRONG financial data.
Pourquoi ce code est "toxique" selon Ousterhout :
- Inconnues inconnues : Le développeur qui ajoute une colonne au fichier CSV ne sait pas que p_data utilise l'index [5]. Le bug n'apparaîtra que lors du prochain audit financier.
- Charge cognitive : Tu dois mémoriser que l[2] est le montant et C le taux de conversion. Ton cerveau sert de base de données de documentation.
- Dépendance forte : La fonction est couplée à la fois au format du fichier, à la logique de conversion et à une variable globale.


✅ La Version "Data Craftsman" (Simple & Robuste)
Ici, on utilise le Domain Driven Design (DDD) simplifié : on sépare la lecture, la validation (via une classe) et le calcul.
Note : Pour gérer ce projet, utilise uv init puis uv add pydantic pour une validation de données encore plus poussée.

from dataclasses import dataclass
from typing import Dict, List, Final
from enum import Enum

class Currency(Enum):
    USD = "USD"
    EUR = "EUR"

@dataclass(frozen=True)
class Transaction:
    """
    Domain object representing a financial transaction.
    Reduces obscurity by naming every field and typing it.
    """
    client_id: str
    amount: float
    currency: Currency

    @property
    def amount_in_eur(self) -> float:
        # Centralized logic: Change amplification is reduced.
        USD_TO_EUR_RATE: Final = 0.89 
        if self.currency == Currency.USD:
            return self.amount * USD_TO_EUR_RATE
        return self.amount

class TransactionProcessor:
    def __init__(self, alert_threshold: float = 10000.0):
        self.alert_threshold = alert_threshold
        self.client_totals: Dict[str, float] = {}

    def process_transactions(self, transactions: List[Transaction]) -> None:
        """
        Business logic is now obvious and isolated from the data source format.
        """
        for tx in transactions:
            eur_amount = tx.amount_in_eur
            
            # Update internal state (no globals)
            self.client_totals[tx.client_id] = self.client_totals.get(tx.client_id, 0.0) + eur_amount
            
            if eur_amount > self.alert_threshold:
                self._trigger_alert(tx.client_id, eur_amount)

    def _trigger_alert(self, client_id: str, amount: float) -> None:
        # Logic for alerting is encapsulated
        print(f"[ALERT] High value transaction for {client_id}: {amount:.2f} EUR")

# Why this is better:
# 1. Obviousness: 'amount_in_eur' is explicit.
# 2. Safety: If the CSV format changes, only the 'adapter' (the code reading the CSV) 
# needs to be updated. The 'Transaction' class and the 'Processor' remain intact.