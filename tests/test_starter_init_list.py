"""
tests/test_starter_init_list.py

Pourquoi ce fichier ?
- Exemple minimal de tests unitaires avec stdlib `unittest`.
- Vous devez enrichir les tests au fur et à mesure de vos implémentations.

Ce qui est testé ici :
- l'initialisation depuis JSON
- la lecture de l'inventaire

Ce que vous devez faire :
- Ajouter des tests pour CRUD, vente, dashboard, export, erreurs, etc.
"""

import json
import tempfile
import unittest
from pathlib import Path

from inventory.config import AppConfig
from inventory.services import InventoryManager


class TestStarterInitList(unittest.TestCase):
    def test_init_then_list(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            db_path = tmp_path / "test.db"
            json_path = tmp_path / "init.json"

            payload = {
                "vat_rate_default": 0.20,
                "products": [
                    {"sku": "P001", "name": "Produit A", "category": "Cat1", "unit_price_ht": 10.0, "quantity": 5},
                    {"sku": "P002", "name": "Produit B", "category": "Cat1", "unit_price_ht": 20.0, "quantity": 2},
                ],
            }
            json_path.write_text(json.dumps(payload), encoding="utf-8")

            app = InventoryManager(AppConfig(db_path=str(db_path)))
            count = app.initialize_from_json(str(json_path), reset=True)
            self.assertEqual(count, 2)

            products = app.list_inventory()
            self.assertEqual(len(products), 2)
            self.assertEqual(products[0].sku, "P001")
