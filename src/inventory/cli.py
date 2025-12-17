"""
cli.py — Interface console (menu interactif)
"""

from __future__ import annotations

import argparse
import logging

from .config import AppConfig
from .exceptions import (
    DataImportError,
    DatabaseError,
    InventoryError,
    ValidationError,
    NotFoundError,
    StockError
)
from .logging_conf import configure_logging
from .services import InventoryManager
from .utils import format_table, to_float, to_int

logger = logging.getLogger(__name__)


def _prompt(text: str) -> str:
    return input(text).strip()


def print_menu() -> None:
    print("\n=== Gestion de stock (JSON → SQLite) ===")
    print("1) Initialiser le stock (depuis un JSON)")
    print("2) Afficher l’inventaire")
    print("3) Ajouter un produit")
    print("4) Modifier un produit")
    print("5) Supprimer un produit")
    print("6) Vendre un produit")
    print("7) Tableau de bord")
    print("8) Quitter")


def action_initialize(app: InventoryManager) -> None:
    path = _prompt("Chemin du fichier JSON (ex: data/initial_stock.json) : ")
    count = app.initialize_from_json(path)
    print(f"✅ Initialisation réussie : {count} produit(s) importé(s).")


def action_list_inventory(app: InventoryManager) -> None:
    products = app.list_inventory()
    headers = ["SKU", "Nom", "Catégorie", "Prix HT", "TVA", "Stock"]
    rows = []
    for p in products:
        rows.append([
            p.sku, p.name, p.category, 
            f"{p.unit_price_ht:.2f}€", f"{p.vat_rate*100:.0f}%", str(p.quantity)
        ])
    print("\n--- Inventaire actuel ---")
    print(format_table(headers, rows))


def action_add_product(app: InventoryManager) -> None:
    print("\n--- Ajouter un produit ---")
    sku = _prompt("SKU : ")
    name = _prompt("Nom : ")
    cat = _prompt("Catégorie : ")
    price = to_float(_prompt("Prix HT : "), "Prix HT")
    qty = to_int(_prompt("Quantité initiale : "), "Quantité")
    vat_str = _prompt("TVA (ex: 0.20, laisser vide pour 20% par défaut) : ")
    vat = to_float(vat_str, "TVA") if vat_str else None
    
    app.add_product(sku, name, cat, price, qty, vat)
    print(f"✅ Produit {sku} ajouté avec succès.")


def action_update_product(app: InventoryManager) -> None:
    sku = _prompt("SKU du produit à modifier : ")
    print("Entrez les nouvelles valeurs :")
    name = _prompt("Nouveau nom : ")
    cat = _prompt("Nouvelle catégorie : ")
    price = to_float(_prompt("Nouveau prix HT : "), "Prix HT")
    qty = to_int(_prompt("Nouveau stock : "), "Quantité")
    vat = to_float(_prompt("Nouveau taux TVA (ex: 0.20) : "), "TVA")
    
    app.update_product(sku, name, cat, price, qty, vat)
    print(f"✅ Produit {sku} mis à jour.")


def action_delete_product(app: InventoryManager) -> None:
    sku = _prompt("SKU du produit à supprimer : ")
    confirm = _prompt(f"Confirmer la suppression de {sku} ? (y/n) : ")
    if confirm.lower() == 'y':
        app.delete_product(sku)
        print(f"✅ Produit {sku} supprimé.")


def action_sell_product(app: InventoryManager) -> None:
    sku = _prompt("SKU du produit vendu : ")
    qty = to_int(_prompt("Quantité vendue : "), "Quantité")
    
    res = app.sell_product(sku, qty)
    print("\n--- Ticket de vente ---")
    print(f"Produit : {res['sku']}")
    print(f"Quantité : {res['qty']}")
    print(f"Total HT : {res['total_ht']:.2f}€")
    print(f"TVA : {res['total_vat']:.2f}€")
    print(f"Total TTC : {res['total_ttc']:.2f}€")
    print("-----------------------")


def action_dashboard(app: InventoryManager) -> None:
    stats = app.get_dashboard_data()
    print("\n=== TABLEAU DE BORD DES VENTES ===")
    print(f"Nombre de ventes : {stats['nb_sales']}")
    print(f"Articles vendus : {stats['total_qty']}")
    print(f"Chiffre d'affaires HT  : {stats['total_ht']:.2f}€")
    print(f"Total TVA collectée    : {stats['total_vat']:.2f}€")
    print(f"Chiffre d'affaires TTC : {stats['total_ttc']:.2f}€")
    print("==================================")


def main() -> int:
    parser = argparse.ArgumentParser(description="Gestion de stock CLI")
    parser.add_argument("--db", default="data/inventory.db", help="Chemin vers la base SQLite")
    parser.add_argument("--log-level", default="INFO", help="Niveau de log (DEBUG, INFO, etc.)")
    args = parser.parse_args()

    configure_logging(log_level=args.log_level)
    config = AppConfig(db_path=args.db)
    app = InventoryManager(config)

    logger.info("App started with db=%s", config.db_path)

    while True:
        try:
            print_menu()
            choice = _prompt("Votre choix (1-8) : ")

            if choice == "1": action_initialize(app)
            elif choice == "2": action_list_inventory(app)
            elif choice == "3": action_add_product(app)
            elif choice == "4": action_update_product(app)
            elif choice == "5": action_delete_product(app)
            elif choice == "6": action_sell_product(app)
            elif choice == "7": action_dashboard(app)
            elif choice == "8":
                print("Au revoir.")
                return 0
            else:
                print("Choix invalide.")

        except (ValidationError, DataImportError, NotFoundError, StockError) as e:
            print(f"⚠️ Erreur : {e}")
        except DatabaseError as e:
            logger.error("Database error: %s", e)
            print(f"❌ Erreur base de données : {e}")
        except Exception as e:
            logger.exception("Unexpected error")
            print(f"🔥 Erreur inattendue : {e}")

if __name__ == "__main__":
    import sys
    sys.exit(main())