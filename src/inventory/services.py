"""
services.py — Logique métier (use-cases)
"""

from __future__ import annotations

import logging
from typing import List, Optional

from .config import AppConfig
from .models import Product, now_iso
from .repository import SQLiteRepository
from .utils import (
    load_initial_json, 
    validate_sku, 
    validate_non_empty, 
    validate_unit_price_ht, 
    validate_quantity, 
    validate_vat_rate,
    to_float,
    to_int
)
from .exceptions import NotFoundError, StockError, ValidationError

logger = logging.getLogger(__name__)


class InventoryManager:
    """Service principal orchestrant la logique métier."""

    def __init__(self, config: AppConfig, repo: Optional[SQLiteRepository] = None) -> None:
        self.config = config
        self.repo = repo or SQLiteRepository(config.db_path)

    def initialize_from_json(self, json_path: str, reset: bool = True) -> int:
        """Initialise la DB depuis un JSON."""
        logger.info("Initialization requested from JSON: %s", json_path)
        payload = load_initial_json(json_path)
        products = payload["products"]

        if reset:
            self.repo.reset_and_create_schema()
        else:
            self.repo.create_schema_if_needed()

        count = 0
        for p in products:
            prod = Product(
                sku=p["sku"],
                name=p["name"],
                category=p["category"],
                unit_price_ht=p["unit_price_ht"],
                quantity=p["quantity"],
                vat_rate=p["vat_rate"],
                created_at=now_iso(),
            )
            self.repo.insert_product(prod)
            count += 1

        logger.info("Initialization OK. %d products inserted.", count)
        return count

    def list_inventory(self) -> List[Product]:
        """Retourne la liste des produits."""
        self.repo.create_schema_if_needed()
        return self.repo.list_products()

    # --- NOUVEAUX USE-CASES (CRUD) ---

    def add_product(self, sku: str, name: str, cat: str, price: float, qty: int, vat: Optional[float] = None) -> Product:
        """Valide et ajoute un produit."""
        sku = validate_sku(sku)
        if self.repo.get_product_by_sku(sku):
            raise ValidationError(f"Le SKU '{sku}' existe déjà.")
        
        vat_val = vat if vat is not None else self.config.default_vat_rate
        
        p = Product(
            sku=sku,
            name=validate_non_empty(name, "nom"),
            category=validate_non_empty(cat, "catégorie"),
            unit_price_ht=validate_unit_price_ht(price),
            quantity=validate_quantity(qty, allow_zero=True),
            vat_rate=validate_vat_rate(vat_val),
            created_at=now_iso()
        )
        self.repo.insert_product(p)
        logger.info("Produit ajouté : %s", sku)
        return p

    def update_product(self, sku: str, name: str, cat: str, price: float, qty: int, vat: float) -> None:
        """Modifie un produit existant."""
        p = self.repo.get_product_by_sku(sku)
        if not p:
            raise NotFoundError(f"Produit avec SKU '{sku}' introuvable.")
        
        updated_p = Product(
            sku=sku,
            name=validate_non_empty(name, "nom"),
            category=validate_non_empty(cat, "catégorie"),
            unit_price_ht=validate_unit_price_ht(price),
            quantity=validate_quantity(qty, allow_zero=True),
            vat_rate=validate_vat_rate(vat),
            id=p.id,
            created_at=p.created_at
        )
        self.repo.update_product(updated_p)
        logger.info("Produit mis à jour : %s", sku)

    def delete_product(self, sku: str) -> None:
        """Supprime un produit."""
        p = self.repo.get_product_by_sku(sku)
        if not p:
            raise NotFoundError(f"Produit '{sku}' introuvable.")
        self.repo.delete_product(sku)
        logger.info("Produit supprimé : %s", sku)

    # --- LOGIQUE DE VENTE ---

    def sell_product(self, sku: str, qty_to_sell: int) -> dict:
        """Gère une vente : calculs financiers et mise à jour stock."""
        p = self.repo.get_product_by_sku(sku)
        if not p:
            raise NotFoundError(f"Produit '{sku}' introuvable.")
        
        if qty_to_sell <= 0:
            raise ValidationError("La quantité doit être supérieure à 0.")
        
        if p.quantity < qty_to_sell:
            raise StockError(f"Stock insuffisant pour {sku} (Disponible: {p.quantity})")

        # Calculs financiers (arrondis à 2 décimales pour la précision monétaire)
        total_ht = round(p.unit_price_ht * qty_to_sell, 2)
        total_vat = round(total_ht * p.vat_rate, 2)
        total_ttc = round(total_ht + total_vat, 2)

        self.repo.record_sale(
            product_id=p.id, # type: ignore
            sku=sku,
            qty=qty_to_sell,
            price_ht=p.unit_price_ht,
            vat_rate=p.vat_rate,
            t_ht=total_ht,
            t_vat=total_vat,
            t_ttc=total_ttc
        )
        
        return {
            "sku": sku,
            "qty": qty_to_sell,
            "total_ht": total_ht,
            "total_vat": total_vat,
            "total_ttc": total_ttc
        }

    def get_dashboard_data(self) -> dict:
        """Récupère les données pour le tableau de bord."""
        return self.repo.get_sales_stats()