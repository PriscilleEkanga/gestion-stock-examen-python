"""
repository.py — Accès aux données SQLite (DAO / Repository)
"""

from __future__ import annotations

import logging
import sqlite3
from contextlib import contextmanager
from typing import Iterable, List, Optional

from .exceptions import DatabaseError
from .models import Product, now_iso

logger = logging.getLogger(__name__)

SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS products (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  sku TEXT NOT NULL UNIQUE,
  name TEXT NOT NULL,
  category TEXT NOT NULL,
  unit_price_ht REAL NOT NULL CHECK(unit_price_ht >= 0),
  vat_rate REAL NOT NULL DEFAULT 0.20 CHECK(vat_rate >= 0 AND vat_rate <= 1),
  quantity INTEGER NOT NULL CHECK(quantity >= 0),
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sales (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  product_id INTEGER NOT NULL,
  sku TEXT NOT NULL,
  quantity INTEGER NOT NULL CHECK(quantity > 0),
  unit_price_ht REAL NOT NULL CHECK(unit_price_ht >= 0),
  vat_rate REAL NOT NULL CHECK(vat_rate >= 0 AND vat_rate <= 1),
  total_ht REAL NOT NULL CHECK(total_ht >= 0),
  total_vat REAL NOT NULL CHECK(total_vat >= 0),
  total_ttc REAL NOT NULL CHECK(total_ttc >= 0),
  sold_at TEXT NOT NULL,
  FOREIGN KEY(product_id) REFERENCES products(id) ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_products_sku ON products(sku);
CREATE INDEX IF NOT EXISTS idx_products_category ON products(category);
CREATE INDEX IF NOT EXISTS idx_sales_sku ON sales(sku);
"""

class SQLiteRepository:
    """Repository SQLite complet."""

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path

    @contextmanager
    def connect(self) -> Iterable[sqlite3.Connection]:
        """Connexion SQLite avec FK activées."""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA foreign_keys = ON")
            yield conn
        except sqlite3.Error as e:
            raise DatabaseError(f"Erreur SQLite: {e}") from e
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def reset_and_create_schema(self) -> None:
        """Supprime les tables puis recrée le schéma."""
        with self.connect() as conn:
            try:
                conn.execute("DROP TABLE IF EXISTS sales")
                conn.execute("DROP TABLE IF EXISTS products")
                conn.executescript(SCHEMA_SQL)
                conn.commit()
                logger.info("DB reset + schema created.")
            except sqlite3.Error as e:
                conn.rollback()
                raise DatabaseError(f"Erreur création schéma: {e}") from e

    def create_schema_if_needed(self) -> None:
        with self.connect() as conn:
            try:
                conn.executescript(SCHEMA_SQL)
                conn.commit()
            except sqlite3.Error as e:
                conn.rollback()
                raise DatabaseError(f"Erreur création schéma: {e}") from e

    # --- CRUD PRODUITS ---

    def insert_product(self, p: Product) -> int:
        with self.connect() as conn:
            try:
                cur = conn.execute(
                    """
                    INSERT INTO products(sku,name,category,unit_price_ht,vat_rate,quantity,created_at)
                    VALUES(?,?,?,?,?,?,?)
                    """,
                    (p.sku, p.name, p.category, p.unit_price_ht, p.vat_rate, p.quantity, p.created_at or now_iso()),
                )
                conn.commit()
                return int(cur.lastrowid)
            except sqlite3.IntegrityError as e:
                conn.rollback()
                raise DatabaseError(f"Contrainte violée (SKU unique ?) : {e}") from e
            except sqlite3.Error as e:
                conn.rollback()
                raise DatabaseError(f"Erreur insert produit: {e}") from e

    def get_product_by_sku(self, sku: str) -> Optional[Product]:
        """Récupère un produit unique par son SKU."""
        with self.connect() as conn:
            cur = conn.execute("SELECT * FROM products WHERE sku = ?", (sku,))
            row = cur.fetchone()
            if not row:
                return None
            return Product(
                id=row["id"], sku=row["sku"], name=row["name"], category=row["category"],
                unit_price_ht=row["unit_price_ht"], vat_rate=row["vat_rate"],
                quantity=row["quantity"], created_at=row["created_at"]
            )

    def update_product(self, p: Product) -> None:
        """Met à jour les informations d'un produit."""
        with self.connect() as conn:
            try:
                conn.execute(
                    """
                    UPDATE products 
                    SET name=?, category=?, unit_price_ht=?, vat_rate=?, quantity=?
                    WHERE sku=?
                    """,
                    (p.name, p.category, p.unit_price_ht, p.vat_rate, p.quantity, p.sku)
                )
                conn.commit()
            except sqlite3.Error as e:
                conn.rollback()
                raise DatabaseError(f"Erreur update produit: {e}") from e

    def delete_product(self, sku: str) -> None:
        """Supprime un produit (échoue si lié à une vente)."""
        with self.connect() as conn:
            try:
                conn.execute("DELETE FROM products WHERE sku = ?", (sku,))
                conn.commit()
            except sqlite3.IntegrityError:
                conn.rollback()
                raise DatabaseError("Impossible de supprimer : ce produit est lié à des ventes.")
            except sqlite3.Error as e:
                conn.rollback()
                raise DatabaseError(f"Erreur suppression: {e}")

    def list_products(self) -> List[Product]:
        with self.connect() as conn:
            cur = conn.execute("SELECT * FROM products ORDER BY sku ASC")
            return [Product(
                id=row["id"], sku=row["sku"], name=row["name"], category=row["category"],
                unit_price_ht=row["unit_price_ht"], vat_rate=row["vat_rate"],
                quantity=row["quantity"], created_at=row["created_at"]
            ) for row in cur.fetchall()]

    # --- VENTES & DASHBOARD ---

    def record_sale(self, product_id: int, sku: str, qty: int, price_ht: float, 
                    vat_rate: float, t_ht: float, t_vat: float, t_ttc: float) -> None:
        """Enregistre une vente et décrémente le stock (Transaction Atomique)."""
        with self.connect() as conn:
            try:
                # 1. On décrémente le stock
                conn.execute(
                    "UPDATE products SET quantity = quantity - ? WHERE id = ?",
                    (qty, product_id)
                )
                # 2. On insère la vente
                conn.execute(
                    """
                    INSERT INTO sales(product_id, sku, quantity, unit_price_ht, vat_rate, 
                                      total_ht, total_vat, total_ttc, sold_at)
                    VALUES(?,?,?,?,?,?,?,?,?)
                    """,
                    (product_id, sku, qty, price_ht, vat_rate, t_ht, t_vat, t_ttc, now_iso())
                )
                conn.commit()
            except sqlite3.Error as e:
                conn.rollback()
                raise DatabaseError(f"Erreur lors de la transaction de vente: {e}")

    def get_sales_stats(self) -> dict:
        """Récupère les agrégations pour le dashboard."""
        with self.connect() as conn:
            cur = conn.execute("""
                SELECT 
                    COUNT(*) as nb_sales,
                    SUM(quantity) as total_qty,
                    SUM(total_ht) as total_ht,
                    SUM(total_vat) as total_vat,
                    SUM(total_ttc) as total_ttc
                FROM sales
            """)
            row = cur.fetchone()
            return dict(row) if row["nb_sales"] > 0 else {
                "nb_sales": 0, "total_qty": 0, "total_ht": 0.0, "total_vat": 0.0, "total_ttc": 0.0
            }