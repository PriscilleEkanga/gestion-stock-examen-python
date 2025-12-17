"""
models.py — Modèles de données (POO + type hints)

Pourquoi ce fichier ?
- Représenter les objets métier (Produit, Vente).
- Fournir des structures typées et lisibles.
- Faciliter tests unitaires et échanges entre couches (service/repository).

Ce que vous devez faire :
- Ajouter/ajuster des champs si nécessaire (ex: created_at, updated_at).
- Utiliser ces classes partout au lieu de manipuler des dicts bruts.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass(frozen=True)
class Product:
    """Un produit stocké dans la table `products`."""

    sku: str
    name: str
    category: str
    unit_price_ht: float
    quantity: int
    vat_rate: float = 0.20
    id: Optional[int] = None
    created_at: Optional[str] = None


@dataclass(frozen=True)
class Sale:
    """Une vente enregistrée dans la table `sales` (à compléter en exercice)."""

    product_id: int
    sku: str
    quantity: int
    unit_price_ht: float
    vat_rate: float
    total_ht: float
    total_vat: float
    total_ttc: float
    sold_at: str


def now_iso() -> str:
    """Date UTC ISO-8601 (ex: 2025-12-13T12:00:00Z)."""
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
