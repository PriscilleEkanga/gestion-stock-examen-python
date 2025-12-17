"""
utils.py — Fonctions utilitaires (validation, calcul, affichage, import JSON)

Pourquoi ce fichier ?
- Éviter de dupliquer des validations partout.
- Centraliser les calculs (HT/TVA/TTC) et conversions de types.
- Améliorer la qualité et la testabilité.

Ce qui est déjà fait :
- Import JSON + validation structure
- Calcul HT/TVA/TTC
- Affichage tabulaire monospace

Ce que vous devez faire :
- Enrichir progressivement ces validations selon vos besoins (CRUD, vente, etc.).
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Tuple

from .exceptions import DataImportError, ValidationError


def ensure_file_exists(path: str) -> None:
    if not path or not os.path.isfile(path):
        raise ValidationError(f"Fichier introuvable: {path}")


def to_float(value: Any, field: str) -> float:
    try:
        return float(value)
    except Exception as e:
        raise ValidationError(f"Champ '{field}' invalide (float attendu).") from e


def to_int(value: Any, field: str) -> int:
    try:
        return int(value)
    except Exception as e:
        raise ValidationError(f"Champ '{field}' invalide (int attendu).") from e


def validate_sku(sku: str) -> str:
    sku = (sku or "").strip()
    if not sku:
        raise ValidationError("SKU obligatoire.")
    return sku


def validate_non_empty(text: str, field: str) -> str:
    text = (text or "").strip()
    if not text:
        raise ValidationError(f"{field} obligatoire.")
    return text


def validate_vat_rate(rate: float) -> float:
    if rate < 0 or rate > 1:
        raise ValidationError("TVA invalide (attendu entre 0 et 1).")
    return rate


def validate_unit_price_ht(price: float) -> float:
    if price < 0:
        raise ValidationError("Prix HT invalide (>= 0).")
    return price


def validate_quantity(qty: int, allow_zero: bool = True) -> int:
    if allow_zero:
        if qty < 0:
            raise ValidationError("Quantité invalide (>= 0).")
    else:
        if qty <= 0:
            raise ValidationError("Quantité invalide (> 0).")
    return qty


def calc_totals(unit_price_ht: float, quantity: int, vat_rate: float) -> Tuple[float, float, float]:
    """Calcule HT/TVA/TTC avec arrondi à 2 décimales."""
    total_ht = round(unit_price_ht * quantity, 2)
    total_vat = round(total_ht * vat_rate, 2)
    total_ttc = round(total_ht + total_vat, 2)
    return total_ht, total_vat, total_ttc


def load_initial_json(path: str) -> Dict[str, Any]:
    """Charge et valide le JSON d'initialisation fourni par l'utilisateur."""
    ensure_file_exists(path)
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        raise DataImportError("JSON invalide (erreur de parsing).") from e
    except Exception as e:
        raise DataImportError("Impossible de lire le fichier JSON.") from e

    if not isinstance(data, dict):
        raise DataImportError("JSON invalide: objet racine attendu.")

    products = data.get("products")
    if not isinstance(products, list) or not products:
        raise DataImportError("JSON invalide: 'products' doit être une liste non vide.")

    vat_default = validate_vat_rate(to_float(data.get("vat_rate_default", 0.20), "vat_rate_default"))

    seen: set[str] = set()
    normalized: List[Dict[str, Any]] = []
    for i, p in enumerate(products, start=1):
        if not isinstance(p, dict):
            raise DataImportError(f"Produit #{i} invalide: objet attendu.")

        sku = validate_sku(str(p.get("sku", "")))
        if sku in seen:
            raise DataImportError(f"SKU dupliqué: {sku}")
        seen.add(sku)

        name = validate_non_empty(str(p.get("name", "")), "name")
        category = validate_non_empty(str(p.get("category", "")), "category")
        unit_price_ht = validate_unit_price_ht(to_float(p.get("unit_price_ht", None), "unit_price_ht"))
        quantity = validate_quantity(to_int(p.get("quantity", None), "quantity"), allow_zero=True)
        vat_rate = validate_vat_rate(to_float(p.get("vat_rate", vat_default), "vat_rate"))

        normalized.append({
            "sku": sku,
            "name": name,
            "category": category,
            "unit_price_ht": unit_price_ht,
            "quantity": quantity,
            "vat_rate": vat_rate,
        })

    return {"vat_rate_default": vat_default, "products": normalized}


def format_table(headers: List[str], rows: List[List[str]]) -> str:
    """Petit rendu tabulaire en monospace (stdlib only)."""
    if not rows:
        return "(aucune donnée)"

    widths = [len(h) for h in headers]
    for r in rows:
        for i, cell in enumerate(r):
            widths[i] = max(widths[i], len(cell))

    def fmt_row(r: List[str]) -> str:
        return " | ".join(cell.ljust(widths[i]) for i, cell in enumerate(r))

    sep = "-+-".join("-" * w for w in widths)
    out = [fmt_row(headers), sep]
    out.extend(fmt_row(r) for r in rows)
    return "\n".join(out)
