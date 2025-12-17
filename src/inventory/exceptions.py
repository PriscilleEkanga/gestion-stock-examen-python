"""
exceptions.py — Exceptions métier

Pourquoi ce fichier ?
- Avoir des erreurs “propres” et lisibles.
- Ne pas remonter des erreurs SQLite/JSON brutes à l’utilisateur.
- Faciliter la gestion d’erreurs dans `cli.py`.

Ce que vous devez faire :
- Réutiliser ces exceptions dans vos TODO (CRUD, vente, dashboard, export).
"""


class InventoryError(Exception):
    """Erreur applicative générique."""


class ValidationError(InventoryError):
    """Entrée invalide."""


class NotFoundError(InventoryError):
    """Ressource introuvable."""


class StockError(InventoryError):
    """Problème de stock (insuffisant, etc.)."""


class DataImportError(InventoryError):
    """Erreur lors de l'import JSON."""


class DatabaseError(InventoryError):
    """Erreur DB."""
