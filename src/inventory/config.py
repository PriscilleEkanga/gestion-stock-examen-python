"""
config.py — Configuration applicative

Pourquoi ce fichier ?
- Centraliser les paramètres : chemin DB, TVA par défaut, etc.
- Éviter des constantes “magiques” dispersées.

Ce que vous devez faire :
- Ajouter ici d’autres options si nécessaire (ex: chemin logs, séparateur CSV, etc.).
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class AppConfig:
    """Configuration de l'application."""
    db_path: str
    default_vat_rate: float = 0.20
