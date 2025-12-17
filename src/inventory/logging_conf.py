"""
logging_conf.py — Configuration du logging

Pourquoi ce fichier ?
- Exiger un logging propre est une bonne pratique d’ingénierie.
- Facilite le debug et la traçabilité.
- Fait partie des critères d’évaluation.

Ce que vous devez faire :
- Garder ce module et utiliser logging dans vos services.
- Ajouter des logs utiles (INFO pour actions, WARNING pour entrées suspectes,
  ERROR pour exceptions).

Note :
- Ce starter kit configure console + fichier (rotation).
"""

from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler


def configure_logging(log_level: str = "INFO", log_file: str = "inventory.log") -> None:
    """Configure un logging simple : console + fichier avec rotation."""
    level = getattr(logging, log_level.upper(), logging.INFO)

    logger = logging.getLogger()
    logger.setLevel(level)

    # Éviter doublons si reconfig
    if logger.handlers:
        return

    fmt = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    ch = logging.StreamHandler()
    ch.setLevel(level)
    ch.setFormatter(fmt)

    fh = RotatingFileHandler(log_file, maxBytes=500_000, backupCount=3, encoding="utf-8")
    fh.setLevel(level)
    fh.setFormatter(fmt)

    logger.addHandler(ch)
    logger.addHandler(fh)
