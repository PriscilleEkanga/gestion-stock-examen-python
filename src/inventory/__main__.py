"""
Point d’entrée du package lorsque vous exécutez : `python -m inventory`

Pourquoi ce fichier ?
- C’est la manière standard d’exécuter un package comme un module.
- Cela évite un script `main.py` unique et encourage une bonne organisation.

Ce que vous devez faire :
- Vous pouvez conserver ce fichier tel quel.
- Toute la logique est dans `cli.py`.

Exécution :
- python -m inventory --db data/inventory.db
"""

from __future__ import annotations

from .cli import main

if __name__ == "__main__":
    raise SystemExit(main())
