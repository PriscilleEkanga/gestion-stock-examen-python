"""
run_inventory.py — Script de lancement de l'application de gestion de stock

RÔLE DE CE FICHIER
-----------------
Ce fichier est un petit script de lancement (launcher).
Il joue le même rôle que la commande avancée :

    python -m inventory

mais de façon beaucoup plus simple et accessible pour les étudiants.

POURQUOI CE FICHIER EXISTE ?
----------------------------
- Permet de lancer l'application très facilement depuis VS Code
  (bouton ▶️ « Run Python File » ou terminal).
- Évite toute configuration compliquée de PYTHONPATH.
- Garantit que l'import `import inventory` fonctionne correctement.
- Sert de point d’entrée pédagogique pour comprendre comment démarre
  une application Python bien structurée.

COMMENT ÇA FONCTIONNE ?
-----------------------
- Ce fichier doit être placé dans le dossier `src/`.
- Quand on exécute ce fichier, Python ajoute automatiquement `src/`
  au chemin de recherche des modules (sys.path).
- Cela permet d'importer le package `inventory` sans configuration
  supplémentaire.
- VS Code (via Run ou launch.json) peut invoquer directement ce script.
- Le script délègue ensuite l'exécution à la vraie interface CLI
  définie dans `inventory/cli.py`, et conserve correctement le code
  de sortie du programme.

COMMENT LANCER L’APPLICATION ?
------------------------------
Depuis VS Code :
- Ouvrir le projet à la racine
- Ouvrir le fichier `src/run_inventory.py`
- Cliquer sur ▶️ « Run Python File »

Depuis le terminal :
    python src/run_inventory.py

À NE PAS MODIFIER
-----------------
- Ce fichier ne contient AUCUNE logique métier.
- Toute la logique de l’application est dans le package `inventory/`.
- Vous devez implémenter les fonctionnalités demandées dans les fichiers
  du dossier `inventory/`, pas ici.

Ce fichier est volontairement simple et doit rester inchangé.
"""

from __future__ import annotations

from inventory.cli import main


if __name__ == "__main__":
    raise SystemExit(main())
