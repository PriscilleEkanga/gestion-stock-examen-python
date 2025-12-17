# Examen — Exercice 2 : Application CLI de gestion de stock (starter kit)

Ce dépôt est une **base de départ** fournie pour vous faire gagner du temps sur :
- la structure du projet (package Python + dossier `src/` + dossier `tests/`),
- la première version du **menu CLI**,
- la fonctionnalité **1) Initialiser** (JSON → SQLite),
- la fonctionnalité **2) Afficher l’inventaire** (lecture SQLite + affichage tabulaire).

Tout le reste (CRUD, vente, dashboard, export, améliorations UX, tests, etc.) est **à implémenter par vous**,
progressivement, en respectant les bonnes pratiques demandées dans l’énoncé.

---

## 1) Prérequis

- Python **3.10+** (recommandé 3.11 / 3.12)
- Aucune dépendance externe : **stdlib only**

Vérifiez votre version :
```bash
python --version
# ou
python3 --version
```

---

## 2) Structure du projet (à comprendre avant de coder)

- `src/inventory/` : le **package principal** de l’application (le “vrai code” est ici)
- `src/run_inventory.py` : **script de lancement** simplifié (méthode la plus simple)
- `tests/` : vos **tests unitaires** (à compléter)
- `data/` : fichiers de données (JSON d’initialisation, DB SQLite, exports)

Important : ce projet utilise un layout `src/`.
Cela signifie que, par défaut, Python ne “voit” pas automatiquement le package `inventory` si vous exécutez depuis la racine.
C’est précisément pour ça que nous proposons **2 méthodes de lancement** ci-dessous.

---

## 3) Lancer l’application (mode interactif, menu)

### Méthode 1 (RECOMMANDÉE) — La plus simple : `run_inventory.py`

Cette méthode est la plus simple pour VS Code et pour éviter les problèmes d’imports.

#### A) Depuis VS Code (bouton Run)
1) Ouvrez **le dossier racine** du projet dans VS Code (celui qui contient `src/`, `data/`, `tests/`)
2) Ouvrez le fichier :
   - `src/run_inventory.py`
3) Cliquez sur ▶️ **Run Python File** (ou F5 selon votre configuration)

#### B) Depuis le terminal
Depuis la racine du projet :

```bash
python src/run_inventory.py
```

✅ Résultat attendu : vous voyez un menu avec 8 choix  
(Seuls les choix **1** et **2** sont fonctionnels dans ce starter kit.)

---

### Méthode 2 — Lancement “standard” du package avec `PYTHONPATH` (utile à connaître)

Cette méthode lance directement le package comme un module Python (bonne pratique), mais nécessite de dire à Python
que `src/` fait partie du chemin des modules.

Depuis la **racine** du projet :

#### macOS / Linux
```bash
PYTHONPATH=./src python3 -m inventory --db data/inventory.db
```

#### Windows (PowerShell)
```powershell
$env:PYTHONPATH = ".\src"
python -m inventory --db data\inventory.db
```

#### Windows (cmd.exe)
```bat
set PYTHONPATH=.\src
python -m inventory --db data\inventory.db
```

---

## 4) Fonctionnalité 1 — Initialiser le stock (JSON → SQLite)

Objectif :
- (re)créer la base `data/inventory.db`
- créer les tables `products` et `sales`
- insérer les produits depuis un fichier JSON

### Comment tester
1) Lancez l’app (méthode 1 ou 2)
2) Choisissez **1) Initialiser**
3) Donnez le chemin :
   - `data/initial_stock.json` (fourni)
   - (vous pouvez aussi entrer un autre fichier JSON si demandé)

Résultat attendu :
- Message : `Initialisation réussie : X produit(s) importé(s).`
- Un fichier `data/inventory.db` apparaît (ou est recréé)

En cas d’erreur (fichier manquant, JSON invalide, champs manquants) :
- un message clair doit être affiché (une partie est déjà gérée dans ce starter kit)

---

## 5) Fonctionnalité 2 — Afficher l’inventaire (SQLite)

Objectif :
- lire la table `products`
- afficher une table en console (SKU, nom, catégorie, prix, stock…)

### Comment tester
1) Faites l’initialisation (fonctionnalité 1)
2) Choisissez **2) Afficher l’inventaire**

Résultat attendu :
- Une table s’affiche avec au moins 8 lignes (les produits du JSON)

---

## 6) Ce que vous devez implémenter ensuite (à partir de ce starter kit)

Les fonctionnalités suivantes sont **volontairement laissées en TODO** :
3) Ajouter un produit  
4) Modifier un produit  
5) Supprimer un produit  
6) Vendre un produit (transaction atomique + calcul HT/TVA/TTC)  
7) Tableau de bord (totaux HT/TVA/TTC + bonus) + export CSV ventes  
8) Quitter (déjà fait)

Bonnes pratiques attendues :
- gestion d’erreurs, messages utilisateurs clairs
- logging (fichier + console), niveaux corrects
- type hints, docstrings, commentaires utiles
- code organisé en fonctions, classes, modules
- tests unitaires progressifs (dossier `tests/`)
- Git : **commits réguliers** (au minimum 8 commits) avec messages pertinents

---

## 7) Exécuter les tests

Les tests fournis sont volontairement minimalistes (starter). Vous devez en ajouter.

### Méthode (recommandée) : avec `PYTHONPATH` (car les tests importent le package)

#### macOS / Linux
```bash
PYTHONPATH=./src python3 -m unittest -v
```

#### Windows (PowerShell)
```powershell
$env:PYTHONPATH = ".\src"
python -m unittest -v
```

#### Windows (cmd.exe)
```bat
set PYTHONPATH=.\src
python -m unittest -v
```

---

## 8) Dépannage rapide (si ça ne marche pas)

### Erreur : `No module named inventory`
- Vous n’avez pas lancé via la bonne méthode, ou `PYTHONPATH` n’est pas défini.
- Solution simple : lancez `python src/run_inventory.py`.

### VS Code : imports soulignés en rouge
- Assurez-vous d’avoir ouvert le **dossier racine** du projet
- Vérifiez que `.vscode/settings.json` existe et contient :
  ```json
  { "python.analysis.extraPaths": ["./src"] }
  ```

---

## 9) Conseil de démarche (très important)

Travaillez en étapes courtes :
1) Petit ajout de code
2) Test manuel ou test unitaire
3) Commit Git (message clair)
4) Recommencez

Un dépôt mono-commit ou 1–2 commits massifs ressemble fortement à un code généré entièrement par IA,
et sera pénalisé selon l’énoncé.

---

## 10) Checklist avant rendu (très courte)

Avant de rendre, vérifiez **rapidement** les points suivants :

- **Dépôt GitHub** : lien prêt à être partagé, dépôt accessible, README à jour
- **Historique Git** : au moins **8 commits** (petits commits réguliers, messages explicites)
- **Lancement** : l’app démarre via `python src/run_inventory.py` (ou via `PYTHONPATH`)
- **Initialisation** : menu → `1) Initialiser` avec `data/initial_stock.json` fonctionne
- **Inventaire** : menu → `2) Afficher l’inventaire` affiche une table correcte
- **Base SQLite** : fichier DB présent (ex: `data/inventory.db`), contraintes OK, FK activées
- **Erreurs** : messages clairs (fichier JSON manquant/invalide, saisies incorrectes, etc.)
- **Logging** : fichier `inventory.log` généré, logs utiles (INFO/WARNING/ERROR)
- **Tests** : `python -m unittest -v` passe (et vous avez ajouté des tests pertinents)
- **Organisation** : code dans `src/inventory/`, pas de logique métier dans `run_inventory.py`
- **Données** : `data/` contient JSON/DB/export (chemins cohérents, pas de fichiers inutiles)

Bon rendu.
