# Projet Big Data Processing

## Participants

- Grégoire CHAMPOURLIER (`ApparentlyGregz`) — adresse e-mail à compléter.
- Hippolyte NUSSBAUMER (`hippolyte-coder`) — adresse e-mail à compléter.

## Prérequis

- Docker Desktop avec le moteur Linux démarré.
- PowerShell pour les commandes ci-dessous.

## Données IMDb

Les sept archives TSV compressées proviennent des [données IMDb non commerciales](https://data.imdb.com/non-commercial-datasets/). Elles ne sont pas versionnées dans Git. Les fichiers locaux utilisés pour le développement ont été téléchargés le 9 novembre 2026.

Depuis la racine du dépôt, télécharger les données dans le répertoire attendu :

```powershell
$raw = "Project_NUSSBAUMER_CHAMPOURLIER/data/raw"
New-Item -ItemType Directory -Force $raw | Out-Null
@(
  "name.basics.tsv.gz"
  "title.akas.tsv.gz"
  "title.basics.tsv.gz"
  "title.crew.tsv.gz"
  "title.episode.tsv.gz"
  "title.principals.tsv.gz"
  "title.ratings.tsv.gz"
) | ForEach-Object {
  Invoke-WebRequest -Uri "https://datasets.imdbws.com/$_" -OutFile (Join-Path $raw $_)
}
```

## Vérifier le chargement Spark

Depuis la racine du dépôt :

```powershell
$projectPath = (Resolve-Path ./Project_NUSSBAUMER_CHAMPOURLIER).Path
docker run --rm --mount "type=bind,source=$projectPath,target=/work" -w /work quay.io/jupyter/pyspark-notebook@sha256:7e102d041c42cca6e0d32269cded10fad2c377aeb09fad1ec546840660d691c4 python src/imdb_loader.py
```

Cette image contient PySpark 4.2.0. Le script vérifie la présence des sept archives, contrôle leurs en-têtes dans Spark et lit une ligne de chaque table. Les colonnes sont chargées comme chaînes de caractères ; les valeurs IMDb `\N` sont traitées comme manquantes. Les guillemets présents dans les titres sont conservés comme du texte TSV. Les conversions numériques auront lieu dans les traitements Spark ultérieurs.

## Vérifications

Les tests ciblés utilisent de petites archives créées temporairement. Le contrôle complet lit toutes les lignes et toutes les colonnes des sept fichiers téléchargés :

```powershell
$projectPath = (Resolve-Path ./Project_NUSSBAUMER_CHAMPOURLIER).Path
$image = "quay.io/jupyter/pyspark-notebook@sha256:7e102d041c42cca6e0d32269cded10fad2c377aeb09fad1ec546840660d691c4"
docker run --rm --mount "type=bind,source=$projectPath,target=/work" -w /work $image python -m unittest discover -s test -p 'test_*.py'
docker run --rm --mount "type=bind,source=$projectPath,target=/work" -w /work $image python -m test.check_full_data
```

## Préparation et qualité des données

Depuis la racine du dépôt, après le téléchargement des archives :

```powershell
$projectPath = (Resolve-Path ./Project_NUSSBAUMER_CHAMPOURLIER).Path
$image = "quay.io/jupyter/pyspark-notebook@sha256:7e102d041c42cca6e0d32269cded10fad2c377aeb09fad1ec546840660d691c4"
docker run --rm --mount "type=bind,source=$projectPath,target=/work" -w /work $image spark-submit --driver-memory 4g --conf spark.sql.shuffle.partitions=32 src/data_quality.py
```

Le résultat est enregistré dans `Project_NUSSBAUMER_CHAMPOURLIER/results/data_quality.json`. Le script conserve les colonnes brutes, ajoute des colonnes nettoyées et des indicateurs d'anomalies dans Spark, et sépare les références valides des références absentes. Les tableaux finaux et leur interprétation seront rédigés par le groupe dans `REPORT.md`.

## Usage d'outils d'IA

OpenAI Codex a aidé à écrire le chargeur PySpark, les contrôles de qualité, leurs tests et les instructions d'exécution de ce fichier. Les explications et raisonnements du `REPORT.md` sont rédigés par les membres du groupe.
