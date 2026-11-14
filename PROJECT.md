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

## Premières analyses

```powershell
$projectPath = (Resolve-Path ./Project_NUSSBAUMER_CHAMPOURLIER).Path
$image = "quay.io/jupyter/pyspark-notebook@sha256:7e102d041c42cca6e0d32269cded10fad2c377aeb09fad1ec546840660d691c4"
docker run --rm --mount "type=bind,source=$projectPath,target=/work" -w /work $image spark-submit --driver-memory 4g --conf spark.sql.shuffle.partitions=32 src/exploration_core.py
```

Le résultat compact est enregistré dans `results/exploration_core.json`. La liste complète des durées atypiques est générée dans `output/runtime_outliers.parquet` et peut être relue avec Spark. Le tableau `results/runtime_outliers_by_genre.csv` résume les résultats par genre et est versionné pour préparer le rapport. Les calculs utilisent les films de type `movie` d'au moins 60 minutes. Pour chaque genre, une durée est marquée comme atypique si elle est inférieure à Q1 − 1,5 × IQR ou supérieure à Q3 + 1,5 × IQR, où IQR = Q3 − Q1. Le total des lignes atypiques compte les couples film/genre ; le résultat indique aussi le nombre de films distincts. Pour les années 1980 à 2025, la popularité d'un genre correspond à la somme des votes IMDb des films notés. Un film présent dans plusieurs genres contribue à chacun de ces genres.

## Analyses sur les personnes et les séries

```powershell
$projectPath = (Resolve-Path ./Project_NUSSBAUMER_CHAMPOURLIER).Path
$image = "quay.io/jupyter/pyspark-notebook@sha256:7e102d041c42cca6e0d32269cded10fad2c377aeb09fad1ec546840660d691c4"
docker run --rm --mount "type=bind,source=$projectPath,target=/work" -w /work $image spark-submit --driver-memory 4g --conf spark.sql.shuffle.partitions=32 src/exploration_joins.py
docker run --rm --mount "type=bind,source=$projectPath,target=/work" -w /work $image spark-submit --driver-memory 4g --conf spark.sql.shuffle.partitions=128 --conf spark.sql.adaptive.coalescePartitions.enabled=false src/exploration_people.py
```

`exploration_joins.py` produit la liste des réalisateurs avec exactement un film noté au moins 8,0 et au moins deux autres films notés dont la moyenne ne dépasse pas 6,0. Seuls les films ayant au moins 1 000 votes participent aux notes. Les crédits de `title.crew` sont repris tels quels, y compris les réalisateurs non crédités au générique. Pour les séries, seules les saisons numérotées comptent ; au moins trois doivent comporter dix épisodes ou plus. Les meilleures et pires notes sont recherchées parmi les épisodes ayant au moins 50 votes. Tous les épisodes à la meilleure note doivent se trouver dans les saisons 1 ou 2 et tous ceux à la pire note dans la dernière saison. Les listes complètes sont dans `results/directors_one_high_film.csv` et `results/series_early_best_late_worst.csv` ; elles indiquent les votes des films et épisodes cités. `results/exploration_joins.json` conserve les effectifs et des exemples.

`exploration_people.py` cherche les acteurs crédités dans au moins quinze films notés sur une période d'au moins vingt ans. Pour ce calcul, seuls les films datés de 2025 ou avant avec au moins 1 000 votes comptent dans la durée de carrière, le nombre de films et la comparaison des notes. Leurs notes maximales et moyennes doivent toutes deux être plus élevées dans la première moitié de cette période. La liste complète est dans `results/actors_early_rating_peak.csv`, avec les votes et identifiants des films aux notes maximales de chaque moitié. Le script regroupe aussi les crédits d'acteur, réalisateur et scénariste, puis compare, pour chaque personne ayant les deux types de films, sa note moyenne dans les films à plusieurs rôles avec celle de ses films à un seul rôle. Cette comparaison utilise également le seuil de 1 000 votes par film. Les effectifs, les combinaisons de rôles et des exemples figurent dans `results/exploration_people.json` et dans les autres CSV du même répertoire ; les listes complètes des professionnels restent dans `output/` sous forme Parquet. La commande crée aussi deux fichiers Parquet intermédiaires dans `output/` pour éviter de relire les crédits compressés. Sur le même jeu de données, l'option `--reuse-intermediates` permet de les réutiliser ; sans cette option, ils sont recalculés.

Selon la [documentation IMDb](https://www.imdb.com/interfaces/), `title.akas.language` décrit la langue d'un titre alternatif, pas celle parlée dans le film. La question sur les films de plusieurs langues nécessitera donc une source qui renseigne réellement la langue des films.

## Usage d'outils d'IA

OpenAI Codex a aidé à écrire le chargeur PySpark, les contrôles de qualité, les analyses Spark, leurs tests et les instructions d'exécution de ce fichier. Les explications et raisonnements du `REPORT.md` sont rédigés par les membres du groupe.

## Sélection des films à enrichir

Depuis la racine du dépôt, après le téléchargement des données IMDb :

```powershell
$projectPath = (Resolve-Path ./Project_NUSSBAUMER_CHAMPOURLIER).Path
$image = "quay.io/jupyter/pyspark-notebook@sha256:7e102d041c42cca6e0d32269cded10fad2c377aeb09fad1ec546840660d691c4"
docker run --rm --mount "type=bind,source=$projectPath,target=/work" -w /work $image spark-submit --driver-memory 4g --conf spark.sql.shuffle.partitions=32 src/enrichment_selection.py
```

Le script classe les films d'au moins 60 minutes de 1980 à 2025 selon le nombre de votes IMDb, séparément pour chaque année et chaque genre. Il retient les dix premiers de chaque groupe, puis les trois premières personnes distinctes selon l'ordre des crédits IMDb de chaque film. Un film présent dans plusieurs genres n'a besoin que d'un synopsis et d'un ensemble de biographies. Les listes sont dans `results/enrichment_films.csv` et `results/enrichment_principals.csv` ; `results/enrichment_selection.json` donne les effectifs. Les films pour lesquels moins de trois personnes sont disponibles figurent dans `results/enrichment_principal_shortfalls.csv`. Cette sélection ne contient pas encore les synopsis ni les biographies provenant d'une source externe.

### Usage de l'IA pour cette étape

OpenAI Codex a également aidé à coder et tester cette sélection.
