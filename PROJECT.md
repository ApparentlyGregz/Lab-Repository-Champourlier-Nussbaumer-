# Projet Big Data Processing

## Participants

- Grégoire CHAMPOURLIER (`ApparentlyGregz`) — gregoire.champourlier@edu.ece.fr.
- Hippolyte NUSSBAUMER (`hippolyte-coder`) — hippolyte.nussbaumer@edu.ece.fr.

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

## Films joués dans plusieurs langues originales

La langue d'un titre alternatif IMDb ne décrit pas la langue du film. Pour cette question, nous utilisons la propriété `P364` de Wikidata, qui indique la langue originale du film. La correspondance avec IMDb se fait par l'identifiant `P345`. Une correspondance ambiguë est écartée. Les fichiers de résultats conservent l'URI Wikidata de chaque film et de chaque langue.

Le périmètre est l'ensemble des longs métrages IMDb sortis entre 1980 et 2025, d'une durée d'au moins 60 minutes, notés au moins 7,0 avec au moins 1 000 votes. Chaque langue comptée pour une personne doit provenir d'un film différent : un film que Wikidata déclare en plusieurs langues ne justifie qu'une seule d'entre elles. Le nombre de langues correspond à un appariement maximal entre les langues et les films de la personne (`language_count`) ; `listed_language_count` indique toutes les langues de tous ses films. Un acteur ou une actrice est retenu avec au moins deux langues ainsi justifiées, et le résultat signale les personnes qui en ont au moins trois.

Avant ce calcul, les variantes que Wikidata identifie séparément pour une même langue sont regroupées, par exemple l'anglais américain avec l'anglais, le chinois standard avec le mandarin ou le chinois yue avec le cantonais. La liste complète figure dans `reference/language_variants.csv` et peut être modifiée sans changer le code. Les langues distinctes restent séparées, ainsi que les entrées génériques comme « Chinese » ou « Serbo-Croatian », les dialectes arabes et l'allemand suisse. Les valeurs `P364` d'origine sont conservées dans `source_language_uri_list`. Le fichier `results/exploration_languages.json` donne aussi les effectifs selon d'autres règles de comptage (toutes les langues de chaque film, films monolingues seulement, avec et sans regroupement des variantes).

Depuis la racine du dépôt, une fois les sept archives IMDb téléchargées :

```powershell
$projectPath = (Resolve-Path ./Project_NUSSBAUMER_CHAMPOURLIER).Path
$image = "quay.io/jupyter/pyspark-notebook@sha256:7e102d041c42cca6e0d32269cded10fad2c377aeb09fad1ec546840660d691c4"
docker run --rm --mount "type=bind,source=$projectPath,target=/work" -w /work $image spark-submit --driver-memory 4g --conf spark.sql.shuffle.partitions=32 src/language_candidates.py
docker run --rm --mount "type=bind,source=$projectPath,target=/work" -w /work $image python src/wikidata_language_collect.py
docker run --rm --mount "type=bind,source=$projectPath,target=/work" -w /work $image python src/wikidata_language_collect.py --stage labels
docker run --rm --mount "type=bind,source=$projectPath,target=/work" -w /work $image spark-submit --driver-memory 4g --conf spark.sql.shuffle.partitions=32 src/exploration_languages.py
```

La liste des films candidats est créée dans `output/language_candidates.csv`. La collecte Wikidata suit les limites de l'API, attend au moins 0,5 seconde entre les requêtes et reprend les identifiants manquants en cas de relance. Les réponses brutes sont dans `data/external/wikidata_original_languages.jsonl` et les noms des langues dans `data/external/wikidata_language_labels.jsonl`. Spark applique les critères, les jointures et les calculs, puis écrit la liste des personnes dans `results/actors_multilingual.csv`, les films qui étayent chaque langue dans `results/actor_language_evidence.csv`, avec la date de collecte Wikidata (`wikidata_retrieved_at`), et les effectifs dans `results/exploration_languages.json`. Les fichiers Parquet intermédiaires sont dans `output/`. Les films sans langue originale documentée ne sont pas classés dans une langue supposée.

OpenAI Codex a aidé à coder et tester cette analyse. Les conclusions et le raisonnement du `REPORT.md` sont rédigés par les membres du groupe.

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

## Synopsis et biographies Wikipédia

L'étape suivante relie les identifiants IMDb sélectionnés à Wikidata avec la propriété `P345` (identifiant IMDb), puis aux articles Wikipédia en anglais. Elle collecte la section « Plot » ou « Synopsis » des films et l'introduction des biographies. Le texte provient des contributeurs de Wikipédia ; aucun résumé n'est généré. Les extraits sont nettoyés (sélection de la section et normalisation des lignes vides) et redistribués sous [CC BY-SA 4.0](https://creativecommons.org/licenses/by-sa/4.0/). Chaque ligne garde l'URL de l'article, l'URL de la révision lue, la page d'historique des contributeurs et la licence. Les identifiants sans source ou sans section utilisable reçoivent un statut explicite.

Depuis la racine du dépôt, après l'exécution de `enrichment_selection.py` :

```powershell
$projectPath = (Resolve-Path ./Project_NUSSBAUMER_CHAMPOURLIER).Path
$image = "quay.io/jupyter/pyspark-notebook@sha256:7e102d041c42cca6e0d32269cded10fad2c377aeb09fad1ec546840660d691c4"
docker run --rm --mount "type=bind,source=$projectPath,target=/work" -w /work $image python src/wikipedia_collect.py --stage map
docker run --rm --mount "type=bind,source=$projectPath,target=/work" -w /work $image python src/wikipedia_collect.py --stage films
docker run --rm --mount "type=bind,source=$projectPath,target=/work" -w /work $image python src/wikipedia_collect.py --stage people
docker run --rm --mount "type=bind,source=$projectPath,target=/work" -w /work $image spark-submit --driver-memory 4g --conf spark.sql.shuffle.partitions=32 src/enrichment_wikipedia.py
```

Les requêtes sont séquentielles, espacées d'au moins 0,5 seconde et réessayées avec attente en cas de limitation ou d'erreur temporaire. Les fichiers bruts dans `data/external/` ne sont pas versionnés ; les relances reprennent aux identifiants non encore enregistrés. La collecte de tous les films peut durer plusieurs heures. `--max-items N` sert à un essai limité. L'usage doit respecter les [règles des API Wikimedia](https://foundation.wikimedia.org/wiki/Policy:Wikimedia_Foundation_API_Usage_Guidelines/en).

Le nettoyage du texte, l'application du schéma, les jointures et les calculs de couverture sont effectués dans Spark. Les données complètes sont écrites dans `output/enriched_films.parquet` et `output/enriched_principals.parquet`, et les versions CSV ainsi que le bilan dans `results/enriched_films.csv`, `results/enriched_principals.csv` et `results/enrichment_wikipedia_summary.json`. Un film présent dans plusieurs genres ne reçoit qu'un seul synopsis. Les 32 films ayant moins de trois crédits principaux dans IMDb restent signalés ; le traitement n'invente pas de personnes ou de biographies.

OpenAI Codex a aidé à coder et tester la collecte et le traitement Spark. Les explications et interprétations dans `REPORT.md` sont rédigées par le groupe.

## Modifications Wikipédia en direct

Ce traitement utilise le flux public [EventStreams `recentchange`](https://stream.wikimedia.org/v2/stream/recentchange), Kafka et PySpark Structured Streaming. Il conserve les modifications et créations d'articles de Wikipédia en anglais, puis recherche les pages dont le titre correspond à un film ou une série IMDb de 1980 à 2025 ayant au moins 100 votes. Les titres IMDb originaux et principaux sont pris en compte. Seules les pages Wikipédia avec un suffixe explicite comme `(film)`, `(2021 film)` ou `(television series)` sont retenues : les titres sans suffixe peuvent désigner un lieu, une personne ou un autre sujet. Le suffixe est retiré pour la comparaison, puis son année et son type d'œuvre sont vérifiés avec IMDb lorsqu'ils sont indiqués. Un même titre peut encore correspondre à plusieurs entrées IMDb ; chaque identifiant trouvé est conservé.

Depuis `Project_NUSSBAUMER_CHAMPOURLIER` dans PowerShell, après le téléchargement des archives IMDb :

```powershell
docker compose -p imdbstream -f streaming/compose.yaml up -d broker
docker compose -p imdbstream -f streaming/compose.yaml exec -T broker /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker:19092 --create --if-not-exists --topic wikipedia-recentchange --partitions 1 --replication-factor 1
docker compose -p imdbstream -f streaming/compose.yaml up -d --build producer
$projectPath = (Resolve-Path .).Path
$image = "quay.io/jupyter/pyspark-notebook@sha256:7e102d041c42cca6e0d32269cded10fad2c377aeb09fad1ec546840660d691c4"
docker run --rm --network imdbstream_default --mount "type=bind,source=$projectPath,target=/work" -w /work $image spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 --driver-memory 4g --conf spark.sql.shuffle.partitions=32 src/wiki_stream.py --minutes 20
docker compose -p imdbstream -f streaming/compose.yaml down
```

Le test dure 20 minutes après le chargement des titres IMDb. Le téléchargement initial du connecteur Kafka Spark requiert une connexion Internet. Chaque exécution reçoit un identifiant UTC : les correspondances complètes sont stockées dans `output/streaming/<identifiant>/matches/`, les métriques des lots dans `results/streaming_batches/<identifiant>/`, et le résumé ainsi que jusqu'à 100 exemples dans `results/streaming_summary.json` et `results/streaming_match_examples.csv`. Le résumé indique l'identifiant de l'exécution. Le producteur se reconnecte au flux avec le dernier identifiant d'événement confirmé par Kafka. Spark utilise l'heure de l'événement et une fenêtre de 30 minutes pour dédoublonner les identifiants ; les événements plus tardifs peuvent ne pas être retenus. Les membres du groupe doivent rédiger eux-mêmes la justification de ce choix et ses limites dans `REPORT.md`.

OpenAI Codex a aussi aidé à écrire les scripts du flux Wikipédia/Kafka/Spark, leurs tests et cette procédure d'exécution.

## Question libre : collaborations répétées

Le script cherche les duos réalisateur–acteur ayant travaillé sur au moins cinq films de 60 minutes ou plus, sortis entre 1980 et 2025 et notés par au moins 1 000 personnes. Il compare la moyenne des notes IMDb de leurs deux premiers films communs à celle de leurs deux derniers films. Les collaborations sont ordonnées par année de sortie, puis par identifiant IMDb en cas d'égalité. Le CSV indique aussi le plus petit nombre de votes parmi les deux films de chaque période. La comparaison est descriptive : elle ne permet pas d'attribuer la différence au duo lui-même.

Depuis la racine du dépôt, après le téléchargement des archives IMDb :

```powershell
$projectPath = (Resolve-Path ./Project_NUSSBAUMER_CHAMPOURLIER).Path
$image = "quay.io/jupyter/pyspark-notebook@sha256:7e102d041c42cca6e0d32269cded10fad2c377aeb09fad1ec546840660d691c4"
docker run --rm --mount "type=bind,source=$projectPath,target=/work" -w /work $image spark-submit --driver-memory 4g --conf spark.sql.shuffle.partitions=128 --conf spark.sql.adaptive.coalescePartitions.enabled=false src/open_question.py
```

La liste complète est écrite dans `results/director_actor_collaborations.csv`, avec un résumé et des exemples dans `results/open_question.json`. L'option `--role-credits-parquet` permet de réutiliser un fichier de crédits par film déjà généré par `exploration_people.py` ; elle est facultative.

OpenAI Codex a également aidé à coder et tester cette analyse. L'interprétation dans `REPORT.md` reste à écrire par le groupe.
