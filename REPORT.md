# Report: Big Data Processing Project (IMDb)

**Group:** Grégoire CHAMPOURLIER, Hippolyte NUSSBAUMER  
**Course project:** Big Data Processing  
**Dataset:** IMDb (files dated 9 November 2026)  
**Results:** `Project_NUSSBAUMER_CHAMPOURLIER/results/`

## Contents

1. [Data preparation](#1-data-preparation)
2. [Exploration](#2-exploration)
3. [Enrichment](#3-enrichment)
4. [Streaming](#4-streaming)
5. [Open question](#5-open-question)

## Source data

| IMDb file | Size (MB) | File date (UTC) |
|---|---|---|
| `name.basics.tsv.gz` | 312.7 | 2026-11-09 |
| `title.akas.tsv.gz` | 522.2 | 2026-11-09 |
| `title.basics.tsv.gz` | 230.1 | 2026-11-09 |
| `title.crew.tsv.gz` | 84.0 | 2026-11-09 |
| `title.episode.tsv.gz` | 55.4 | 2026-11-09 |
| `title.principals.tsv.gz` | 789.8 | 2026-11-09 |
| `title.ratings.tsv.gz` | 8.7 | 2026-11-09 |

## Introduction

In this project, we worked with the public IMDb datasets to study movies, TV series and the people who work on them. The files are large, so our goal was to use big data tools instead of trying to process everything in a normal spreadsheet. We wanted to clean the data, answer questions with it, add information from Wikipedia and test a live data stream.

We mainly used **PySpark** to read the IMDb files, join tables and calculate the results. We used **Kafka** for the live Wikipedia events and **Docker** to run the different services. We also used Wikipedia and Wikidata when we needed information that was not in IMDb, such as movie plots, biographies and original languages.

The report has five parts. First, we check the quality of the IMDb data. Then, we explore movie and TV data with several questions. The third part is about adding information from Wikipedia. The fourth part explains our streaming pipeline. Finally, we answer our own question about directors and actors who worked together on several films. The commands to run the project are in `PROJECT.md`, and the result files are in `Project_NUSSBAUMER_CHAMPOURLIER/results/`.

## 1. Data preparation

*Question: For each column of each table in the IMDB dataset, calculate the percentage of nulls, missing, or empty records. Which five columns […] have the highest percentage of null values? Identify other data quality issues […]. Design and implement a way to handle these values.*

### 1.1 Missing values per column

Source: `results/data_quality.json` (key `columns`).

| Table | Column | Rows | Missing | % missing |
|---|---|---|---|---|
| name.basics | `nconst` | 15,800,186 | 0 | 0.00 |
| name.basics | `primaryName` | 15,800,186 | 89 | 0.00 |
| name.basics | `birthYear` | 15,800,186 | 15,114,676 | 95.66 |
| name.basics | `deathYear` | 15,800,186 | 15,534,925 | 98.32 |
| name.basics | `primaryProfession` | 15,800,186 | 3,215,910 | 20.35 |
| name.basics | `knownForTitles` | 15,800,186 | 1,915,225 | 12.12 |
| title.akas | `titleId` | 60,307,782 | 0 | 0.00 |
| title.akas | `ordering` | 60,307,782 | 0 | 0.00 |
| title.akas | `title` | 60,307,782 | 0 | 0.00 |
| title.akas | `region` | 60,307,782 | 12,989,222 | 21.54 |
| title.akas | `language` | 60,307,782 | 19,634,437 | 32.56 |
| title.akas | `types` | 60,307,782 | 40,336,246 | 66.88 |
| title.akas | `attributes` | 60,307,782 | 59,990,660 | 99.47 |
| title.akas | `isOriginalTitle` | 60,307,782 | 0 | 0.00 |
| title.basics | `tconst` | 12,949,098 | 0 | 0.00 |
| title.basics | `titleType` | 12,949,098 | 0 | 0.00 |
| title.basics | `primaryTitle` | 12,949,098 | 0 | 0.00 |
| title.basics | `originalTitle` | 12,949,098 | 0 | 0.00 |
| title.basics | `isAdult` | 12,949,098 | 0 | 0.00 |
| title.basics | `startYear` | 12,949,098 | 1,487,756 | 11.49 |
| title.basics | `endYear` | 12,949,098 | 12,784,830 | 98.73 |
| title.basics | `runtimeMinutes` | 12,949,098 | 8,051,538 | 62.18 |
| title.basics | `genres` | 12,949,098 | 547,330 | 4.23 |
| title.crew | `tconst` | 12,948,225 | 0 | 0.00 |
| title.crew | `directors` | 12,948,225 | 5,783,777 | 44.67 |
| title.crew | `writers` | 12,948,225 | 6,424,710 | 49.62 |
| title.episode | `tconst` | 10,023,528 | 0 | 0.00 |
| title.episode | `parentTconst` | 10,023,528 | 0 | 0.00 |
| title.episode | `seasonNumber` | 10,023,528 | 2,119,623 | 21.15 |
| title.episode | `episodeNumber` | 10,023,528 | 2,119,623 | 21.15 |
| title.principals | `tconst` | 102,629,356 | 0 | 0.00 |
| title.principals | `ordering` | 102,629,356 | 0 | 0.00 |
| title.principals | `nconst` | 102,629,356 | 0 | 0.00 |
| title.principals | `category` | 102,629,356 | 0 | 0.00 |
| title.principals | `job` | 102,629,356 | 82,777,959 | 80.66 |
| title.principals | `characters` | 102,629,356 | 52,638,875 | 51.29 |
| title.ratings | `tconst` | 1,730,480 | 0 | 0.00 |
| title.ratings | `averageRating` | 1,730,480 | 0 | 0.00 |
| title.ratings | `numVotes` | 1,730,480 | 0 | 0.00 |

Mean missing rate per table:

| Table | Rows | Columns | Mean % missing | Columns with no missing value |
|---|---|---|---|---|
| name.basics | 15,800,186 | 6 | 37.74 | 1 |
| title.akas | 60,307,782 | 8 | 27.56 | 4 |
| title.basics | 12,949,098 | 9 | 19.63 | 5 |
| title.crew | 12,948,225 | 3 | 31.43 | 1 |
| title.episode | 10,023,528 | 4 | 10.57 | 2 |
| title.principals | 102,629,356 | 6 | 21.99 | 4 |
| title.ratings | 1,730,480 | 3 | 0.00 | 3 |

### 1.2 The five most incomplete columns

| Rank | Table | Column | Missing | % missing |
|---|---|---|---|---|
| 1 | title.akas | `attributes` | 59,990,660 | 99.47 |
| 2 | title.basics | `endYear` | 12,784,830 | 98.73 |
| 3 | name.basics | `deathYear` | 15,534,925 | 98.32 |
| 4 | name.basics | `birthYear` | 15,114,676 | 95.66 |
| 5 | title.principals | `job` | 82,777,959 | 80.66 |

### 1.3 Other data quality issues

| Check | Table | Rule | Rows |
|---|---|---|---|
| Invalid birth year | name.basics | `birthYear` cannot be converted to an integer | 0 |
| Invalid death year | name.basics | `deathYear` not numeric, or earlier than `birthYear` | 28 |
| Invalid runtime | title.basics | `runtimeMinutes` not numeric, or ≤ 0 | 4 |
| Invalid rating | title.ratings | `averageRating` outside [1, 10] | 0 |
| Invalid vote count | title.ratings | `numVotes` not numeric, or < 0 | 0 |
| Rating without a title | title.ratings → title.basics | `tconst` not found in `title.basics` | 0 |
| Episode without a series | title.episode → title.basics | `parentTconst` not found in `title.basics` | 5 |

Columns added by the cleaning step (`src/data_quality.py`):

| Table | Cleaned columns | Flags |
|---|---|---|
| name.basics | `birthYear_clean`, `deathYear_clean` | `invalid_birth_year`, `invalid_death_year` |
| title.basics | `runtimeMinutes_clean` | `invalid_runtime` |
| title.ratings | `averageRating_clean`, `numVotes_clean` | `invalid_rating`, `invalid_votes` |

### 1.4 Handling the values

For the missing-value check, we treated a null value, the IMDb marker `\N`, and an empty text field as missing information. This matters because IMDb uses `\N` in its TSV files instead of leaving every missing field blank. We counted missing values for each column before using the data in the analyses.

We did not replace all missing values with zero. For example, a missing `deathYear` does not mean the person died in year 0, and a missing `endYear` can be normal for an ongoing series or for other title types. Filling those fields with zero would create false information. When an analysis needs a year or a runtime, we use the available valid values and leave the other ones out of that calculation.

Our cleaning step keeps the original data and adds separate columns with a `_clean` suffix. It also adds flags for invalid years, runtimes, ratings and vote counts. This makes it possible to use the valid numbers without losing the original values. For example, the 28 invalid death-year records and the 4 invalid runtime records can be found again using the flags. We also checked whether IDs used to connect tables really exist. No rating had a missing title, but 5 episodes pointed to a series that was not found. We reported these 5 cases instead of pretending they were valid matches.

### 1.5 Methodological Explanation

We used PySpark for the data-quality checks because it can read and process all seven IMDb files, including the very large `title.principals` table. For each column, we counted the rows and the missing values, then calculated a percentage. We also added a few checks for values that do not make sense, such as a negative runtime or a death year before a birth year.

We chose to keep the original columns and create cleaned columns instead of deleting complete rows. A row can still be useful even if one field is missing. Deleting all incomplete rows would remove a very large part of the dataset. We also preferred not to fill missing years with averages because the result would look precise but would not be real information.

There are limits to this work. The checks cover the main numeric problems and two links between tables, but not every possible foreign key. For example, we did not check every person ID in `title.principals` against `name.basics`, or every title ID in the alternative-title table. Also, a value can be valid in format but still be wrong in real life. Our checks cannot detect every such case.

## 2. Exploration

### 2.1 Movies with an unusual runtime for their genre

*Question: Find all feature-length movies by genre whose length is notably different than the average length of movies in that genre.*

| Parameter | Value |
|---|---|
| Title type | `movie` |
| Minimum runtime of a feature film | 60 min |
| Method | Tukey IQR rule (below Q1 − 1.5 × IQR or above Q3 + 1.5 × IQR) |
| Quartile accuracy (`percentile_approx`) | 10,000 |

Total: **32,427** outlier movie/genre pairs, which is **21,323** distinct movies. Full list: `output/runtime_outliers.parquet`.

| Genre | Movies | Mean (min) | Median | IQR | Lower bound | Upper bound | Outliers | % outliers |
|---|---|---|---|---|---|---|---|---|
| Action | 38,752 | 103.3 | 95 | 27 | 46.5 | 154.5 | 2,055 | 5.3% |
| Adult | 5,257 | 83.5 | 80 | 15 | 49.5 | 109.5 | 405 | 7.7% |
| Adventure | 22,513 | 95.8 | 91 | 21 | 50.5 | 134.5 | 1,311 | 5.8% |
| Animation | 7,170 | 87.9 | 85 | 20 | 45.0 | 125.0 | 279 | 3.9% |
| Biography | 13,631 | 95.9 | 92 | 29 | 35.5 | 151.5 | 396 | 2.9% |
| Comedy | 92,093 | 95.2 | 92 | 19 | 55.5 | 131.5 | 4,818 | 5.2% |
| Crime | 33,218 | 97.4 | 93 | 21 | 53.5 | 137.5 | 1,870 | 5.6% |
| Documentary | 82,215 | 88.7 | 83 | 21 | 40.5 | 124.5 | 3,306 | 4.0% |
| Drama | 184,409 | 99.0 | 95 | 24 | 49.0 | 145.0 | 8,061 | 4.4% |
| Family | 14,713 | 95.7 | 90 | 23 | 46.5 | 138.5 | 922 | 6.3% |
| Fantasy | 11,968 | 96.6 | 92 | 22 | 50.0 | 138.0 | 589 | 4.9% |
| Film-Noir | 850 | 82.7 | 82 | 20 | 42.0 | 122.0 | 3 | 0.4% |
| Game-Show | 6 | 90.2 | 77 | 33 | 17.5 | 149.5 | 1 | 16.7% |
| History | 11,690 | 101.3 | 94 | 31 | 34.5 | 158.5 | 481 | 4.1% |
| Horror | 29,054 | 90.8 | 89 | 17 | 54.5 | 122.5 | 1,261 | 4.3% |
| Music | 11,899 | 94.6 | 90 | 25 | 40.5 | 140.5 | 510 | 4.3% |
| Musical | 8,145 | 102.1 | 95 | 29 | 40.5 | 156.5 | 433 | 5.3% |
| Mystery | 15,628 | 96.7 | 93 | 21 | 52.5 | 136.5 | 686 | 4.4% |
| News | 1,059 | 85.8 | 80 | 19 | 42.5 | 118.5 | 51 | 4.8% |
| Reality-TV | 251 | 99.8 | 90 | 23 | 40.5 | 132.5 | 22 | 8.8% |
| Romance | 41,095 | 100.6 | 96 | 24 | 50.0 | 146.0 | 2,109 | 5.1% |
| Sci-Fi | 9,188 | 93.9 | 90 | 19 | 52.5 | 128.5 | 491 | 5.3% |
| Sport | 5,820 | 95.4 | 90 | 21 | 50.5 | 134.5 | 266 | 4.6% |
| Thriller | 32,994 | 97.6 | 93 | 21 | 53.5 | 137.5 | 1,574 | 4.8% |
| War | 7,403 | 98.5 | 94 | 23 | 49.5 | 141.5 | 372 | 5.0% |
| Western | 4,310 | 87.5 | 85 | 25 | 32.5 | 132.5 | 155 | 3.6% |

The 20 most extreme deviations:

| Genre | Movie | IMDb | Year | Runtime (min) | Genre mean |
|---|---|---|---|---|---|
| Documentary | Logistics | tt8273150 | 2012 | 51,420 | 88.7 |
| Documentary | Ambiancé | tt3854496 | 2020 | 43,200 | 88.7 |
| Documentary | Carnets Filmés (Liste Complète) | tt12277054 | 2019 | 28,643 | 88.7 |
| Documentary | Modern Times Forever | tt2659636 | 2011 | 14,400 | 88.7 |
| Documentary | Beijing 2003 | tt2355497 | 2004 | 9,000 | 88.7 |
| Documentary | Hunger! | tt5068890 | 2015 | 6,000 | 88.7 |
| Drama | Hunger! | tt5068890 | 2015 | 6,000 | 99.0 |
| Drama | Matrjoschka | tt1806963 | 2006 | 5,700 | 99.0 |
| Comedy | London EC1 | tt5136218 | 2015 | 5,460 | 95.2 |
| Mystery | London EC1 | tt5136218 | 2015 | 5,460 | 96.7 |
| Drama | London EC1 | tt5136218 | 2015 | 5,460 | 99.0 |
| Documentary | The Cure for Insomnia | tt0284020 | 1987 | 5,220 | 88.7 |
| Music | The Cure for Insomnia | tt0284020 | 1987 | 5,220 | 94.6 |
| Drama | A 2nd generation film | tt3837350 | 2013 | 3,077 | 99.0 |
| Documentary | Saynatakuna: Masks | tt5135246 | 2013 | 2,905 | 88.7 |
| Documentary | The Longest Most Meaningless Movie in the World | tt0342707 | 1970 | 2,880 | 88.7 |
| History | Saynatakuna: Masks | tt5135246 | 2013 | 2,905 | 101.3 |
| Drama | The Longest Most Meaningless Movie in the World | tt0342707 | 1970 | 2,880 | 99.0 |
| History | The Longest Most Meaningless Movie in the World | tt0342707 | 1970 | 2,880 | 101.3 |
| Documentary | World Peace & Prayer Day | tt1866307 | 2011 | 2,400 | 88.7 |

We found **32,427 movie/genre results** outside the normal runtime range. They correspond to **21,323 different movies**, because one movie can belong to several genres. Drama has the largest number of outlier results, with 8,061, followed by Comedy with 4,818 and Documentary with 3,306. The percentage is not the same in every genre: for example, about 5.3% of Action movies are outliers, compared with 3.9% of Animation movies.

Some results are very unusual. *Logistics* is listed with a runtime of 51,420 minutes, while the average Documentary runtime is 88.7 minutes. There are also very long experimental movies such as *Ambiancé*. These are not necessarily mistakes: some films are made to be much longer than normal movies. We would need to check them individually before deciding that the data is incorrect.

We used the **IQR method** because the average can be strongly affected by these extreme movies. The IQR uses the middle part of the runtimes for each genre and is better for deciding what is unusual. We still included the genre averages because they help explain the difference. Another method would be to use the mean and standard deviation, but a few extremely long films would have too much influence. Since we only kept films of at least 60 minutes, and the calculated lower limits are below 60 minutes, our outliers are mainly unusually long films. The full list is in `output/runtime_outliers.parquet`.

### 2.2 Top three movie genres per year (1980–2025)

*Question: What are the top three most popular movie genres for each year between 1980 and 2025?*

Popularity measure: `sum_of_IMDb_numVotes` (sum of the IMDb votes of the rated movies of the genre; a movie with several genres counts in each of them).

| Year | 1st | Votes | Movies | 2nd | Votes | Movies | 3rd | Votes | Movies |
|---|---|---|---|---|---|---|---|---|---|
| 1980 | Drama | 3,163,679 | 1,063 | Adventure | 2,482,309 | 135 | Horror | 2,073,720 | 142 |
| 1981 | Adventure | 2,246,531 | 152 | Drama | 1,709,083 | 1,020 | Horror | 1,276,383 | 158 |
| 1982 | Drama | 3,072,485 | 1,108 | Sci-Fi | 2,458,546 | 51 | Action | 1,873,716 | 294 |
| 1983 | Drama | 2,920,636 | 1,133 | Action | 2,024,898 | 297 | Adventure | 1,984,742 | 158 |
| 1984 | Action | 3,668,601 | 288 | Drama | 3,023,903 | 1,113 | Adventure | 2,930,445 | 143 |
| 1985 | Comedy | 4,563,769 | 621 | Adventure | 3,411,758 | 148 | Drama | 3,077,974 | 1,071 |
| 1986 | Drama | 3,999,027 | 1,126 | Adventure | 2,961,628 | 160 | Comedy | 2,786,011 | 627 |
| 1987 | Drama | 4,134,922 | 1,227 | Comedy | 3,685,730 | 623 | Action | 2,356,033 | 419 |
| 1988 | Drama | 4,465,188 | 1,150 | Comedy | 4,429,867 | 618 | Action | 2,709,127 | 442 |
| 1989 | Comedy | 5,012,838 | 662 | Drama | 3,960,386 | 1,193 | Adventure | 2,944,137 | 167 |
| 1990 | Drama | 6,320,268 | 1,158 | Comedy | 4,016,403 | 621 | Crime | 3,589,573 | 355 |
| 1991 | Drama | 5,584,693 | 1,178 | Crime | 3,675,404 | 264 | Comedy | 3,194,791 | 622 |
| 1992 | Drama | 6,089,879 | 1,063 | Comedy | 4,071,316 | 615 | Crime | 3,610,689 | 282 |
| 1993 | Drama | 8,996,064 | 1,076 | Comedy | 4,499,067 | 557 | Crime | 3,046,678 | 288 |
| 1994 | Drama | 15,742,157 | 1,123 | Crime | 6,505,914 | 272 | Comedy | 5,676,321 | 628 |
| 1995 | Drama | 12,525,648 | 1,165 | Crime | 7,766,999 | 255 | Comedy | 6,082,408 | 585 |
| 1996 | Drama | 7,861,437 | 1,135 | Comedy | 4,790,270 | 660 | Crime | 4,445,628 | 245 |
| 1997 | Drama | 12,628,128 | 1,224 | Action | 6,768,763 | 336 | Comedy | 5,606,135 | 720 |
| 1998 | Drama | 11,783,906 | 1,255 | Comedy | 9,864,503 | 752 | Crime | 6,219,924 | 304 |
| 1999 | Drama | 15,522,763 | 1,371 | Comedy | 8,223,637 | 810 | Crime | 7,183,671 | 296 |
| 2000 | Drama | 13,025,719 | 1,461 | Comedy | 8,336,499 | 806 | Adventure | 6,982,715 | 141 |
| 2001 | Drama | 14,737,243 | 1,594 | Comedy | 9,509,527 | 887 | Adventure | 9,250,155 | 150 |
| 2002 | Drama | 17,062,340 | 1,636 | Adventure | 8,690,535 | 149 | Action | 8,393,183 | 396 |
| 2003 | Drama | 13,254,158 | 1,675 | Action | 11,710,912 | 368 | Comedy | 10,760,966 | 1,024 |
| 2004 | Drama | 16,476,460 | 1,776 | Action | 11,516,160 | 412 | Comedy | 10,949,509 | 1,034 |
| 2005 | Drama | 13,688,646 | 1,926 | Comedy | 10,193,732 | 1,138 | Action | 9,061,417 | 390 |
| 2006 | Drama | 20,885,807 | 2,187 | Comedy | 11,776,960 | 1,248 | Thriller | 8,666,781 | 421 |
| 2007 | Drama | 18,536,783 | 2,218 | Comedy | 11,718,538 | 1,235 | Action | 8,992,322 | 432 |
| 2008 | Drama | 17,574,210 | 2,482 | Action | 13,404,387 | 499 | Comedy | 11,327,815 | 1,405 |
| 2009 | Drama | 17,590,672 | 2,773 | Comedy | 13,462,651 | 1,548 | Action | 12,118,563 | 595 |
| 2010 | Drama | 17,552,874 | 2,931 | Action | 13,331,176 | 673 | Adventure | 13,177,855 | 336 |
| 2011 | Drama | 20,717,586 | 3,156 | Action | 17,146,468 | 700 | Comedy | 13,980,622 | 1,721 |
| 2012 | Drama | 20,114,768 | 3,268 | Action | 14,895,263 | 683 | Comedy | 12,509,262 | 1,829 |
| 2013 | Drama | 21,915,484 | 3,614 | Action | 17,179,453 | 776 | Comedy | 14,956,716 | 1,936 |
| 2014 | Drama | 24,514,158 | 3,784 | Action | 20,930,106 | 814 | Adventure | 17,738,995 | 518 |
| 2015 | Drama | 20,481,990 | 3,910 | Action | 15,559,741 | 850 | Adventure | 14,162,940 | 516 |
| 2016 | Drama | 20,883,557 | 3,937 | Action | 17,207,276 | 891 | Adventure | 12,363,760 | 530 |
| 2017 | Drama | 20,801,742 | 4,071 | Action | 16,707,506 | 909 | Comedy | 11,301,385 | 2,130 |
| 2018 | Drama | 17,404,158 | 4,186 | Action | 15,079,270 | 891 | Adventure | 13,344,499 | 509 |
| 2019 | Drama | 21,998,258 | 4,249 | Action | 12,355,053 | 901 | Comedy | 12,246,407 | 2,129 |
| 2020 | Drama | 10,462,065 | 3,148 | Comedy | 6,507,297 | 1,546 | Action | 6,126,941 | 606 |
| 2021 | Drama | 14,383,260 | 3,588 | Action | 12,870,759 | 826 | Adventure | 10,189,309 | 401 |
| 2022 | Drama | 15,957,111 | 4,102 | Action | 13,349,185 | 924 | Comedy | 10,196,049 | 2,205 |
| 2023 | Drama | 14,754,249 | 4,295 | Action | 10,567,691 | 1,043 | Comedy | 9,490,302 | 2,243 |
| 2024 | Drama | 11,636,055 | 3,986 | Action | 8,648,553 | 993 | Comedy | 7,081,670 | 2,090 |
| 2025 | Drama | 8,614,613 | 3,556 | Action | 7,687,716 | 941 | Comedy | 5,885,146 | 1,908 |

Number of years in the top 3 and in 1st place (1980–2025):

| Genre | Years in top 3 | Years in 1st place |
|---|---|---|
| Drama | 46 | 42 |
| Comedy | 34 | 2 |
| Action | 29 | 1 |
| Adventure | 16 | 1 |
| Crime | 9 | 0 |
| Horror | 2 | 0 |
| Sci-Fi | 1 | 0 |
| Thriller | 1 | 0 |

The clearest result is that **Drama dominates the ranking**. It is in the top three in all 46 years and is number one in 42 of them. Comedy is also very important, with 34 years in the top three. Action appears in the top three 29 times, and it becomes especially common from the 2000s onward. In the early 1980s, genres such as Horror, Adventure and Sci-Fi appear more often near the top. By the 2010s, the most common top three are Drama, Action and either Comedy or Adventure.

We measured popularity with the **total number of IMDb votes** for the rated movies of each genre and year. We chose votes instead of the average rating because a film can have a very high rating from a small number of people. A large number of votes is a useful sign of audience attention. When a movie has more than one genre, its votes count in every one of those genres. This is important when reading the numbers.

This method has limits. IMDb users are not a perfect sample of all viewers, and older films have had more time to collect votes. A genre with many movies can also get more total votes than a smaller genre. We could compare the average votes per film or count movies with at least a certain number of votes, but that would answer a slightly different question. Our results describe popularity **on IMDb**, not total cinema tickets or box-office sales.

### 2.3 Top 3 compared with the other genres

*Question: Compare the results from the previous question. How do the top three genres compare to the rest of the genres of movies in a given year?*

| Year | Top-3 votes | Other votes | Top-3 vote share | Top-3 movie/genre pairs | Other pairs | Top-3 pair share | Other genres |
|---|---|---|---|---|---|---|---|
| 1980 | 7,719,708 | 9,716,244 | 44.3% | 1,340 | 2,257 | 37.3% | 21 |
| 1981 | 5,231,997 | 6,013,380 | 46.5% | 1,330 | 2,349 | 36.2% | 20 |
| 1982 | 7,404,747 | 9,529,194 | 43.7% | 1,453 | 2,267 | 39.1% | 19 |
| 1983 | 6,930,276 | 8,179,179 | 45.9% | 1,588 | 2,212 | 41.8% | 19 |
| 1984 | 9,622,949 | 12,160,654 | 44.2% | 1,544 | 2,292 | 40.3% | 19 |
| 1985 | 11,053,501 | 10,394,687 | 51.5% | 1,840 | 2,046 | 47.3% | 19 |
| 1986 | 9,746,666 | 11,327,326 | 46.2% | 1,913 | 2,088 | 47.8% | 19 |
| 1987 | 10,176,685 | 13,680,127 | 42.7% | 2,269 | 1,955 | 53.7% | 19 |
| 1988 | 11,604,182 | 13,165,481 | 46.8% | 2,210 | 2,043 | 52.0% | 19 |
| 1989 | 11,917,361 | 12,172,741 | 49.5% | 2,022 | 2,355 | 46.2% | 19 |
| 1990 | 13,926,244 | 16,602,260 | 45.6% | 2,134 | 2,051 | 51.0% | 19 |
| 1991 | 12,454,888 | 15,693,839 | 44.2% | 2,064 | 1,957 | 51.3% | 19 |
| 1992 | 13,771,884 | 15,903,364 | 46.4% | 1,960 | 1,850 | 51.4% | 19 |
| 1993 | 16,541,809 | 20,382,177 | 44.8% | 1,921 | 1,853 | 50.9% | 19 |
| 1994 | 27,924,392 | 19,960,435 | 58.3% | 2,023 | 1,866 | 52.0% | 19 |
| 1995 | 26,375,055 | 27,414,105 | 49.0% | 2,005 | 2,026 | 49.7% | 19 |
| 1996 | 17,097,335 | 20,977,615 | 44.9% | 2,040 | 1,971 | 50.9% | 20 |
| 1997 | 25,003,026 | 31,691,347 | 44.1% | 2,280 | 1,968 | 53.7% | 19 |
| 1998 | 27,868,333 | 26,256,930 | 51.5% | 2,311 | 2,098 | 52.4% | 19 |
| 1999 | 30,930,071 | 39,944,628 | 43.6% | 2,477 | 2,250 | 52.4% | 19 |
| 2000 | 28,344,933 | 34,622,051 | 45.0% | 2,408 | 2,678 | 47.3% | 19 |
| 2001 | 33,496,925 | 45,549,906 | 42.4% | 2,631 | 2,853 | 48.0% | 19 |
| 2002 | 34,146,058 | 46,363,637 | 42.4% | 2,181 | 3,415 | 39.0% | 22 |
| 2003 | 35,726,036 | 42,786,949 | 45.5% | 3,067 | 2,768 | 52.6% | 20 |
| 2004 | 38,942,129 | 55,281,280 | 41.3% | 3,222 | 3,131 | 50.7% | 21 |
| 2005 | 32,943,795 | 48,631,455 | 40.4% | 3,454 | 3,532 | 49.4% | 21 |
| 2006 | 41,329,548 | 55,455,341 | 42.7% | 3,856 | 4,079 | 48.6% | 20 |
| 2007 | 39,247,643 | 63,144,224 | 38.3% | 3,885 | 4,456 | 46.6% | 21 |
| 2008 | 42,306,412 | 62,403,110 | 40.4% | 4,386 | 4,982 | 46.8% | 20 |
| 2009 | 43,171,886 | 61,246,719 | 41.3% | 4,916 | 5,696 | 46.3% | 20 |
| 2010 | 44,061,905 | 66,142,414 | 40.0% | 3,940 | 7,697 | 33.9% | 21 |
| 2011 | 51,844,676 | 60,594,558 | 46.1% | 5,577 | 7,039 | 44.2% | 21 |
| 2012 | 47,519,293 | 63,628,282 | 42.8% | 5,780 | 7,583 | 43.3% | 21 |
| 2013 | 54,051,653 | 76,019,349 | 41.6% | 6,326 | 8,523 | 42.6% | 20 |
| 2014 | 63,183,259 | 71,041,955 | 47.1% | 5,116 | 11,029 | 31.7% | 23 |
| 2015 | 50,204,671 | 57,471,954 | 46.6% | 5,276 | 10,883 | 32.7% | 21 |
| 2016 | 50,454,593 | 63,961,357 | 44.1% | 5,358 | 10,941 | 32.9% | 21 |
| 2017 | 48,810,633 | 59,508,233 | 45.1% | 7,110 | 9,460 | 42.9% | 22 |
| 2018 | 45,827,927 | 57,606,259 | 44.3% | 5,586 | 11,175 | 33.3% | 22 |
| 2019 | 46,599,718 | 60,191,534 | 43.6% | 7,279 | 9,657 | 43.0% | 22 |
| 2020 | 23,096,303 | 28,304,018 | 44.9% | 5,300 | 7,780 | 40.5% | 21 |
| 2021 | 37,443,328 | 43,135,096 | 46.5% | 4,815 | 9,749 | 33.1% | 22 |
| 2022 | 39,502,345 | 45,650,558 | 46.4% | 7,231 | 9,757 | 42.6% | 22 |
| 2023 | 34,812,242 | 42,955,559 | 44.8% | 7,581 | 9,741 | 43.8% | 22 |
| 2024 | 27,366,278 | 36,404,334 | 42.9% | 7,069 | 9,748 | 42.0% | 23 |
| 2025 | 22,187,475 | 34,237,626 | 39.3% | 6,405 | 8,813 | 42.1% | 22 |

| Top-3 vote share | Value |
|---|---|
| Mean over 1980–2025 | 44.8% |
| Minimum | 38.3% |
| Maximum | 58.3% |

The top three genres get an average of **44.8% of the votes** in a year. Their share changes, but it is usually around 40% to 50%. The lowest value is 38.3% in 2007, and the highest is 58.3% in 1994. This means that the leading genres are important, but the other genres together still receive a large part of the attention.

The number of movie/genre pairs tells a similar story. For example, in 2014, the top three account for 47.1% of the votes but only 31.7% of the movie/genre pairs. So a smaller part of the movie entries received almost half of the votes. In other years, the top three represent a larger part of the film entries without having the same advantage in votes.

We compared the top three with all other genres for each year, rather than choosing the same three genres for the whole period. This lets us follow changes in the ranking. However, because one movie can have several genres, these are **genre totals**, not separate groups of unique movies. We should not add the values and interpret them as the number of different people who watched the films.

### 2.4 Directors with exactly one movie rated ≥ 8.0 and weaker other movies (48)

*Question: List all directors who have directed exactly one movie with a rating >= 8.0 but who have multiple other movies directed with an average rating of <= 6.0.*

| Parameter | Value |
|---|---|
| Rating of the successful movie | ≥ 8.0 (exactly one movie) |
| Minimum votes per movie | 1,000 |
| Other rated movies | ≥ 2 |
| Mean rating of the other movies | ≤ 6.0 |
| Source of the credits | `title.crew` (`directors` column) |

| Director | Successful movie | IMDb | Rating | Votes | Other movies | Mean of others |
|---|---|---|---|---|---|---|
| Robert Rodriguez | Sin City | tt0401792 | 8.0 | 825,612 | 22 | 5.94 |
| Wai Keung Lau | Infernal Affairs | tt0338564 | 8.0 | 139,880 | 21 | 6.00 |
| Puri Jagannadh | Pokiri | tt0816258 | 8.0 | 17,303 | 18 | 5.80 |
| Martin Campbell | Casino Royale | tt0381061 | 8.0 | 758,442 | 16 | 5.92 |
| M. Night Shyamalan | The Sixth Sense | tt0167404 | 8.2 | 1,176,662 | 15 | 5.87 |
| Sundar C. | Anbe Sivam | tt0367495 | 8.6 | 28,817 | 10 | 5.52 |
| Irvin Kershner | Star Wars: Episode V - The Empire Strikes Back | tt0080684 | 8.7 | 1,532,576 | 9 | 5.94 |
| Sinan Çetin | Abbas in Flower | tt0263208 | 8.0 | 12,164 | 7 | 4.84 |
| Harry Baweja | Chaar Sahibzaade 2: Rise of Banda Singh Bahadur | tt6246170 | 8.6 | 1,051 | 6 | 5.30 |
| Gunasekhar | Okkadu | tt0366840 | 8.0 | 11,850 | 6 | 5.38 |
| Tate Taylor | The Help | tt1454029 | 8.1 | 534,932 | 6 | 5.88 |
| Umesh Shukla | OMG: Oh My God! | tt2283748 | 8.1 | 72,445 | 6 | 5.58 |
| Vikas Bahl | Queen | tt3322420 | 8.1 | 72,011 | 6 | 5.83 |
| Predrag Antonijevic | Dara of Jasenovac | tt10554232 | 8.0 | 82,505 | 5 | 5.70 |
| P. Vasu | Aapthamitra | tt1339248 | 8.2 | 1,463 | 5 | 5.24 |
| Dharani | Ghilli | tt0422320 | 8.2 | 19,429 | 5 | 5.44 |
| Suseenthiran | Jeeva | tt4047112 | 8.0 | 2,448 | 5 | 5.96 |
| J.P. Dutta | Border | tt0118751 | 8.0 | 20,772 | 4 | 5.33 |
| David Leland | Concert for George | tt0380275 | 8.6 | 2,330 | 4 | 5.85 |
| Albert Magnoli | Sign 'o' the Times | tt0093970 | 8.0 | 3,052 | 4 | 5.80 |
| Ananth Mahadevan | Phule | tt21108804 | 8.1 | 3,248 | 4 | 5.15 |
| James McTeigue | V for Vendetta | tt0434409 | 8.1 | 1,261,298 | 4 | 5.97 |
| Sangeeth Sivan | Yoddha | tt0290937 | 8.4 | 5,495 | 4 | 5.25 |
| Kamal Tabrizi | The Lizard | tt0416960 | 8.4 | 24,812 | 4 | 5.53 |
| Billy Bob Thornton | Sling Blade | tt0117666 | 8.0 | 105,194 | 3 | 5.93 |
| R. Madhesh | Mudhalvan | tt0220656 | 8.4 | 9,589 | 3 | 5.80 |
| Mert Baykal | Pardon | tt0425333 | 8.1 | 24,931 | 3 | 5.90 |
| Vincent Paronnaud | Persepolis | tt0808417 | 8.0 | 107,467 | 3 | 5.70 |
| Ajay Bahl | Section 375 | tt8144834 | 8.0 | 21,933 | 3 | 4.63 |
| Prince | Sign 'o' the Times | tt0093970 | 8.0 | 3,052 | 2 | 4.90 |
| Carlos Bolado | Promises | tt0282864 | 8.3 | 2,796 | 2 | 5.85 |
| Garth Davis | Lion | tt3741834 | 8.0 | 270,238 | 2 | 5.65 |
| David Hogan | Sign 'o' the Times | tt0093970 | 8.0 | 3,052 | 2 | 4.60 |
| Richard Kelly | Donnie Darko | tt0246578 | 8.0 | 925,075 | 2 | 5.45 |
| Frank Miller | Sin City | tt0401792 | 8.0 | 825,612 | 2 | 5.65 |
| Ismael Rodríguez | Two Careful Fellows | tt0044560 | 8.0 | 1,140 | 2 | 5.95 |
| Denis Sanders | Elvis: That's the Way It Is | tt0065687 | 8.0 | 3,534 | 2 | 5.60 |
| Marcus Raboy | Conan O'Brien: The Kennedy Center Mark Twain Prize for American Humor | tt35445387 | 8.0 | 1,584 | 2 | 5.45 |
| Ezhil | Thulladha Manamum Thullum | tt1517561 | 8.3 | 7,360 | 2 | 4.55 |
| Johny Antony | C.I.D. Moosa | tt0377575 | 8.1 | 2,812 | 2 | 5.80 |
| Sateesh Paramveda | Seetha Kalyana Vaibhogame | tt32134802 | 8.4 | 2,362 | 2 | 5.80 |
| Christopher Ashley | Come from Away | tt7638556 | 8.5 | 5,013 | 2 | 5.60 |
| Saurabh Varma | The Great Punjab Robbery | tt39150248 | 9.3 | 1,757 | 2 | 5.90 |
| Nalan Kumarasamy | Soodhu Kavvum | tt2877108 | 8.2 | 13,767 | 2 | 5.95 |
| Ravikanth Perepu | Kshanam | tt5504168 | 8.1 | 6,705 | 2 | 5.95 |
| Harish Vyas | Om Ka Hari | tt36853476 | 9.8 | 5,025 | 2 | 5.75 |
| Karthick Naren | Dhuruvangal Pathinaaru | tt6380520 | 8.1 | 19,405 | 2 | 5.15 |
| Satish Paramvedha | Seetha Kalyana Vaibhogame | tt32134802 | 8.4 | 2,362 | 2 | 5.80 |

We found **48 directors** who meet the conditions. They have exactly one movie rated at least 8.0 among the movies we kept, but they also have at least two other rated films with a mean rating of 6.0 or less. For example, M. Night Shyamalan appears with *The Sixth Sense* at 8.2, while his 15 other included movies have a mean rating of 5.87. Martin Campbell appears with *Casino Royale* at 8.0, with 16 other movies averaging 5.92.

We used a minimum of **1,000 votes per movie** to avoid giving too much importance to ratings based on only a few votes. We also required at least two other movies so that the comparison was not based on just one film. The 8.0 threshold represents a very well-rated movie, while 6.0 makes the difference with the director's other work clear.

These numbers do not mean that the directors are bad, or that they only made one good film in their whole career. The result only concerns the movies included in this dataset and passing our vote filter. Changing the thresholds could give a different list. We also use an ordinary mean for the other movies, so each included movie has the same weight.

### 2.5 Series with the best episode early and the worst one in the last season (190)

*Question: Identify television series with at least three seasons consisting of >=10 episodes where the worst episode occurred in the last season and the best episode occurred within the first two seasons.*

| Parameter | Value |
|---|---|
| Seasons with at least 10 episodes | ≥ 3 |
| Minimum votes per episode | 50 |
| Ties | all best episodes in season 1 or 2, all worst episodes in the last season |
| Seasons used | numbered seasons only |

| Series | IMDb | Seasons with ≥ 10 ep. | Last season | Rated episodes | Best rating | Season | Worst rating | Season |
|---|---|---|---|---|---|---|---|---|
| Coach | tt0096560 | 9 | 9 | 199 | 9.1 | 2 | 6.7 | 9 |
| The Beverly Hillbillies | tt0055662 | 9 | 9 | 274 | 8.8 | 2 | 5.8 | 9 |
| The Flash | tt3107288 | 9 | 9 | 184 | 9.6 | 1 | 3.8 | 9 |
| 24 | tt0285331 | 8 | 8 | 192 | 9.3 | 1 | 7.6 | 8 |
| All American | tt7414406 | 8 | 8 | 127 | 9.0 | 2 | 6.1 | 8 |
| The Cosby Show | tt0086687 | 8 | 8 | 197 | 8.5 | 2 | 5.4 | 8 |
| Weeds | tt0439100 | 8 | 8 | 102 | 8.9 | 2 | 6.9 | 8 |
| Los Serrano | tt0362372 | 7 | 8 | 2 | 8.6 | 1 | 4.9 | 8 |
| NCIS: New Orleans | tt3560084 | 7 | 7 | 155 | 8.4 | 2 | 6.5 | 7 |
| Station 19 | tt7053188 | 7 | 7 | 105 | 9.2 | 2 | 6.5 | 7 |
| The Outer Limits | tt0112111 | 7 | 7 | 152 | 8.4 | 2 | 5.2 | 7 |
| True Blood | tt0844441 | 7 | 7 | 81 | 8.8 | 2 | 5.4 | 7 |
| Wings | tt0098948 | 7 | 8 | 172 | 8.6 | 2 | 6.4 | 8 |
| As If | tt13675832 | 6 | 6 | 68 | 9.4 | 2 | 6.2 | 6 |
| Bull | tt5827228 | 6 | 6 | 125 | 8.6 | 2 | 5.9 | 6 |
| Emergency! | tt0068067 | 6 | 7 | 131 | 9.0 | 2 | 6.5 | 7 |
| Nip/Tuck | tt0361217 | 6 | 6 | 100 | 8.9 | 2 | 6.7 | 6 |
| The Resident | tt6483832 | 6 | 6 | 107 | 9.1 | 2 | 6.9 | 6 |
| Alias | tt0285333 | 5 | 5 | 105 | 9.0 | 2 | 7.1 | 5 |
| Avengers Assemble | tt2455546 | 5 | 5 | 127 | 8.3 | 2 | 5.9 | 5 |
| Chuck | tt0934814 | 5 | 5 | 91 | 9.5 | 2 | 7.4 | 5 |
| Dynasty | tt6128300 | 5 | 5 | 108 | 9.3 | 1 | 6.8 | 5 |
| Falling Skies | tt1462059 | 5 | 5 | 52 | 8.2 | 2 | 5.9 | 5 |
| Get Smart | tt0058805 | 5 | 5 | 138 | 8.8 | 2 | 6.4 | 5 |
| Gomer Pyle: USMC | tt0057752 | 5 | 5 | 150 | 8.8 | 2 | 6.1 | 5 |
| House of Cards | tt1856010 | 5 | 6 | 73 | 9.5 | 2 | 2.5 | 6 |
| Kim's Convenience | tt5912064 | 5 | 5 | 65 | 8.1 | 1 | 7.1 | 5 |
| Mayans M.C. | tt5715524 | 5 | 5 | 50 | 8.9 | 2 | 7.4 | 5 |
| No One Could Live Here | tt0381733 | 5 | 5 | 91 | 8.8 | 1 | 6.3 | 5 |
| Talking Tom & Friends | tt4648640 | 5 | 5 | 158 | 9.2 | 1 | 5.5 | 5 |
| The Kids in the Hall | tt0096626 | 5 | 5 | 111 | 8.3 | 2 | 6.8 | 5 |
| The Last Ship | tt2402207 | 5 | 5 | 56 | 9.1 | 2 | 6.1 | 5 |
| Van Helsing | tt5197820 | 5 | 5 | 65 | 7.7 | 2 | 5.5 | 5 |
| X-Men | tt0103584 | 5 | 5 | 76 | 8.4 | 1 | 5.3 | 5 |
| You | tt7335184 | 5 | 5 | 50 | 9.1 | 2 | 5.3 | 5 |
| iZombie | tt3501584 | 5 | 5 | 71 | 9.1 | 2 | 7.3 | 5 |
| 13 Reasons Why | tt1837492 | 4 | 4 | 49 | 9.1 | 1 | 5.1 | 4 |
| 30 for 30 | tt1408430 | 4 | 5 | 135 | 8.5 | 2 | 5.2 | 5 |
| 31 Minutes | tt0376364 | 4 | 4 | 33 | 9.6 | 2 | 6.5 | 4 |
| Acapulco | tt13567344 | 4 | 4 | 40 | 8.9 | 2 | 6.6 | 4 |
| Alfred Hitchcock Presents | tt0088476 | 4 | 4 | 77 | 8.0 | 1 | 5.1 | 4 |
| Claws | tt5640558 | 4 | 4 | 38 | 8.6 | 2 | 6.9 | 4 |
| Cybill | tt0111932 | 4 | 4 | 86 | 8.4 | 2 | 6.8 | 4 |
| Dear White People | tt5707802 | 4 | 4 | 40 | 8.2 | 1 | 5.3 | 4 |
| Doctor Doctor | tt6015808 | 4 | 5 | 45 | 9.0 | 1 | 7.0 | 5 |
| Due South | tt0108756 | 4 | 4 | 67 | 9.0 | 1 | 7.1 | 4 |
| Frankie Drake Mysteries | tt6881870 | 4 | 4 | 41 | 8.0 | 1 | 6.9 | 4 |
| Good Girls | tt6474378 | 4 | 4 | 50 | 8.4 | 1 | 5.9 | 4 |
| Heroes | tt0813715 | 4 | 4 | 78 | 8.9 | 1 | 6.8 | 4 |
| In Treatment | tt0835434 | 4 | 4 | 130 | 9.1 | 1 | 5.6 | 4 |
| In the Dark | tt7772602 | 4 | 4 | 52 | 8.6 | 2 | 5.8 | 4 |
| Johnny Bravo | tt0118360 | 4 | 4 | 67 | 8.2 | 1 | 6.5 | 4 |
| Moonlighting | tt0088571 | 4 | 5 | 66 | 8.4 | 2 | 6.2 | 5 |
| Mr Selfridge | tt2310212 | 4 | 4 | 40 | 8.6 | 2 | 7.4 | 4 |
| Op-Docs | tt4263816 | 4 | 6 | 3 | 7.5 | 1 | 6.2 | 6 |
| Picket Fences | tt0103512 | 4 | 4 | 87 | 8.6 | 1 | 6.8 | 4 |
| Profiler | tt0115322 | 4 | 4 | 33 | 8.4 | 1 | 6.6 | 4 |
| Roswell, New Mexico | tt7588054 | 4 | 4 | 52 | 8.5 | 1 | 6.2 | 4 |
| Route 66 | tt0053534 | 4 | 4 | 116 | 9.0 | 2 | 5.5 | 4 |
| Sailor Moon | tt0114327 | 4 | 4 | 82 | 8.8 | 1 | 6.3 | 4 |
| Saved by the Bell | tt0096694 | 4 | 4 | 86 | 8.2 | 2 | 6.4 | 4 |
| Scorpion | tt3514324 | 4 | 4 | 93 | 8.6 | 2 | 6.2 | 4 |
| Sliders | tt0112167 | 4 | 5 | 87 | 8.3 | 2 | 4.8 | 5 |
| Soy Luna | tt5189554 | 4 | 4 | 160 | 9.1 | 2 | 2.8 | 4 |
| That's My Life | tt3659018 | 4 | 4 | 16 | 8.3 | 1 | 5.7 | 4 |
| The Boondocks | tt0373732 | 4 | 4 | 56 | 9.3 | 2 | 5.8 | 4 |
| The Cleaning Lady | tt11188682 | 4 | 4 | 46 | 7.9 | 2 | 6.6 | 4 |
| The Commish | tt0101069 | 4 | 5 | 88 | 8.4 | 2 | 7.1 | 5 |
| The Glades | tt1552112 | 4 | 4 | 49 | 7.8 | 1 | 7.0 | 4 |
| The High Chaparral | tt0061263 | 4 | 4 | 97 | 8.8 | 2 | 6.8 | 4 |
| The Last Man on Earth | tt3230454 | 4 | 4 | 67 | 8.2 | 2 | 6.3 | 4 |
| The Librarians | tt3663490 | 4 | 4 | 42 | 8.7 | 2 | 7.4 | 4 |
| The Man in the High Castle | tt1740299 | 4 | 4 | 40 | 9.2 | 2 | 6.0 | 4 |
| The Royals | tt3597912 | 4 | 4 | 40 | 9.0 | 2 | 7.2 | 4 |
| The Untouchables | tt0052522 | 4 | 4 | 118 | 8.6 | 2 | 7.0 | 4 |
| The Valley of the Wolves | tt0421384 | 4 | 4 | 97 | 9.0 | 2 | 6.8 | 4 |
| Through the Wormhole | tt1513168 | 4 | 8 | 62 | 8.3 | 1 | 6.6 | 8 |
| Unforgettable | tt1842530 | 4 | 4 | 61 | 8.2 | 1 | 6.6 | 4 |
| We Bare Bears | tt4839610 | 4 | 4 | 139 | 9.6 | 2 | 6.3 | 4 |
| Welcome Back, Kotter | tt0072582 | 4 | 4 | 95 | 7.8 | 1 | 5.7 | 4 |
| Young Justice | tt1641384 | 4 | 4 | 98 | 9.7 | 2 | 6.8 | 4 |
| 'Til Death | tt0759475 | 3 | 4 | 70 | 8.2 | 1 | 5.6 | 4 |
| All Rise | tt10329042 | 3 | 3 | 58 | 8.4 | 1 | 4.7 | 3 |
| American Dreams | tt0319930 | 3 | 3 | 4 | 7.9 | 1 | 3.4 | 3 |
| And Just Like That... | tt13819960 | 3 | 3 | 33 | 7.6 | 2 | 4.8 | 3 |
| Animaniacs | tt6951546 | 3 | 3 | 36 | 8.2 | 2 | 7.1 | 3 |
| Arifureta: From Commonplace to World's Strongest | tt10431290 | 3 | 3 | 45 | 8.1 | 2 | 6.7 | 3 |
| Atypical | tt6315640 | 3 | 4 | 38 | 9.1 | 2 | 7.4 | 4 |
| Ballers | tt2891574 | 3 | 5 | 47 | 8.5 | 2 | 7.1 | 5 |
| Batman | tt0059968 | 3 | 3 | 120 | 8.4 | 2 | 5.7 | 3 |
| Batman: The Brave and the Bold | tt1213218 | 3 | 3 | 65 | 9.0 | 2 | 6.3 | 3 |
| Beastars | tt11043632 | 3 | 3 | 48 | 8.9 | 2 | 7.0 | 3 |
| Beauty and the Beast | tt0092319 | 3 | 3 | 55 | 9.0 | 2 | 6.5 | 3 |
| Bedrag | tt3638488 | 3 | 3 | 30 | 8.1 | 2 | 7.4 | 3 |
| Ben 10: Alien Force | tt1192169 | 3 | 3 | 46 | 9.2 | 2 | 6.2 | 3 |
| Ben 10: Ultimate Alien | tt1622696 | 3 | 3 | 52 | 9.3 | 1 | 5.1 | 3 |
| Beyblade: Metal Fusion | tt1674928 | 3 | 3 | 119 | 9.8 | 2 | 7.7 | 3 |
| Big Hero 6: The Series | tt5515212 | 3 | 3 | 56 | 8.8 | 2 | 6.2 | 3 |
| Bloodline | tt3520702 | 3 | 3 | 33 | 9.1 | 1 | 3.6 | 3 |
| CatDog | tt0154061 | 3 | 3 | 67 | 7.4 | 1 | 5.6 | 3 |
| Celebrity Big Brother | tt7948268 | 3 | 3 | 12 | 7.5 | 2 | 5.2 | 3 |
| Central Park | tt8129006 | 3 | 3 | 39 | 8.9 | 2 | 6.5 | 3 |
| Children Ruin Everything | tt10738442 | 3 | 4 | 47 | 6.8 | 2 | 5.7 | 4 |
| Crossing Lines | tt2427220 | 3 | 3 | 34 | 8.3 | 2 | 7.1 | 3 |
| Danny Phantom | tt0366005 | 3 | 3 | 49 | 9.3 | 2 | 6.7 | 3 |
| Dead to Me | tt8064302 | 3 | 3 | 30 | 8.6 | 2 | 7.4 | 3 |
| Deadbeat | tt3147316 | 3 | 3 | 36 | 7.8 | 2 | 6.4 | 3 |
| Demon Slayer: Kimetsu no Yaiba | tt9335498 | 3 | 4 | 63 | 9.8 | 2 | 7.1 | 4 |
| Designated Survivor | tt5296406 | 3 | 3 | 53 | 8.5 | 1 | 6.5 | 3 |
| Dicte: Crime Reporter | tt2025899 | 3 | 3 | 30 | 7.6 | 2 | 6.5 | 3 |
| Doom Patrol | tt8416494 | 3 | 4 | 46 | 9.0 | 1 | 6.2 | 4 |
| Explained | tt8005374 | 3 | 3 | 44 | 8.5 | 2 | 6.3 | 3 |
| Fake Profile | tt14494938 | 3 | 3 | 25 | 7.1 | 2 | 3.2 | 3 |
| For My Son | tt4343334 | 3 | 3 | 82 | 9.4 | 1 | 6.6 | 3 |
| GLOW | tt5770786 | 3 | 3 | 30 | 9.0 | 2 | 7.3 | 3 |
| Gargoyles | tt0108783 | 3 | 3 | 78 | 8.5 | 2 | 6.6 | 3 |
| Golden Boy | tt21105088 | 3 | 3 | 93 | 8.7 | 2 | 4.1 | 3 |
| Guardians of the Galaxy | tt4176370 | 3 | 3 | 79 | 7.8 | 1 | 6.0 | 3 |
| H2O: Just Add Water | tt0491603 | 3 | 3 | 78 | 8.6 | 2 | 7.0 | 3 |
| Hemlock Grove | tt2309295 | 3 | 3 | 33 | 7.9 | 2 | 6.3 | 3 |
| House of Villains | tt26695859 | 3 | 3 | 20 | 8.8 | 2 | 5.8 | 3 |
| Inconstant Love | tt9721776 | 3 | 3 | 68 | 8.9 | 1 | 6.8 | 3 |
| Inside Edge | tt6112414 | 3 | 3 | 30 | 8.9 | 2 | 6.3 | 3 |
| Invasion | tt9737326 | 3 | 3 | 30 | 7.3 | 2 | 5.4 | 3 |
| Jessica Jones | tt2357547 | 3 | 3 | 39 | 9.0 | 1 | 7.1 | 3 |
| K.C. Undercover | tt3598030 | 3 | 3 | 76 | 7.9 | 1 | 5.4 | 3 |
| Kung Fu Panda: Legends of Awesomeness | tt1545214 | 3 | 3 | 78 | 8.4 | 2 | 6.5 | 3 |
| Land of the Lost | tt0071005 | 3 | 3 | 43 | 8.3 | 1 | 5.9 | 3 |
| Lethal Weapon | tt5164196 | 3 | 3 | 55 | 9.0 | 1 | 6.2 | 3 |
| Lie to Me | tt1235099 | 3 | 3 | 48 | 8.8 | 1 | 7.0 | 3 |
| Life with Louie | tt0112047 | 3 | 3 | 39 | 9.2 | 2 | 8.1 | 3 |
| Lopez vs. Lopez | tt14852960 | 3 | 3 | 43 | 8.4 | 1 | 6.8 | 3 |
| Merlí | tt4580372 | 3 | 3 | 40 | 9.0 | 1 | 7.0 | 3 |
| Millennium | tt0115270 | 3 | 3 | 67 | 8.8 | 2 | 6.3 | 3 |
| Monty Python's Flying Circus | tt0063929 | 3 | 4 | 45 | 8.9 | 2 | 7.2 | 4 |
| Motherland: Fort Salem | tt9900092 | 3 | 3 | 30 | 8.9 | 2 | 7.5 | 3 |
| Murder in the First | tt2402061 | 3 | 3 | 32 | 8.3 | 2 | 7.2 | 3 |
| My Favorite Martian | tt0056775 | 3 | 3 | 94 | 8.2 | 2 | 6.4 | 3 |
| My Two Dads | tt0092410 | 3 | 3 | 60 | 8.9 | 1 | 7.6 | 3 |
| Mythic Quest | tt8879940 | 3 | 4 | 40 | 9.4 | 1 | 6.5 | 4 |
| Narcos | tt2707408 | 3 | 3 | 30 | 9.4 | 2 | 7.8 | 3 |
| Narcos: Mexico | tt8714904 | 3 | 3 | 30 | 9.0 | 2 | 7.4 | 3 |
| Necessary Roughness | tt1657505 | 3 | 3 | 38 | 8.4 | 2 | 5.4 | 3 |
| Norm Macdonald Live | tt6407712 | 3 | 3 | 21 | 9.8 | 1 | 5.1 | 3 |
| On My Block | tt7879820 | 3 | 4 | 38 | 8.9 | 1 | 7.2 | 4 |
| One Punch Man | tt4508902 | 3 | 3 | 36 | 9.5 | 1 | 1.6 | 3 |
| Our Cartoon President | tt7689460 | 3 | 3 | 46 | 6.5 | 2 | 4.6 | 3 |
| Paradise PD | tt8235236 | 3 | 4 | 40 | 7.4 | 2 | 6.1 | 4 |
| Peter Gunn | tt0051301 | 3 | 3 | 114 | 8.3 | 1 | 6.6 | 3 |
| Physical | tt11828492 | 3 | 3 | 30 | 8.1 | 2 | 6.6 | 3 |
| Primal | tt10332508 | 3 | 3 | 30 | 9.3 | 2 | 7.8 | 3 |
| Producers' Showcase | tt0046637 | 3 | 3 | 3 | 7.9 | 1 | 5.8 | 3 |
| Quantico | tt4428122 | 3 | 3 | 57 | 7.7 | 1 | 4.8 | 3 |
| Roswell | tt0201391 | 3 | 3 | 61 | 8.4 | 1 | 7.0 | 3 |
| Runaways | tt1236246 | 3 | 3 | 33 | 8.0 | 2 | 6.8 | 3 |
| Saving Grace | tt0830900 | 3 | 3 | 46 | 9.0 | 1 | 6.1 | 3 |
| SeaChange | tt0138785 | 3 | 4 | 3 | 9.0 | 1 | 7.1 | 4 |
| Silicon Valley | tt2575988 | 3 | 6 | 53 | 9.3 | 1 | 7.3 | 6 |
| Sneaky Pete | tt5011816 | 3 | 3 | 30 | 9.0 | 1 | 7.4 | 3 |
| Spider-Man | tt0061301 | 3 | 3 | 52 | 7.5 | 2 | 5.0 | 3 |
| Star Trek | tt0060028 | 3 | 3 | 80 | 9.2 | 1 | 5.2 | 3 |
| Stitchers | tt3868848 | 3 | 3 | 31 | 8.6 | 1 | 7.0 | 3 |
| Strangers with Candy | tt0194624 | 3 | 3 | 31 | 8.5 | 2 | 7.3 | 3 |
| Sue Thomas: F.B.Eye | tt0329934 | 3 | 3 | 56 | 9.0 | 2 | 7.1 | 3 |
| Tales from the Cryptkeeper | tt0122837 | 3 | 3 | 28 | 7.9 | 2 | 6.1 | 3 |
| The Bionic Woman | tt0073965 | 3 | 3 | 58 | 8.6 | 2 | 5.6 | 3 |
| The Boat | tt1788634 | 3 | 3 | 29 | 8.9 | 2 | 5.0 | 3 |
| The Girlfriend Experience | tt3846642 | 3 | 3 | 37 | 8.2 | 1 | 5.0 | 3 |
| The Great | tt2235759 | 3 | 3 | 30 | 8.7 | 2 | 7.3 | 3 |
| The Jim Jefferies Show | tt6987966 | 3 | 3 | 24 | 8.3 | 1 | 5.8 | 3 |
| The Little Murders of Agatha Christie | tt1349600 | 3 | 3 | 44 | 8.6 | 2 | 6.4 | 3 |
| The Name of the Game | tt0062591 | 3 | 3 | 2 | 7.8 | 1 | 6.6 | 3 |
| The Penthouse: War in Life | tt13067118 | 3 | 3 | 48 | 8.3 | 2 | 6.1 | 3 |
| The Ricky Gervais Show | tt1553644 | 3 | 3 | 39 | 8.8 | 2 | 7.7 | 3 |
| The Suite Life of Zack & Cody | tt0426371 | 3 | 3 | 87 | 8.2 | 1 | 6.6 | 3 |
| The Twilight Zone | tt0088634 | 3 | 3 | 65 | 7.8 | 1 | 5.4 | 3 |
| The Umbrella Academy | tt1312171 | 3 | 4 | 36 | 8.9 | 2 | 4.8 | 4 |
| Timon & Pumbaa | tt0112197 | 3 | 3 | 85 | 7.6 | 1 | 6.2 | 3 |
| To Your Eternity | tt12063450 | 3 | 3 | 62 | 9.7 | 1 | 6.6 | 3 |
| Turkish for Beginners | tt0469009 | 3 | 3 | 52 | 7.6 | 1 | 6.2 | 3 |
| Tyrant | tt2568204 | 3 | 3 | 32 | 8.8 | 2 | 6.6 | 3 |
| UnREAL | tt3314218 | 3 | 4 | 38 | 8.4 | 2 | 7.2 | 4 |
| Under the Dome | tt1553656 | 3 | 3 | 39 | 7.6 | 1 | 6.1 | 3 |
| Var Bunlar | tt20603316 | 3 | 3 | 39 | 8.8 | 2 | 5.7 | 3 |
| Veronica Mars | tt0412253 | 3 | 4 | 72 | 9.1 | 1 | 5.8 | 4 |
| Versailles | tt3830558 | 3 | 3 | 30 | 8.0 | 1 | 6.9 | 3 |
| What's Happening!! | tt0074071 | 3 | 3 | 48 | 8.6 | 1 | 6.3 | 3 |
| Wu-Tang: An American Saga | tt9113406 | 3 | 3 | 30 | 9.3 | 2 | 5.5 | 3 |
| Your Favorite Martian | tt2039304 | 3 | 3 | 24 | 9.2 | 2 | 4.0 | 3 |
| Zoo | tt3250026 | 3 | 3 | 39 | 7.9 | 2 | 5.9 | 3 |

The query returned **190 series** where an episode with the best rating is in season 1 or 2 and an episode with the worst rating is in the final numbered season. For example, *The Flash* has a best episode rated 9.6 in season 1 and a worst episode rated 3.8 in season 9. *House of Cards* has a best rating of 9.5 in season 2 and a worst rating of 2.5 in season 6. These examples show a large difference between early and late episodes.

We only considered series with at least **three seasons containing 10 or more episodes**, to avoid very short series. We required at least **50 votes per episode** because very small vote counts can make ratings unstable. We used numbered seasons so that the order is clear. When several episodes share the best or worst rating, all best-rated episodes must be in the first two seasons, and all worst-rated episodes must be in the last season. This is stricter than picking just one of the tied episodes.

This is a way to find series that may have become less popular with IMDb users, but it is not proof that the whole series became worse. We compare the best and worst **individual episodes**, not the average rating of every season. Some series also have only a few episodes with enough votes, even if they have many episodes in IMDb. Comparing season averages would give a more general picture, but it would be a different method.

### 2.6 Actors and actresses successful in several languages

*Question: Find any actors/actresses who have achieved success across films in multiple distinct languages […]. Additionally, list all actors/actresses who have achieved success in movies in >=3 languages.*

| Parameter | Value |
|---|---|
| Movies | feature films 1980–2025, ≥ 60 min |
| Success | rating ≥ 7.0 and ≥ 1,000 votes |
| Original language | Wikidata `P364`, linked through `P345` |
| Counting rule | a different movie for each language (maximum matching) |
| Merged language variants | 26 (`reference/language_variants.csv`) |

| Candidate movies | With language | No Wikidata match | No language in Wikidata | Ambiguous (dropped) |
|---|---|---|---|---|
| 10,401 | 9,360 | 387 | 645 | 9 |

Comparison of the counting rules:

| Rule | Variants merged | ≥ 2 languages | ≥ 3 languages |
|---|---|---|---|
| All languages of each movie | yes | 4,611 | 1,977 |
| A different movie for each language | yes | 4,611 | 1,008 |
| Single-language movies only | yes | 1,713 | 262 |
| All languages of each movie | no | 4,763 | 2,113 |
| A different movie for each language | no | 4,763 | 1,094 |
| Single-language movies only | no | 1,861 | 297 |

Result we kept: **4,611** people in at least 2 languages, **1,008** in at least 3.

Breakdown by number of languages:

| Languages | People |
|---|---|
| 2 | 3,603 |
| 3 | 696 |
| 4 | 211 |
| 5 | 72 |
| 6 | 21 |
| 7 | 5 |
| 8 | 1 |
| 10 | 1 |
| 11 | 1 |

Most common languages among these people:

| Language | People |
|---|---|
| English | 2,230 |
| French | 1,025 |
| German | 658 |
| Hindi | 605 |
| Spanish | 533 |
| Tamil | 505 |
| Telugu | 386 |
| Russian | 298 |
| Malayalam | 295 |
| Arabic | 292 |
| Japanese | 288 |
| Mandarin | 266 |
| Italian | 214 |
| Cantonese | 181 |
| Serbian | 131 |

The first 30 people (full list: `results/actors_multilingual.csv`):

| Person | Languages | Detail | Successful movies | Languages listed (without the rule) |
|---|---|---|---|---|
| Bruno Ganz | 11 | French, English, German, Turkish, Icelandic, Modern Greek, Swiss German, Italian, Russian, Swedish, Norwegian | 15 | 14 |
| Tony Leung Chiu-wai | 10 | Spanish, French, Hindi, English, German, Wu Chinese, Japanese, Cantonese, Mandarin, Vietnamese | 20 | 11 |
| Atul Kulkarni | 8 | Hindi, Marathi, English, Kannada, Malayalam, Punjabi, Tamil, Telugu | 14 | 8 |
| Golshifteh Farahani | 7 | Arabic, French, English, German, Turkish, Georgian, Persian | 12 | 8 |
| Erland Josephson | 7 | French, English, German, Modern Greek, Italian, Swedish, Serbian | 8 | 10 |
| Monica Bellucci | 7 | Spanish, French, English, Aramaic, Modern Greek, Italian, Russian | 7 | 12 |
| Ingeborga Dapkunaite | 7 | French, Hindi, English, Chechen, Russian, Swedish, Lithuanian | 7 | 8 |
| Branko Djuric | 7 | Spanish, French, Italian, Slovene, Macedonian, Serbo-Croatian, Bosnian | 7 | 11 |
| Tilda Swinton | 6 | Spanish, French, English, Italian, Russian, Korean | 19 | 7 |
| Tabu | 6 | French, Hindi, English, Malayalam, Tamil, Telugu | 18 | 7 |
| Mukesh Rishi | 6 | Hindi, Marathi, German, Kannada, Tamil, Telugu | 18 | 7 |
| Nikolaj Lie Kaas | 6 | French, English, German, Russian, Swedish, Danish | 17 | 8 |
| Mads Mikkelsen | 6 | French, Hindi, English, German, Swedish, Danish | 17 | 6 |
| Juliette Binoche | 6 | Arabic, French, English, Italian, Romanian, Norwegian | 15 | 7 |
| Javier Bardem | 6 | Spanish, French, English, Wolof, Catalan, Galician | 13 | 8 |
| Ulrich Tukur | 6 | French, English, German, Dutch, Russian, Hebrew | 13 | 6 |
| Max von Sydow | 6 | French, English, German, Swedish, Danish, Norwegian | 12 | 6 |
| Emir Hadžihafizbegović | 6 | English, German, Croatian, Serbian, Serbo-Croatian, Bosnian | 12 | 7 |
| Sachin Khedekar | 6 | Hindi, Marathi, English, Malayalam, Tamil, Telugu | 12 | 6 |
| Miki Manojlović | 6 | English, German, Bulgarian, Serbian, Serbo-Croatian, Bosnian | 12 | 7 |
| Armin Mueller-Stahl | 6 | Arabic, Finnish, English, German, Russian, Hungarian | 11 | 8 |
| Zbigniew Zamachowski | 6 | French, English, German, Russian, Polish, Yiddish | 11 | 6 |
| Trine Dyrholm | 6 | French, German, Swedish, Danish, Norwegian, Serbian | 9 | 7 |
| Chang Chen | 6 | Spanish, English, Japanese, Chinese, Cantonese, Mandarin | 8 | 7 |
| Mustafa Nadarević | 6 | French, Croatian, Albanian, Serbian, Serbo-Croatian, Bosnian | 8 | 9 |
| August Diehl | 6 | English, German, Aramaic, Russian, Korean, Hebrew | 7 | 7 |
| Raymond Thiry | 6 | Arabic, French, English, German, Croatian, Dutch | 7 | 9 |
| Hanns Zischler | 6 | Arabic, English, German, Russian, Danish, Hebrew | 7 | 8 |
| Pål Sverre Hagen | 6 | Finnish, French, English, Swedish, Danish, Norwegian | 7 | 8 |
| Nassar | 5 | Hindi, German, Malayalam, Tamil, Telugu | 65 | 6 |

With our chosen method, we found **4,611 actors and actresses** who worked in successful movies linked to at least two languages, including **1,008** linked to at least three. Bruno Ganz is at the top of the sample list with 11 languages, followed by Tony Leung Chiu-wai with 10. English, French and German are common in the results, but many other languages also appear.

IMDb does not give a reliable original-language field for every movie, so we used Wikidata. We linked a movie with its IMDb ID (`P345`) and read its original-language information (`P364`). We then connected those movie languages to actors and actresses through their film credits. For success, we required a rating of at least **7.0** and at least **1,000 votes**. This prevents us from counting every small or poorly rated appearance as a successful movie.

We chose the rule **one different movie for each language**. Without this rule, one multilingual film could count as several languages by itself. We also merged 26 language variants, so very similar language labels would not increase the result unfairly. A stricter alternative is to use only single-language films: that gives 262 people in at least three languages, but it removes many valid multilingual films.

There are important limits. Out of 10,401 candidate movies, 387 had no Wikidata match, 645 had no language there, and 9 ambiguous movies were dropped. Also, a film's original language does **not** prove that an actor speaks that language. Our result is about the languages of their successful films, not their personal language skills.

### 2.7 Careers with an early peak (1,947)

*Question: Find all actors/actresses who have been in movies for 20+ years and at least 15 or more movies whose popularity peaked early in their career.*

| Parameter | Value |
|---|---|
| Career length | ≥ 20 years |
| Movies | ≥ 15 rated movies |
| Minimum votes per movie | 1,000 |
| Last year included | 2025 |
| Early career | first half of the period (inclusive) |
| Criterion | maximum rating AND mean rating both higher in the early career |

| Gap in mean rating (early − late) | Value |
|---|---|
| Mean | 0.66 |
| Median | 0.56 |
| Maximum | 4.18 |

The 30 careers with the most movies (full list: `results/actors_early_rating_peak.csv`):

| Person | Movies | Period | Early movies | Late movies | Early peak | Late peak | Early mean | Late mean |
|---|---|---|---|---|---|---|---|---|
| Amitabh Bachchan | 121 | 1971–2024 | 41 | 80 | 8.3 | 8.1 | 7.05 | 6.07 |
| Akshay Kumar | 119 | 1991–2025 | 55 | 64 | 8.2 | 8.1 | 5.83 | 5.79 |
| Brahmanandam | 114 | 1987–2025 | 29 | 85 | 8.9 | 8.7 | 7.28 | 6.26 |
| Tanikella Bharani | 110 | 1989–2025 | 24 | 86 | 8.8 | 8.5 | 6.99 | 6.36 |
| Samuel L. Jackson | 106 | 1987–2025 | 53 | 53 | 8.8 | 8.5 | 6.46 | 6.28 |
| Gérard Depardieu | 102 | 1973–2022 | 46 | 56 | 8.1 | 7.5 | 6.75 | 6.01 |
| Bruce Willis | 101 | 1987–2023 | 39 | 62 | 8.8 | 7.7 | 6.48 | 4.93 |
| Mohanlal | 99 | 1985–2025 | 45 | 54 | 8.8 | 8.4 | 8.07 | 6.04 |
| Robert De Niro | 98 | 1968–2025 | 43 | 55 | 9.0 | 8.3 | 6.89 | 6.21 |
| Willem Dafoe | 96 | 1981–2025 | 32 | 64 | 8.2 | 8.1 | 6.62 | 6.35 |
| John Carradine | 86 | 1936–1989 | 50 | 36 | 8.1 | 7.6 | 6.39 | 4.80 |
| Siddique | 86 | 1987–2025 | 13 | 73 | 9.0 | 8.4 | 7.73 | 6.28 |
| Harvey Keitel | 85 | 1967–2025 | 43 | 42 | 8.2 | 8.1 | 6.41 | 5.89 |
| Bette Davis | 84 | 1931–1989 | 67 | 17 | 8.2 | 8.0 | 6.88 | 6.65 |
| Morgan Freeman | 83 | 1980–2024 | 30 | 53 | 9.3 | 9.1 | 6.78 | 6.24 |
| Kemal Sunal | 81 | 1972–1999 | 58 | 23 | 9.1 | 7.3 | 7.44 | 6.65 |
| Mammootty | 79 | 1982–2025 | 24 | 55 | 8.9 | 8.6 | 7.87 | 6.56 |
| Liam Neeson | 78 | 1983–2025 | 30 | 48 | 9.0 | 8.2 | 6.48 | 6.34 |
| Anthony Hopkins | 77 | 1968–2025 | 29 | 48 | 8.6 | 8.2 | 6.63 | 6.32 |
| Susan Sarandon | 77 | 1970–2024 | 26 | 51 | 7.6 | 7.1 | 6.68 | 6.06 |
| Dermot Mulroney | 77 | 1988–2025 | 28 | 49 | 7.5 | 7.2 | 6.21 | 5.50 |
| Danny Trejo | 77 | 1995–2025 | 29 | 48 | 7.5 | 7.2 | 5.19 | 4.61 |
| Danny Glover | 76 | 1984–2022 | 27 | 49 | 7.7 | 7.6 | 6.59 | 5.66 |
| Dennis Quaid | 73 | 1979–2025 | 33 | 40 | 7.8 | 7.5 | 6.46 | 6.15 |
| Brian Cox | 73 | 1986–2025 | 29 | 44 | 7.8 | 7.7 | 6.67 | 6.13 |
| James Stewart | 72 | 1935–1991 | 60 | 12 | 8.6 | 7.6 | 7.03 | 6.72 |
| Steve Buscemi | 72 | 1986–2025 | 35 | 37 | 8.2 | 7.6 | 6.52 | 6.14 |
| Michael Madsen | 72 | 1987–2024 | 31 | 41 | 8.2 | 7.8 | 5.84 | 4.39 |
| Anthony Quinn | 71 | 1937–2002 | 52 | 19 | 8.3 | 8.2 | 6.80 | 6.21 |
| Alec Baldwin | 71 | 1988–2024 | 39 | 32 | 8.5 | 7.6 | 6.30 | 5.94 |

We found **1,947 actors and actresses** whose included movie ratings were better in the first part of their careers. The average difference between the early and late mean ratings is **0.66 points**, and the median difference is 0.56. For example, Bruce Willis has an early average of 6.48 and a later average of 4.93 in the table. Robert De Niro also fits our rule, with an early average of 6.89 and a later average of 6.21.

We selected people with at least **20 years** between their first and last included films and at least **15 rated movies**. Each film needed at least **1,000 votes**, and we used movies up to 2025. We split each career into two time periods at the middle of the person's career years, with the middle year included in the early part. To count as an early peak, both the **highest rating** and the **average rating** had to be higher in the early part. This gave us a more complete test than looking at one famous film only.

We call this an early peak in **IMDb ratings**, not necessarily in real popularity or box-office success. The method also has limits: the two halves do not always have the same number of movies, and IMDb audiences may rate films from different periods differently. Other options would be to compare vote counts, use box office, or split each career by number of films rather than by years.

### 2.8 Multi-talented professionals

*Question: Identify film professionals who can be considered "multi-talented" […]. How do these individuals perform when performing multiple roles on the same project?*

People who worked in at least two of the roles actor, director, writer: **242,663**. Vote threshold for the rating comparison: 1,000.

| Roles combined on the same movie | Person/movie pairs | People |
|---|---|---|
| director + writer | 329,043 | 157,759 |
| actor + director + writer | 52,067 | 33,370 |
| actor + writer | 43,513 | 32,858 |
| actor + director | 19,906 | 13,270 |

Comparison, for the same person, between the movies where they combine roles and the movies where they have only one role:

| Comparable people | Mean gap (multi − single) | Median gap | Better mean when multi-role | Share |
|---|---|---|---|---|
| 8,697 | -0.001 | 0.031 | 4,441 | 51.1% |

Examples:

| Person | Roles | Multi-role movies | Single-role movies | Multi-role mean | Single-role mean | Gap |
|---|---|---|---|---|---|---|
| Woody Allen | ['actor', 'director', 'writer'] | 54 | 7 | 6.89 | 5.87 | 1.02 |
| Werner Herzog | ['actor', 'director', 'writer'] | 41 | 6 | 7.11 | 6.47 | 0.64 |
| Claude Chabrol | ['actor', 'director', 'writer'] | 37 | 8 | 6.82 | 6.75 | 0.07 |
| Ingmar Bergman | ['actor', 'director', 'writer'] | 36 | 6 | 7.33 | 7.20 | 0.13 |
| Jean-Luc Godard | ['actor', 'director', 'writer'] | 35 | 5 | 6.65 | 6.22 | 0.43 |
| Tyler Perry | ['actor', 'director', 'writer'] | 35 | 7 | 5.36 | 6.43 | -1.07 |
| Akira Kurosawa | ['director', 'writer'] | 31 | 10 | 7.54 | 6.96 | 0.58 |
| Jesús Franco | ['actor', 'director', 'writer'] | 30 | 8 | 4.86 | 4.54 | 0.32 |
| Satyajit Ray | ['actor', 'director', 'writer'] | 28 | 6 | 7.93 | 7.05 | 0.88 |
| Wim Wenders | ['actor', 'director', 'writer'] | 27 | 8 | 7.00 | 6.41 | 0.58 |
| Clint Eastwood | ['actor', 'director'] | 26 | 40 | 6.89 | 6.90 | -0.01 |
| Billy Wilder | ['actor', 'director', 'writer'] | 26 | 16 | 7.38 | 6.98 | 0.40 |
| Sylvester Stallone | ['actor', 'director', 'writer'] | 25 | 38 | 6.32 | 5.61 | 0.71 |
| Luis Buñuel | ['actor', 'director', 'writer'] | 25 | 4 | 7.36 | 7.28 | 0.09 |
| Éric Rohmer | ['actor', 'director', 'writer'] | 25 | 1 | 7.15 | 5.50 | 1.65 |
| Kamal Haasan | ['actor', 'director', 'writer'] | 25 | 28 | 7.51 | 7.69 | -0.18 |
| Yasujirō Ozu | ['director', 'writer'] | 25 | 2 | 7.61 | 7.00 | 0.61 |
| Priyadarshan | ['director', 'writer'] | 25 | 15 | 6.82 | 7.12 | -0.30 |
| Mani Ratnam | ['director', 'writer'] | 25 | 3 | 7.41 | 6.33 | 1.07 |
| Blake Edwards | ['actor', 'director', 'writer'] | 24 | 16 | 6.17 | 6.46 | -0.29 |
| Hark Tsui | ['actor', 'director', 'writer'] | 24 | 30 | 6.37 | 6.23 | 0.14 |
| Carlo Verdone | ['actor', 'director', 'writer'] | 24 | 8 | 6.23 | 6.33 | -0.10 |
| Jing Wong | ['actor', 'director', 'writer'] | 24 | 11 | 6.11 | 6.56 | -0.45 |
| Federico Fellini | ['actor', 'director', 'writer'] | 23 | 7 | 7.25 | 7.46 | -0.21 |
| Rainer Werner Fassbinder | ['actor', 'director', 'writer'] | 23 | 4 | 7.04 | 6.52 | 0.52 |

We first looked for people who have worked in at least two of three roles: **actor, director and writer**. There are **242,663** people in this group. The most frequent combination on the same movie is director and writer, with 329,043 person/movie pairs. Doing all three roles on the same film is less common, but it still appears 52,067 times.

To see whether combining roles makes a difference, we compared each person's mean movie rating when they had several roles on the same film with their mean rating when they had just one role. Only movies with at least **1,000 votes** were used for this rating comparison. We had **8,697 people** with both kinds of movies. Their average difference was almost zero (**-0.001 points**), and 51.1% had a better mean when combining roles.

So we did not find a clear overall advantage from taking more roles. Some people do much better when they combine roles, while others do not. For example, Woody Allen has a positive gap of 1.02 points in this comparison, while Tyler Perry has a negative gap of 1.07 points. These are comparisons, not proof that having several roles causes a film to be better or worse. The film type, budget, period and audience can all affect ratings.

### 2.9 Methodological Explanation (exploration)

For the exploration questions, we used a similar process: filter the relevant IMDb records, join the needed tables, group the data, and calculate the result with PySpark. We used IMDb IDs rather than movie names for joins, because different films can have the same title. We also separated columns containing several values, such as movie genres, directors or actors, when we needed to study each value.

The choices depend on the question. We used the IQR rule for unusual movie lengths because it is less affected by extreme runtimes. We used votes for genre attention, ratings for the director and career questions, and a minimum number of votes when a result could be misleading without enough feedback. The TV-series question also needed episode-to-series links and the order of seasons. The language question needed Wikidata because the IMDb files did not have all the information we needed.

We could have used other thresholds or measures in almost every section. Higher vote limits would make ratings more stable, but we would lose many smaller movies and series. Weighted averages could give more importance to widely rated films, but then very famous films would dominate the analysis. Our goal was to use simple rules that we can explain and repeat. The main limit is that IMDb ratings and votes show the opinion and activity of IMDb users, not every viewer in the world.

## 3. Enrichment

*Question: For all feature-length films released between 1980 and 2025, enrich the dataset by including a film synopsis for each of the top 10 films per genre for each year and biographical information for at least three principals from each film. […] every enriched record must maintain an explicit lineage field citing its source URL/URI […] all other operations such as cleaning, schema enforcement, etc., must be performed using Spark.*

### 3.1 Movie selection

| Parameter | Value |
|---|---|
| Years | 1980–2025 |
| Minimum runtime | 60 min |
| Movies per genre and per year | 10 |
| Movie ranking | `IMDb_numVotes_desc_then_averageRating_desc_then_tconst` |
| Principals per movie | 3 |
| Principal ranking | `IMDb_ordering_asc_then_nconst` |

| Genre/year groups | Movie/genre/year rows | Distinct movies | Principal credits | Movies with < 3 principals |
|---|---|---|---|---|
| 1,071 | 10,161 | 7,152 | 21,404 | 32 |

### 3.2 Wikipedia coverage

| Movie status | Code | Movies | Share |
|---|---|---|---|
| Enriched | `enriched` | 5,927 | 82.9% |
| No Wikidata entity | `no_wikidata_match` | 461 | 6.4% |
| Article without a Plot/Synopsis section | `plot_section_missing` | 432 | 6.0% |
| No single English article | `no_unique_english_article` | 326 | 4.6% |
| Several Wikidata entities | `ambiguous_wikidata_match` | 6 | 0.1% |

| Biography status (per credit) | Code | Credits | Share |
|---|---|---|---|
| Enriched | `enriched` | 18,071 | 84.4% |
| No Wikidata entity | `no_wikidata_match` | 2,057 | 9.6% |
| No single English article | `no_unique_english_article` | 1,239 | 5.8% |
| Several Wikidata entities | `ambiguous_wikidata_match` | 19 | 0.1% |
| Introduction too short (< 50 characters) | `biography_missing` | 18 | 0.1% |

| Indicator | Value |
|---|---|
| Movies with a synopsis | 5,927 / 7,152 (82.9%) |
| Movies with 3 biographies | 5,220 (73.0%) |
| Distinct people with a biography | 8,008 |
| Movies with fewer than 3 principals in IMDb | 32 |

Coverage by decade:

| Decade | Movies | With synopsis | Share | With 3 biographies | Share |
|---|---|---|---|---|---|
| 1980s | 1,507 | 1,203 | 79.8% | 1,059 | 70.3% |
| 1990s | 1,529 | 1,261 | 82.5% | 1,130 | 73.9% |
| 2000s | 1,493 | 1,294 | 86.7% | 1,191 | 79.8% |
| 2010s | 1,655 | 1,371 | 82.8% | 1,191 | 72.0% |
| 2020s | 968 | 798 | 82.4% | 649 | 67.0% |

Synopsis coverage by genre:

| Genre | Movies | With synopsis | Share |
|---|---|---|---|
| Animation | 460 | 437 | 95.0% |
| Family | 460 | 447 | 97.2% |
| War | 460 | 398 | 86.5% |
| History | 460 | 414 | 90.0% |
| Adventure | 460 | 459 | 99.8% |
| Horror | 460 | 460 | 100.0% |
| Sci-Fi | 460 | 458 | 99.6% |
| Romance | 460 | 459 | 99.8% |
| Documentary | 460 | 227 | 49.3% |
| Fantasy | 460 | 458 | 99.6% |
| Music | 460 | 412 | 89.6% |
| Biography | 460 | 440 | 95.7% |
| Comedy | 460 | 460 | 100.0% |
| Thriller | 460 | 460 | 100.0% |
| Crime | 460 | 460 | 100.0% |
| Drama | 460 | 460 | 100.0% |
| Mystery | 460 | 460 | 100.0% |
| Action | 460 | 459 | 99.8% |
| Musical | 460 | 390 | 84.8% |
| Sport | 460 | 382 | 83.0% |
| Western | 406 | 197 | 48.5% |
| Adult | 346 | 24 | 6.9% |
| News | 133 | 20 | 15.0% |
| Reality-TV | 57 | 4 | 7.0% |
| Talk-Show | 16 | 0 | 0.0% |
| Game-Show | 3 | 0 | 0.0% |

| Synopsis length (characters) | Value |
|---|---|
| Minimum | 55 |
| Median | 3,614 |
| Maximum | 13,583 |

Examples of enriched movies (truncated text):

| Movie | Year | Start of the synopsis | Source revision | License |
|---|---|---|---|---|
| Animalympics | 1980 | The film is a series of vignettes presented as the broadcast of the first animal Olympic Games through the fic… | https://en.wikipedia.org/w/index.php?oldid=1372144226 | https://creativecommons.org/licenses/by-sa/4.0/ |
| The Architecture of Doom | 1989 | The film explores the obsession Adolf Hitler had with his own particular vision of what was and was not aesthe… | https://en.wikipedia.org/w/index.php?oldid=1364558836 | https://creativecommons.org/licenses/by-sa/4.0/ |
| Mystery, Alaska | 1999 | In Mystery, Alaska, "the Saturday Game", a weekly hockey game, is played on an open pond. The entire town turn… | https://en.wikipedia.org/w/index.php?oldid=1383823774 | https://creativecommons.org/licenses/by-sa/4.0/ |
| Frost/Nixon | 2008 | After the Watergate scandal of 1972–74, 400 million people worldwide watch on television as former United Stat… | https://en.wikipedia.org/w/index.php?oldid=1385307601 | https://creativecommons.org/licenses/by-sa/4.0/ |
| Pirates of the Caribbean: Dead Men Tell No Tales | 2017 | 13 years after Will Turner first leaves Elizabeth Swann aboard the Flying Dutchman, Will is visited by his twe… | https://en.wikipedia.org/w/index.php?oldid=1384142397 | https://creativecommons.org/licenses/by-sa/4.0/ |

Examples of biographies (truncated text):

| Person | Category | Movie | Start of the biography | Source revision |
|---|---|---|---|---|
| Meg Ryan | actress | tt0035423 | Margaret Mary Emily Anne Hyra (born November 19, 1961), known professionally as Meg Ryan, is an American actre… | https://en.wikipedia.org/w/index.php?oldid=1385293372 |
| Joe Pesci | actor | tt0099785 | Joseph Frank Pesci ( PESH-ee Italian: [ˈpeʃʃi]; born February 9, 1943) is an American actor and musician. He i… | https://en.wikipedia.org/w/index.php?oldid=1382105468 |
| Frankie Muniz | actor | tt0156812 | Francisco James Muniz IV (; born December 5, 1985) is an American actor and racing driver. Muniz came to promi… | https://en.wikipedia.org/w/index.php?oldid=1385714691 |
| Madison Pettis | actress | tt0492956 | Madison Michelle Pettis (born July 22, 1998) is an American actress. She is known for her roles as Sophie Mart… | https://en.wikipedia.org/w/index.php?oldid=1380762341 |
| Liam Hemsworth | actor | tt1976989 | Liam Hemsworth (born 13 January 1990) is an Australian actor. He played the roles of Josh Taylor in the soap o… | https://en.wikipedia.org/w/index.php?oldid=1385721438 |

Lineage fields on every row: `article_url`, `source_uri`, `revision_id`, `contributors_uri`, `license_uri`, `retrieved_at`, `enrichment_status` (plus `principal_note` and `biography_note` for movies).

### 3.3 Methodological Explanation

For this part, we selected feature-length movies released from **1980 to 2025**, with a runtime of at least **60 minutes**. We ranked the films separately for each genre and year, mainly by IMDb vote count. If movies had the same number of votes, we used the rating and then the IMDb ID to keep a fixed order. We selected up to ten films in each group. This created **10,161 movie/genre/year rows**, representing **7,152 different movies**. One movie can appear in several genres.

For each movie, we selected the first three people in IMDb's principal-credit order. There were **21,404 credits**, and 32 movies had fewer than three principal credits available. We did not invent missing people. We used the IMDb IDs to find matching Wikidata entities and English Wikipedia pages. We collected the movie Plot/Synopsis sections and the short biographical introductions of the selected people.

The external collection step is needed to request information from Wikipedia and Wikidata. After that, PySpark is used for the data operations: joining the collected information to IMDb, checking the schema, cleaning fields, and preparing the enriched output. Keeping this separation makes the processing easier to repeat.

We kept the source information with the records: article URL, revision URL or URI, revision ID, contributors URI, license URI, retrieval time and an enrichment status. This is important because Wikipedia content changes. A revision link lets someone find the version we used, while the contributor and license fields help document the source and the reuse conditions. Wikipedia text is reused under its license conditions; keeping the links does not remove the need to follow those conditions when publishing the content.

We enriched **5,927 movies with a synopsis (82.9%)**. We also had **5,220 movies with three biographies (73.0%)**, and **8,008 different people** with a biography. The missing results are recorded with statuses, such as no Wikidata match or no suitable English article. Some genres have very high synopsis coverage, but Documentary, Western and a few smaller genres have much lower coverage. This shows that external data is not equally available for every kind of film.

For a larger project, we could save and reuse fetched articles, avoid requesting the same page several times, and update only records whose revision changed. We could also use another source for films without a Wikipedia plot. We chose Wikipedia because it is accessible and has clear page and revision links, but it is not complete, and an automated match should not be accepted when it is ambiguous.

## 4. Streaming

*Question: Design and implement a PySpark Structured Streaming pipeline that ingests live Wikipedia change events via Kafka and identifies updates relevant to your IMDb dataset. […] The REPORT.md should include an explanation of your chosen method to deal with late arriving data.*

| Parameter | Value |
|---|---|
| Source | <https://stream.wikimedia.org/v2/stream/recentchange> |
| Events kept by the producer | English Wikipedia article edits and creations |
| IMDb titles compared | rated films and series, 1980-2025, at least 100 votes |
| Kafka topic | `wikipedia-recentchange` (1 partition) |
| Kafka read | `startingOffsets = latest` |
| Trigger | `processingTime = 30 seconds` |
| Watermark | 30 minutes on `event_time` |
| Deduplication | `dropDuplicatesWithinWatermark(event_id)` |
| Page filter | explicit suffix: `(film)`, `(2021 film)`, `(TV series)`… |

| Metric | Value |
|---|---|
| Run ID | `20261114T145223Z` |
| First event | 2026-11-14 14:53:20 |
| Last event | 2026-11-14 15:21:57 |
| Batches | 20 |
| Events processed | 2,288 |
| Distinct matching events | 19 |
| Event/IMDb title pairs | 21 |
| Distinct IMDb titles | 17 |

Metrics per batch:

| Batch | Events | Matches | First event | Last event |
|---|---|---|---|---|
| 0 | 1 | 0 | 2026-11-14 14:53:20 | 2026-11-14 14:53:20 |
| 1 | 4 | 0 | 2026-11-14 14:53:21 | 2026-11-14 14:53:29 |
| 2 | 33 | 0 | 2026-11-14 14:53:26 | 2026-11-14 14:53:56 |
| 3 | 36 | 0 | 2026-11-14 14:53:58 | 2026-11-14 14:54:27 |
| 4 | 38 | 1 | 2026-11-14 14:54:25 | 2026-11-14 14:54:55 |
| 5 | 43 | 1 | 2026-11-14 14:54:47 | 2026-11-14 14:55:27 |
| 6 | 36 | 0 | 2026-11-14 14:55:22 | 2026-11-14 14:55:57 |
| 7 | 39 | 0 | 2026-11-14 14:55:29 | 2026-11-14 14:56:27 |
| 8 | 39 | 0 | 2026-11-14 14:56:26 | 2026-11-14 14:56:56 |
| 9 | 39 | 1 | 2026-11-14 14:56:48 | 2026-11-14 14:57:24 |
| 10 | 26 | 0 | 2026-11-14 14:57:25 | 2026-11-14 14:57:52 |
| 11 | 40 | 0 | 2026-11-14 14:57:52 | 2026-11-14 14:58:27 |
| 12 | 32 | 0 | 2026-11-14 14:58:23 | 2026-11-14 14:58:55 |
| 13 | 33 | 0 | 2026-11-14 14:58:37 | 2026-11-14 14:59:21 |
| 14 | 22 | 2 | 2026-11-14 14:59:15 | 2026-11-14 14:59:37 |
| 15 | 1,474 | 14 | 2026-11-14 14:59:20 | 2026-11-14 15:17:27 |
| 16 | 237 | 1 | 2026-11-14 15:17:25 | 2026-11-14 15:20:25 |
| 17 | 42 | 0 | 2026-11-14 15:20:24 | 2026-11-14 15:20:57 |
| 18 | 40 | 0 | 2026-11-14 15:20:56 | 2026-11-14 15:21:27 |
| 19 | 34 | 1 | 2026-11-14 15:21:20 | 2026-11-14 15:21:57 |

All matches (`results/streaming_match_examples.csv`):

| Time | Wikipedia page | Type | IMDb | IMDb title | IMDb type | Year | Votes |
|---|---|---|---|---|---|---|---|
| 2026-11-14 14:54:44 | Trigger Point (TV series) | edit | tt11958610 | Trigger Point | tvSeries | 2022 | 12,041 |
| 2026-11-14 14:55:18 | Anna Karenina (2013 TV series) | edit | tt2821430 | Anna Karenina | tvMiniSeries | 2013 | 786 |
| 2026-11-14 14:57:07 | Beautiful Boy (2018 film) | edit | tt1226837 | Beautiful Boy | movie | 2018 | 134,212 |
| 2026-11-14 14:59:27 | 24 (TV series) | edit | tt0285331 | 24 | tvSeries | 2001 | 208,864 |
| 2026-11-14 14:59:27 | 24 (TV series) | edit | tt2792284 | 24 | tvSeries | 2013 | 3,708 |
| 2026-11-14 15:01:23 | Leave the World Behind (film) | edit | tt12747748 | Leave the World Behind | movie | 2023 | 249,468 |
| 2026-11-14 15:01:23 | Leave the World Behind (film) | edit | tt3288290 | Leave the World Behind | movie | 2014 | 2,426 |
| 2026-11-14 15:02:41 | Kudrat (1998 film) | edit | tt0286772 | Kudrat | movie | 1998 | 202 |
| 2026-11-14 15:05:26 | Naal (film) | edit | tt9130246 | Naal | movie | 2018 | 1,442 |
| 2026-11-14 15:06:14 | Banished (TV series) | edit | tt3508050 | Banished | tvMiniSeries | 2015 | 4,532 |
| 2026-11-14 15:07:02 | Battlefield Earth (film) | edit | tt0185183 | Battlefield Earth | movie | 2000 | 85,392 |
| 2026-11-14 15:08:12 | Blessed (2004 film) | edit | tt0390450 | Blessed | movie | 2004 | 2,472 |
| 2026-11-14 15:09:04 | Bra Boys (film) | edit | tt0951318 | Bra Boys | movie | 2007 | 1,389 |
| 2026-11-14 15:09:20 | Beautiful Boy (2018 film) | edit | tt1226837 | Beautiful Boy | movie | 2018 | 134,212 |
| 2026-11-14 15:13:18 | Beautiful Boy (2018 film) | edit | tt1226837 | Beautiful Boy | movie | 2018 | 134,212 |
| 2026-11-14 15:13:28 | Clueless (TV series) | edit | tt0115137 | Clueless | tvSeries | 1996 | 4,048 |
| 2026-11-14 15:13:43 | After Hours (film) | edit | tt0088680 | After Hours | movie | 1985 | 101,944 |
| 2026-11-14 15:15:02 | After Hours (film) | edit | tt0088680 | After Hours | movie | 1985 | 101,944 |
| 2026-11-14 15:15:29 | After Hours (film) | edit | tt0088680 | After Hours | movie | 1985 | 101,944 |
| 2026-11-14 15:17:32 | Wentworth (TV series) | edit | tt2433738 | Wentworth | tvSeries | 2013 | 27,377 |
| 2026-11-14 15:21:48 | Case Closed (TV series) | edit | tt0105968 | Case Closed | tvSeries | 1993 | 421 |

### 4.1 Handling late data

In a live stream, an event can arrive later than the time when it actually happened. For example, the connection may be slow or the producer may send several older events together. We use the event timestamp (`event_time`) and a **30-minute watermark** to handle this problem.

The watermark lets Spark keep some information about recent events while it checks for duplicate event IDs. We use `dropDuplicatesWithinWatermark(event_id)` so that the same event is not normally counted twice if it arrives inside that time window. This is useful for a stream that can reconnect or resend events.

The trade-off is that an event arriving **more than 30 minutes late**, after the watermark has passed its event time, may be dropped and will not be reliably included in the result. A longer watermark would accept more late events, but Spark would need to keep deduplication information for longer and use more memory. A shorter one would be lighter but could miss more events. We chose 30 minutes as a reasonable setting for this test. It is not a guarantee that every late Wikipedia edit will be processed.

### 4.2 Methodological Explanation

Our streaming pipeline starts with Wikimedia's **recentchange** event stream. A producer keeps edits and page creations from English Wikipedia and sends them to the Kafka topic `wikipedia-recentchange`. PySpark Structured Streaming then reads the topic and processes new messages in batches every **30 seconds**. We ran these services with Docker.

We compare the article titles from Wikipedia with a set of IMDb films and series released between **1980 and 2025** that have at least **100 votes**. Before matching, we apply a filter for clear page names, such as titles ending in `(film)`, `(2021 film)` or `(TV series)`. This reduces many unrelated Wikipedia events. We then match the candidate pages to our IMDb titles and keep the relevant events.

In our test run on **14 November 2026**, we processed **2,288 events** in **20 batches**. We found **19 distinct matching events**, producing **21 event/title pairs** linked to **17 IMDb titles**. There are more pairs than events because some Wikipedia page names can match more than one IMDb title, such as *24* or *Leave the World Behind*. Also, a movie page can be edited several times.

This method is simple enough to run continuously, but the matching is not perfect. A page can have a different name from the IMDb title, and some articles do not use the suffixes we filter for. On the other side, the same name can refer to different productions. A more exact method would use Wikipedia or Wikidata IDs, but it would need more joins or external lookups. We used `startingOffsets = latest`, so this run only looks at new Kafka messages after the consumer starts, not the full Wikipedia history. The topic has one partition in this setup, which also limits how far this exact configuration can scale.

## 5. Open question

*Question: Propose and answer a question related to the IMDB and related datasets that was not covered by previous questions. […] it must require some complex analytics such as multi-table joins. […] 1. What is the question being investigated, why is it interesting and what additional data (if any) was required […] 2. Results […] 3. […] methodology used, why it was chosen, and what alternatives were considered.*

Question: *How do ratings change from the first two to the last two films of recurring director–actor pairs?*

Our extra question is: **How do IMDb ratings change between the first two and the last two films made by the same director and actor?** We wanted to see whether a team that works together many times tends to keep the same level of ratings, improve, or get lower ratings later.

This question needs several IMDb tables. We use `title.basics` to select movies and years, `title.ratings` for votes and ratings, `title.crew` to find directors, `title.principals` to find actors, and `name.basics` for their names. We did not need another external dataset. We only kept feature films from **1980 to 2025**, at least **60 minutes** long, with **1,000 votes or more**. A director and actor needed at least **five shared movies** to be included.

| Parameter | Value |
|---|---|
| Years | 1980–2025 |
| Minimum runtime | 60 min |
| Minimum votes per movie | 1,000 |
| Minimum shared movies | 5 |
| Movies compared at each end | 2 |
| Measure | `last_two_mean_minus_first_two_mean` |
| IMDb tables | `title.basics`, `title.ratings`, `title.crew`, `title.principals`, `name.basics` |

| Pairs | Mean change | Median change | Up | Down | Unchanged |
|---|---|---|---|---|---|
| 660 | -0.59 | -0.50 | 134 | 516 | 10 |

Distribution of the changes:

| Change | Pairs | Share |
|---|---|---|
| ≤ −2 | 39 | 5.9% |
| (−2, −1] | 132 | 20.0% |
| (−1, −0.5] | 161 | 24.4% |
| (−0.5, 0) | 184 | 27.9% |
| 0 | 10 | 1.5% |
| (0, 0.5) | 91 | 13.8% |
| [0.5, 1) | 33 | 5.0% |
| ≥ 1 | 10 | 1.5% |

Change by number of shared movies:

| Shared movies | Pairs | Mean change | Median change |
|---|---|---|---|
| 5 | 328 | -0.56 | -0.45 |
| 6–7 | 224 | -0.51 | -0.40 |
| 8–10 | 80 | -0.76 | -0.60 |
| > 10 | 28 | -1.11 | -0.55 |

The 20 pairs with the most shared movies:

| Director | Actor | Movies | Period | Mean of first 2 | Min. votes | Mean of last 2 | Min. votes | Change |
|---|---|---|---|---|---|---|---|---|
| Kartal Tibet | Kemal Sunal | 23 | 1980–1990 | 7.65 | 4,453 | 6.95 | 3,093 | -0.70 |
| Johnnie To | Suet Lam | 20 | 1998–2016 | 7.15 | 1,314 | 6.15 | 2,295 | -1.00 |
| Kunihiko Yuyama | Ikue Otani | 18 | 1998–2019 | 6.25 | 26,525 | 6.00 | 7,296 | -0.25 |
| David Dhawan | Govinda | 16 | 1990–2009 | 6.65 | 1,442 | 4.85 | 1,484 | -1.80 |
| Priyadarshan | Mohanlal | 15 | 1985–2021 | 7.60 | 1,221 | 6.55 | 5,143 | -1.05 |
| Neri Parenti | Paolo Villaggio | 13 | 1980–1996 | 6.90 | 2,091 | 5.45 | 1,471 | -1.45 |
| Priyadarshan | Paresh Rawal | 13 | 2000–2021 | 7.10 | 1,146 | 5.15 | 4,622 | -1.95 |
| Puri Jagannadh | Mohammad Ali | 13 | 2000–2024 | 7.20 | 1,098 | 2.75 | 2,425 | -4.45 |
| Togan Gökbakar | Şahan Gökbakar | 13 | 2006–2024 | 4.65 | 3,832 | 4.55 | 1,190 | -0.10 |
| Woody Allen | Mia Farrow | 12 | 1982–1992 | 7.10 | 22,163 | 7.10 | 19,246 | 0.00 |
| Neri Parenti | Christian De Sica | 12 | 1985–2008 | 4.80 | 1,293 | 4.35 | 1,389 | -0.45 |
| Priyadarshan | Rajpal Naurang Yadav | 12 | 2003–2021 | 7.25 | 19,613 | 4.75 | 6,957 | -2.50 |
| Kartal Tibet | Reha Yurdakul | 12 | 1981–1989 | 6.85 | 4,453 | 6.60 | 1,970 | -0.25 |
| Johnnie To | Eddie Cheung | 12 | 1993–2016 | 6.50 | 1,136 | 6.00 | 1,096 | -0.50 |
| Johnnie To | Simon Yam | 12 | 1998–2009 | 7.25 | 1,314 | 6.65 | 3,350 | -0.60 |
| Rohit Shetty | Ajay Devgn | 12 | 2003–2024 | 6.45 | 1,855 | 5.05 | 76,647 | -1.40 |
| Andrew Jones | Lee Bane | 12 | 2015–2018 | 2.90 | 1,080 | 3.10 | 1,000 | 0.20 |
| Garry Marshall | Hector Elizondo | 11 | 1984–2010 | 6.05 | 6,616 | 5.85 | 29,581 | -0.20 |
| Abbas Alibhai Burmawalla | Johny Lever | 11 | 1992–2017 | 7.35 | 9,039 | 3.60 | 1,106 | -3.75 |
| Mastan Alibhai Burmawalla | Johny Lever | 11 | 1992–2017 | 7.35 | 9,039 | 3.60 | 1,106 | -3.75 |

The 10 largest drops:

| Director | Actor | Movies | Period | Mean of first 2 | Min. votes | Mean of last 2 | Min. votes | Change |
|---|---|---|---|---|---|---|---|---|
| Puri Jagannadh | Mohammad Ali | 13 | 2000–2024 | 7.20 | 1,098 | 2.75 | 2,425 | -4.45 |
| Abbas Alibhai Burmawalla | Johny Lever | 11 | 1992–2017 | 7.35 | 9,039 | 3.60 | 1,106 | -3.75 |
| Mastan Alibhai Burmawalla | Johny Lever | 11 | 1992–2017 | 7.35 | 9,039 | 3.60 | 1,106 | -3.75 |
| Nikita Mikhalkov | Oleg Menshikov | 5 | 1982–2011 | 7.55 | 1,852 | 4.20 | 2,360 | -3.35 |
| Jing Wong | Yun-Fat Chow | 6 | 1989–2016 | 6.95 | 3,164 | 3.65 | 1,309 | -3.30 |
| Prabhu Deva | Prakash Raj | 5 | 2005–2011 | 7.75 | 4,453 | 4.50 | 1,127 | -3.25 |
| David Zucker | Leslie Nielsen | 5 | 1980–2008 | 7.65 | 226,982 | 4.55 | 10,557 | -3.10 |
| Kartal Tibet | Ayşen Gruda | 6 | 1981–2006 | 7.10 | 1,329 | 4.05 | 3,179 | -3.05 |
| Khaled Marei | Ahmed Helmy | 5 | 2008–2019 | 8.05 | 8,347 | 5.10 | 1,543 | -2.95 |
| Patryk Vega | Andrzej Grabowski | 7 | 2005–2020 | 6.60 | 2,105 | 3.75 | 1,304 | -2.85 |

The 10 largest rises:

| Director | Actor | Movies | Period | Mean of first 2 | Min. votes | Mean of last 2 | Min. votes | Change |
|---|---|---|---|---|---|---|---|---|
| Mehmet Ada Öztekin | Sarp Akkaya | 6 | 2017–2024 | 5.75 | 1,398 | 8.75 | 3,520 | 3.00 |
| Jesse V. Johnson | Scott Adkins | 7 | 2005–2020 | 4.65 | 1,309 | 6.20 | 6,350 | 1.55 |
| John Lyde | Melanie Stone | 5 | 2014–2016 | 4.20 | 1,025 | 5.55 | 1,804 | 1.35 |
| Pedro Almodóvar | Carmen Maura | 7 | 1980–2006 | 6.30 | 7,033 | 7.55 | 51,589 | 1.25 |
| Timothy Woodward Jr. | Michael Paré | 6 | 2015–2017 | 3.40 | 1,359 | 4.45 | 1,855 | 1.05 |
| Harley Wallen | Kaiti Wallen | 8 | 2018–2025 | 3.50 | 2,512 | 4.55 | 1,276 | 1.05 |
| Edward Drake | Lochlyn Munro | 5 | 2021–2023 | 2.75 | 7,337 | 3.75 | 2,047 | 1.00 |
| Andrei Konchalovsky | Yuliya Vysotskaya | 5 | 2002–2020 | 6.25 | 1,215 | 7.25 | 2,185 | 1.00 |
| Edward Drake | Bruce Willis | 7 | 2021–2023 | 2.75 | 7,337 | 3.75 | 2,047 | 1.00 |
| Pedro Almodóvar | Julieta Serrano | 7 | 1980–2021 | 6.30 | 7,033 | 7.30 | 41,137 | 1.00 |

Full list: `results/director_actor_collaborations.csv`.

We found **660 director–actor pairs**. The mean rating change between the first two and last two shared movies is **-0.59 points**, and the median change is **-0.50**. In total, **516 pairs** have a lower rating at the end, **134 pairs** have a higher rating, and **10 pairs** have no change. This means that about **78%** of the pairs in our selection have a negative change.

There are some large differences. The pair Puri Jagannadh and Mohammad Ali goes from an early mean rating of 7.20 to 2.75, a change of -4.45 points. On the other hand, Mehmet Ada Öztekin and Sarp Akkaya go from 5.75 to 8.75, an increase of 3.00 points. We also see different patterns for pairs with many shared films. For example, Woody Allen and Mia Farrow have the same mean of 7.10 in their first and last two included movies.

The results suggest that many repeated collaborations in this selection have lower IMDb ratings in their later films. Still, we cannot say that working together more often causes lower ratings. Many other things change over time, including the type of movies made, the audiences and the careers of the people involved.

### 5.1 Methodological Explanation

We first joined the movie information with ratings, directors and actor credits. We used the IMDb IDs for all joins, then built the director–actor pairs for every movie. We grouped the shared movies by pair and ordered them by release year. For each pair with at least five eligible films, we calculated the mean rating of the **first two** and the mean rating of the **last two**. Our measure is simply:

**Rating change = mean of last two movies − mean of first two movies.**

We chose two movies at each end instead of only one because a single very good or bad film could have too much effect. We also required five shared films so that there is a real collaboration history between the two periods. A minimum of 1,000 votes per movie helps avoid ratings based on very small samples.

Other methods are possible. We could compare the first three and last three movies, use a line showing ratings over time, or weight ratings by the number of votes. We could also compare the pairs with directors or actors who did not work together again. These methods could give a more complete answer, but they need different assumptions.

There are limits. The result is based on films included in IMDb and on current IMDb ratings, even for much older movies. Film years are not always a perfect production order, and the same movie can create several director–actor pairs. Some pairs also share the same films, so the 660 results are not fully independent. Finally, our analysis compares ratings; it does not explain the reasons behind them.

## Conclusion

This project helped us use PySpark on large IMDb files and work with several types of data processing. We first checked missing and invalid values, then explored movies, series and film careers. We also added information from Wikipedia and built a Kafka streaming pipeline for live page changes.

Some results were quite clear. Drama is the leading genre by our IMDb vote measure in most years. We found 21,323 movies with unusual runtimes for at least one genre, 1,008 actors and actresses linked to successful films in at least three languages, and 190 series matching our early-best and late-worst episode rule. In our extra question, 516 of the 660 director–actor pairs had lower ratings in their last two shared films. The Wikipedia enrichment covered 82.9% of the selected movies with a synopsis, and the streaming test found relevant changes in live Wikipedia events.

The main difficulty was choosing rules that are useful without losing too much data. IMDb has missing fields, its ratings depend on its users, and some Wikipedia information is not available or cannot be matched safely. We tried to make these limits clear and keep the original data and the source links when possible. In the end, the project shows how big data tools can help answer detailed questions, while the results still need to be interpreted carefully.

