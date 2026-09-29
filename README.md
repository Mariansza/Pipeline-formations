# Pipeline formations

Référentiel de formations post-bac construit à partir de deux jeux Parcoursup en open data, avec DuckDB, dbt Core et Dagster. Projet d'entraînement au Data Engineering, construit étape par étape.

Objectif final : une table canonique `formations` (une ligne par formation), enrichie d'un taux d'admission, avec des contrôles de qualité automatisés, une vue agrégée par académie, le tout orchestré.

## Sources

- [Cartographie des formations Parcoursup](https://www.data.gouv.fr/datasets/cartographie-des-formations-parcoursup) (sessions 2020 à 2026)
- [Parcoursup 2025 : vœux et réponses des établissements](https://www.data.gouv.fr/datasets/parcoursup-2025-voeux-de-poursuite-detudes-et-de-reorientation-dans-lenseignement-superieur-et-reponses-des-etablissements-1)

Données sous Licence Ouverte v2.0.

## Reconstruire l'état actuel

Prérequis : [uv](https://docs.astral.sh/uv/).

```bash
# 1. Client DuckDB
uv tool install duckdb-cli

# 2. Données brutes (exports générés à la volée, noms de colonnes techniques)
mkdir -p data/raw
curl -fL -o data/raw/parcoursup_2025.csv "https://data.education.gouv.fr/api/explore/v2.1/catalog/datasets/fr-esr-parcoursup/exports/csv?use_labels=false"
curl -fL -o data/raw/cartographie.csv "https://data.enseignementsup-recherche.gouv.fr/api/explore/v2.1/catalog/datasets/fr-esr-cartographie_formations_parcoursup/exports/csv?use_labels=false"

# 3. Chargement dans la base
duckdb formations.duckdb
```

Dans le shell DuckDB :

```sql
CREATE OR REPLACE TABLE raw_parcoursup_2025 AS SELECT * FROM read_csv('data/raw/parcoursup_2025.csv');
CREATE OR REPLACE TABLE raw_cartographie    AS SELECT * FROM read_csv('data/raw/cartographie.csv');
```

Les CSV (environ 103 Mo) et `formations.duckdb` ne sont pas versionnés.
