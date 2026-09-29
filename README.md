# Pipeline formations

Référentiel de formations post-bac construit à partir de deux jeux Parcoursup en open data, avec DuckDB, dbt Core et Dagster. Projet d'entraînement au Data Engineering, construit étape par étape.

Objectif final : une table canonique `formations` (une ligne par formation), enrichie d'un taux d'admission, avec des contrôles de qualité automatisés, une vue agrégée par académie, le tout orchestré.

## Sources

- [Cartographie des formations Parcoursup](https://www.data.gouv.fr/datasets/cartographie-des-formations-parcoursup) (sessions 2020 à 2026)
- [Parcoursup 2025 : vœux et réponses des établissements](https://www.data.gouv.fr/datasets/parcoursup-2025-voeux-de-poursuite-detudes-et-de-reorientation-dans-lenseignement-superieur-et-reponses-des-etablissements-1)
