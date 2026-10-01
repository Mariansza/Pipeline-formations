from dagster import AssetSelection, Definitions, define_asset_job, in_process_executor
from dagster_dbt import DbtCliResource

from .dbt_assets import formations_dbt_assets, projet_dbt
from .ingestion import (
    cartographie_structure_attendue,
    parcoursup_structure_attendue,
    raw_cartographie,
    raw_parcoursup_2025,
)

# Un job nommé qui exécute tout le pipeline : ingestion, dbt, et tous les contrôles.
# Pas de sélection à faire dans l'interface : Jobs > pipeline_complet > Launch run.
pipeline_complet = define_asset_job(
    "pipeline_complet",
    selection=AssetSelection.all(),
    description="Télécharge les sources, construit tous les modèles dbt et lance tous les contrôles.",
)

defs = Definitions(
    assets=[raw_parcoursup_2025, raw_cartographie, formations_dbt_assets],
    asset_checks=[parcoursup_structure_attendue, cartographie_structure_attendue],
    jobs=[pipeline_complet],
    resources={"dbt": DbtCliResource(project_dir=projet_dbt)},
    # DuckDB n'accepte qu'un seul processus en écriture : on exécute les étapes l'une après l'autre
    # dans le même processus, au lieu de les lancer en parallèle dans des processus séparés.
    executor=in_process_executor,
)
