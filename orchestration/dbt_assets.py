from pathlib import Path

from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

# Le projet dbt vit dans dbt/ : Dagster lit son manifeste (la description de tous les modèles
# et de leurs dépendances) pour créer un asset par modèle.
projet_dbt = DbtProject(project_dir=Path(__file__).parent.parent / "dbt")
projet_dbt.prepare_if_dev()


@dbt_assets(manifest=projet_dbt.manifest_path)
def formations_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    # dbt build = construire les modèles et lancer leurs tests, dans l'ordre du graphe.
    # Les tests dbt deviennent des « asset checks » rattachés au modèle testé.
    yield from dbt.cli(["build"], context=context).stream()
