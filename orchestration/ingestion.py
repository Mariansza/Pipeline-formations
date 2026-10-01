import shutil
import urllib.request
from pathlib import Path

import duckdb
from dagster import AssetCheckResult, AssetKey, MaterializeResult, asset, asset_check

RACINE = Path(__file__).parent.parent
DOSSIER_BRUT = RACINE / "data" / "raw"
BASE_DUCKDB = RACINE / "formations.duckdb"

URL_PARCOURSUP = (
    "https://data.education.gouv.fr/api/explore/v2.1/catalog/datasets/"
    "fr-esr-parcoursup/exports/csv?use_labels=false"
)
URL_CARTOGRAPHIE = (
    "https://data.enseignementsup-recherche.gouv.fr/api/explore/v2.1/catalog/datasets/"
    "fr-esr-cartographie_formations_parcoursup/exports/csv?use_labels=false"
)

# Les clés d'asset reprennent celles que Dagster donne aux sources dbt (source « brut »,
# voir dbt/models/staging/_sources.yml) : c'est ce qui relie ces assets au graphe dbt.
CLE_PARCOURSUP = AssetKey(["brut", "raw_parcoursup_2025"])
CLE_CARTOGRAPHIE = AssetKey(["brut", "raw_cartographie"])

# Seuils de vraisemblance, très en dessous des volumes observés (14 252 et 157 509 lignes).
MIN_LIGNES_PARCOURSUP = 10_000
MIN_LIGNES_CARTOGRAPHIE = 100_000


def _telecharger(url: str, destination: Path) -> None:
    # On écrit dans un fichier temporaire puis on le renomme : un téléchargement interrompu
    # ne laisse jamais un CSV tronqué à la place du bon.
    destination.parent.mkdir(parents=True, exist_ok=True)
    partiel = destination.with_suffix(".csv.part")
    requete = urllib.request.Request(url, headers={"User-Agent": "pipeline-formations"})
    with urllib.request.urlopen(requete, timeout=600) as reponse, open(partiel, "wb") as sortie:
        shutil.copyfileobj(reponse, sortie)
    partiel.replace(destination)


def _charger(table: str, csv: Path) -> int:
    # Copie fidèle du CSV, sans nettoyage : le nettoyage est le rôle du staging dbt.
    connexion = duckdb.connect(str(BASE_DUCKDB))
    try:
        connexion.execute(f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM read_csv(?)", [str(csv)])
        return connexion.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
    finally:
        connexion.close()


def _structure(table: str, colonnes_cles: list[str], min_lignes: int) -> AssetCheckResult:
    connexion = duckdb.connect(str(BASE_DUCKDB), read_only=True)
    try:
        nb_lignes = connexion.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        colonnes = {ligne[0] for ligne in connexion.execute(f"DESCRIBE {table}").fetchall()}
    finally:
        connexion.close()
    manquantes = [c for c in colonnes_cles if c not in colonnes]
    return AssetCheckResult(
        passed=nb_lignes >= min_lignes and not manquantes,
        metadata={"nb_lignes": nb_lignes, "min_lignes": min_lignes, "colonnes_manquantes": manquantes},
    )


@asset(key=CLE_PARCOURSUP, group_name="ingestion", description="Vœux et réponses Parcoursup 2025, copie brute.")
def raw_parcoursup_2025() -> MaterializeResult:
    csv = DOSSIER_BRUT / "parcoursup_2025.csv"
    _telecharger(URL_PARCOURSUP, csv)
    return MaterializeResult(metadata={"nb_lignes": _charger("raw_parcoursup_2025", csv), "url": URL_PARCOURSUP})


@asset(key=CLE_CARTOGRAPHIE, group_name="ingestion", description="Cartographie des formations Parcoursup, copie brute.")
def raw_cartographie() -> MaterializeResult:
    csv = DOSSIER_BRUT / "cartographie.csv"
    _telecharger(URL_CARTOGRAPHIE, csv)
    return MaterializeResult(metadata={"nb_lignes": _charger("raw_cartographie", csv), "url": URL_CARTOGRAPHIE})


# blocking=True : si le contrôle échoue, les assets dbt en aval ne sont pas exécutés.
@asset_check(asset=CLE_PARCOURSUP, blocking=True, description="Assez de lignes et colonne clé présente.")
def parcoursup_structure_attendue() -> AssetCheckResult:
    return _structure("raw_parcoursup_2025", ["cod_aff_form"], MIN_LIGNES_PARCOURSUP)


@asset_check(asset=CLE_CARTOGRAPHIE, blocking=True, description="Assez de lignes et colonnes clés présentes.")
def cartographie_structure_attendue() -> AssetCheckResult:
    return _structure("raw_cartographie", ["gta", "annee"], MIN_LIGNES_CARTOGRAPHIE)
