# =============================================================================
# Image Docker custom AniData Airflow
# =============================================================================
# Étend l'image officielle Apache Airflow 2.x avec :
# - le code du scraper (package anidata_scraper)
# - les DAGs (00_hello_anidata, 01_process_json_xml, 02_scraper_site_local)
# - les dépendances Python supplémentaires
#
# Cette image est buildée et publiée automatiquement par .github/workflows/ci-cd.yml
# sur GitHub Container Registry (GHCR) à chaque merge sur main.
#
# Build context = racine du repo anidata/, donc tous les COPY référencent
# les sous-dossiers anidata-scraper/ et airflow/dags/.
# =============================================================================

# Image de base officielle, version pinnée pour la reproductibilité
FROM apache/airflow:2.10.4-python3.10

# Métadonnées (visibles sur GHCR)
LABEL org.opencontainers.image.title="AniData Airflow"
LABEL org.opencontainers.image.description="Airflow custom pour AniData Lab — scraper + DAGs"
LABEL org.opencontainers.image.licenses="MIT"

# On reste en utilisateur airflow pour la sécurité (jamais en root pour pip)
USER airflow

# --- Dépendances du scraper ---------------------------------------------------
# On copie d'abord uniquement les requirements pour profiter du cache Docker :
# tant que requirements.txt n'a pas changé, cette couche n'est pas reconstruite.
COPY --chown=airflow:root anidata-scraper/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# --- Code du scraper ----------------------------------------------------------
# Le package est installé pour être importable depuis n'importe quel DAG
# via `from anidata_scraper import scrape_to_file`, ou exécutable via
# `python -m anidata_scraper.scraper`.
COPY --chown=airflow:root anidata-scraper/pyproject.toml /opt/airflow/scraper/pyproject.toml
COPY --chown=airflow:root anidata-scraper/anidata_scraper/ /opt/airflow/scraper/anidata_scraper/
RUN pip install --no-cache-dir /opt/airflow/scraper/

# --- DAGs ---------------------------------------------------------------------
# Les DAGs sont copiés dans le dossier que Airflow scanne par défaut.
COPY --chown=airflow:root airflow/dags/ /opt/airflow/dags/

# --- Dossier de données -------------------------------------------------------
# Création des dossiers où le scraper écrit ses fichiers JSON.
# Ces dossiers sont montés en volume dans docker-compose pour persistance.
RUN mkdir -p /opt/airflow/data/raw /opt/airflow/data/scraped_data

# Vérification rapide à la fin du build : le package s'importe correctement
RUN python -c "import anidata_scraper; print(f'AniData Scraper {anidata_scraper.__version__} OK')"
