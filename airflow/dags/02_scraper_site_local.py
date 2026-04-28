"""
🌐 AniData Lab - DAG de scraping du site local
Pipeline pour scraper le site mock-site et récupérer les données anime
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import os


default_args = {
    "owner": "anidata-lab",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def start_scraping():
    """Fonction de démarrage du scraping"""
    print("\n" + "="*60)
    print("  🌐 DÉMARRAGE DU SCRAPING DU SITE LOCAL")
    print("="*60 + "\n")
    print("🔗 Ciblant : http://localhost:8000/")
    print("📊 Données : Pages anime du site mock-site")
    print("💾 Destination : /opt/airflow/data/scraped_data/")
    print("\n" + "="*60 + "\n")


def verify_scraped_data():
    """Vérifie que les données scrappées ont bien été créées"""
    import os
    
    scraped_dir = "/opt/airflow/data/scraped_data"
    
    print("\n" + "="*60)
    print("  ✅ VÉRIFICATION DES DONNÉES SCRAPPÉES")
    print("="*60 + "\n")
    
    if os.path.exists(scraped_dir):
        files = os.listdir(scraped_dir)
        print(f"✅ {len(files)} fichier(s) trouvé(s):")
        for f in files:
            print(f"   📄 {f}")
    else:
        print(f"⚠️  Dossier {scraped_dir} non trouvé")
    
    print("\n" + "="*60 + "\n")


with DAG(
    dag_id="02_scraper_site_local",
    default_args=default_args,
    description="Scrape le site local mock-site pour récupérer les données anime",
    schedule_interval=None,  # Déclenchement manuel ou via GitHub Actions
    start_date=datetime(2026, 3, 23),
    catchup=False,
    tags=["anidata", "scraper", "production"],
) as dag:

    task_start = PythonOperator(
        task_id="start_scraping",
        python_callable=start_scraping,
        doc_md="Démarrage du scraping du site local",
    )

    # Scraping du site (utilise le script du projet anidata-scraper)
    task_scrape = BashOperator(
        task_id="scrape_site",
        bash_command="cd /opt/airflow && python anidata-scraper/anidata_scraper/scraper.py",
        doc_md="Scrape le site local http://localhost:8000/",
    )

    # Vérification des données scrappées
    task_verify = PythonOperator(
        task_id="verify_scraped_data",
        python_callable=verify_scraped_data,
        doc_md="Vérifie que les données ont bien été scrappées",
    )

    # Définir le flux : démarrage → scraping → vérification
    task_start >> task_scrape >> task_verify
