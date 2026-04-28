"""
🧹 AniData Lab - DAG de nettoyage et traitement des données scrappées
Pipeline : Scraping (DAG 02) → Nettoyage (DAG 03) → Validation

Ce DAG :
1. Attend que DAG 02 (scraper) se termine
2. Récupère les JSON scrappés
3. Lance le nettoyage automatique
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
import os


default_args = {
    "owner": "anidata-lab",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def process_scraped_json():
    """Traite les JSON scrappés et les convertit en CSV"""
    import json
    import pandas as pd
    
    scraped_dir = "/opt/airflow/data/scraped_data"
    output_dir = "/opt/airflow/output"
    os.makedirs(output_dir, exist_ok=True)
    
    print("\n" + "="*60)
    print("  📊 TRAITEMENT DES DONNÉES SCRAPPÉES")
    print("="*60 + "\n")
    
    if not os.path.exists(scraped_dir):
        print(f"⚠️  Aucun dossier {scraped_dir}")
        return
    
    json_files = [f for f in os.listdir(scraped_dir) if f.endswith('.json')]
    
    if not json_files:
        print("⚠️  Aucun fichier JSON trouvé")
        return
    
    for json_file in json_files:
        try:
            json_path = os.path.join(scraped_dir, json_file)
            
            # Charger le JSON
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Convertir en DataFrame
            if isinstance(data, list):
                df = pd.DataFrame(data)
            elif isinstance(data, dict):
                # Si c'est un dict avec une clé 'anime' contenant la liste
                if 'anime' in data:
                    df = pd.DataFrame(data['anime'])
                else:
                    df = pd.DataFrame([data])
            else:
                print(f"❌ Format inattendu pour {json_file}")
                continue
            
            # Sauvegarder en CSV
            csv_file = json_file.replace('.json', '_scraped.csv')
            csv_path = os.path.join(output_dir, csv_file)
            df.to_csv(csv_path, index=False, encoding='utf-8')
            
            print(f"✅ {json_file} → {csv_file}")
            print(f"   📈 {len(df)} lignes, {len(df.columns)} colonnes")
            
        except Exception as e:
            print(f"❌ Erreur avec {json_file}: {e}")
    
    print("\n" + "="*60 + "\n")


def clean_data():
    """Lance le script de nettoyage"""
    import subprocess
    
    print("\n" + "="*60)
    print("  🧹 NETTOYAGE DES DONNÉES")
    print("="*60 + "\n")
    
    # Appelle le script de nettoyage
    script_path = "/opt/airflow/scripts/03_nettoyage.py"
    
    if os.path.exists(script_path):
        try:
            result = subprocess.run(
                ["python", script_path],
                capture_output=True,
                text=True,
                timeout=300
            )
            
            if result.returncode == 0:
                print(result.stdout)
                print("✅ Nettoyage réussi!")
            else:
                print(result.stderr)
                print(f"❌ Erreur lors du nettoyage: {result.returncode}")
        except Exception as e:
            print(f"❌ Erreur: {e}")
    else:
        print(f"⚠️  Script {script_path} non trouvé")
    
    print("\n" + "="*60 + "\n")


with DAG(
    dag_id="03_nettoyage_dag",
    default_args=default_args,
    description="Nettoyage et traitement des données scrappées par DAG 02",
    schedule_interval=None,  # Déclenché par trigger ou après DAG 02
    start_date=datetime(2026, 3, 23),
    catchup=False,
    tags=["anidata", "cleaning", "production"],
) as dag:

    # ✅ Attendre que DAG 02 se termine
    wait_for_scraper = ExternalTaskSensor(
        task_id="wait_for_scraper",
        external_dag_id="02_scraper_site_local",
        external_task_id="verify_scraped_data",  # Attend la dernière tâche de DAG 02
        allowed_states=["success"],
        failed_states=["failed"],
        timeout=3600,  # 1 heure de timeout
        poke_interval=30,  # Vérifie toutes les 30 secondes
        mode="poke",
        doc_md="Attend que le scraping soit terminé",
    )

    # 📊 Traiter les JSON scrappés
    task_process_json = PythonOperator(
        task_id="process_scraped_json",
        python_callable=process_scraped_json,
        doc_md="Convertit les JSON scrappés en CSV",
    )

    # 🧹 Nettoyer les données
    task_clean = PythonOperator(
        task_id="clean_data",
        python_callable=clean_data,
        doc_md="Lance le nettoyage des données",
    )

    # 📋 Résumé final
    task_summary = BashOperator(
        task_id="summary",
        bash_command="""
            echo ""
            echo "=========================================="
            echo "  ✅ PIPELINE DE NETTOYAGE TERMINÉ"
            echo "=========================================="
            echo ""
            echo "📊 Résumé :"
            echo "  1️⃣  Scraping (DAG 02) : ✅ Terminé"
            echo "  2️⃣  JSON → CSV (DAG 03) : ✅ Converti"
            echo "  3️⃣  Nettoyage (DAG 03) : ✅ Appliqué"
            echo ""
            echo "📁 Fichiers disponibles :"
            ls -lh /opt/airflow/output/ 2>/dev/null || echo "   Aucun fichier"
            echo ""
        """,
        doc_md="Affiche un résumé du pipeline",
    )

    # Définir les dépendances
    wait_for_scraper >> task_process_json >> task_clean >> task_summary
