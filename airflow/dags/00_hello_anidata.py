"""
🎌 AniData Lab - DAG de bienvenue
Pipeline complet d'analyse anime : audit → nettoyage → feature engineering → validation → indexation ES
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import TaskInstance
import pandas as pd
import os


default_args = {
    "owner": "anidata-lab",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def hello_anidata():
    """Fonction de test : bienvenue dans AniData Lab !"""
    print("🎌 Bienvenue dans AniData Lab !")
    print("=" * 50)
    print("Votre environnement Airflow est opérationnel.")
    print("Prêt à construire votre pipeline anime/manga !")
    print("=" * 50)
    return "Hello AniData !"


def check_data_files():
    """Vérifie que les fichiers CSV sont accessibles."""
    import os

    data_dir = "/opt/airflow/data"
    expected_files = [
        "anime.csv",
        "rating_complete.csv",
        "anime_with_synopsis.csv",
    ]

    print(f"📂 Vérification du dossier : {data_dir}")
    print("-" * 50)

    found = []
    missing = []

    for filename in expected_files:
        filepath = os.path.join(data_dir, filename)
        if os.path.exists(filepath):
            size_mb = os.path.getsize(filepath) / (1024 * 1024)
            print(f"  ✅ {filename} ({size_mb:.1f} MB)")
            found.append(filename)
        else:
            print(f"  ❌ {filename} - MANQUANT")
            missing.append(filename)

    print("-" * 50)
    print(f"Trouvés : {len(found)} / {len(expected_files)}")

    if missing:
        print(f"⚠️  Fichiers manquants : {missing}")
        print("Téléchargez-les depuis Kaggle et placez-les dans ./data/")
    else:
        print("🎉 Tous les fichiers sont présents !")

    return {"found": found, "missing": missing}


def enrich_with_external_data(ti):
    """Récupère les fichiers convertis du 2e DAG via XCom."""
    
    print("\n" + "="*60)
    print("  🔄 ENRICHISSEMENT AVEC DONNÉES EXTERNES")
    print("="*60 + "\n")
    
    try:
        # Récupérer les fichiers convertis du 2e DAG via XCom
        data = ti.xcom_pull(dag_id="01_process_json_xml", task_ids="convert_to_csv")
        
        if data and data.get("converted_files"):
            converted_files = data["converted_files"]
            print(f"✅ Récupéré {len(converted_files)} fichier(s) du 2e DAG:")
            for f in converted_files:
                print(f"   📄 {os.path.basename(f)}")
            print("\n✅ Enrichissement réussi!")
        else:
            print("ℹ️  Aucun fichier converti trouvé (exécute d'abord le 2e DAG)")
    except Exception as e:
        print(f"ℹ️  Info: {e}")
        print("   (Le 2e DAG doit être exécuté au préalable)")
    
    print("="*60 + "\n")


with DAG(
    dag_id="00_hello_anidata",
    default_args=default_args,
    description="Pipeline complet AniData Lab : audit → nettoyage → feature eng → validation → ES",
    schedule_interval=None,  # Déclenchement manuel uniquement
    start_date=datetime(2026, 3, 23),
    catchup=False,
    tags=["anidata", "pipeline", "production"],
) as dag:

    task_hello = PythonOperator(
        task_id="hello_anidata",
        python_callable=hello_anidata,
        doc_md="Bienvenue dans le pipeline AniData Lab",
    )

    task_check = PythonOperator(
        task_id="check_data_files",
        python_callable=check_data_files,
        doc_md="Vérifies la présence des fichiers CSV",
    )

    # Audit des données brutes
    task_audit = BashOperator(
        task_id="01_audit_complet",
        bash_command="python /opt/airflow/scripts/01_audit_complet.py",
        doc_md="Audit complet des données brutes",
    )

    # Nettoyage des données
    task_nettoyage = BashOperator(
        task_id="03_nettoyage",
        bash_command="python /opt/airflow/scripts/03_nettoyage.py",
        doc_md="Nettoyage et préparation des données",
    )

    # Feature Engineering
    task_feature_eng = BashOperator(
        task_id="04_feature_engineering",
        bash_command="python /opt/airflow/scripts/04_feature_engineering.py",
        doc_md="Création des features pour le ML",
    )

    # Validation des données
    task_validation = BashOperator(
        task_id="05_validation",
        bash_command="python /opt/airflow/scripts/05_validation.py",
        doc_md="Validation et contrôle qualité",
    )
    
    # Enrichissement avec données externes (2e DAG)
    task_enrich = PythonOperator(
        task_id="07_enrichissement_externe",
        python_callable=enrich_with_external_data,
        doc_md="Enrichit les données avec les fichiers convertis du 2e DAG",
    )

    # Indexation dans Elasticsearch
    task_indexation = BashOperator(
        task_id="06_indexation_elasticsearch",
        bash_command="python /opt/airflow/scripts/message.py",
        doc_md="Indexation des données dans Elasticsearch",
    )

    # Définir le flux : audit → nettoyage → feature eng → validation → enrichissement → ES
    task_hello >> task_check >> task_audit >> task_nettoyage >> task_feature_eng >> task_validation >> task_enrich >> task_indexation
