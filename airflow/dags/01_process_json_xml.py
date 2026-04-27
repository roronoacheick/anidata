"""
🎌 AniData Lab - Convert JSON/XML to CSV
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import json
import pandas as pd
import xml.etree.ElementTree as ET

default_args = {
    "owner": "anidata-lab",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def convert_files():
    """Convertit JSON et XML en CSV et retourne la liste via XCom."""
    output_dir = "/opt/airflow/output"
    data_dir = "/opt/airflow/data"
    converted_files = []
    
    print("\n📝 Conversion JSON/XML → CSV\n")
    
    # Convertir JSON
    for directory in [data_dir, output_dir]:
        if os.path.exists(directory):
            for file in os.listdir(directory):
                if file.endswith(".json"):
                    try:
                        path = os.path.join(directory, file)
                        with open(path) as f:
                            data = json.load(f)
                        df = pd.DataFrame(data if isinstance(data, list) else [data])
                        csv_path = path.replace(".json", "_converted.csv")
                        df.to_csv(csv_path, index=False, encoding="utf-8")
                        converted_files.append(csv_path)
                        print(f"✅ {file} → CSV")
                    except Exception as e:
                        print(f"❌ {file}: {e}")
    
    # Convertir XML
    for directory in [data_dir, output_dir]:
        if os.path.exists(directory):
            for file in os.listdir(directory):
                if file.endswith(".xml"):
                    try:
                        path = os.path.join(directory, file)
                        tree = ET.parse(path)
                        root = tree.getroot()
                        rows = [
                            {elem.tag: elem.text for elem in child}
                            for child in root
                        ]
                        df = pd.DataFrame(rows)
                        csv_path = path.replace(".xml", "_converted.csv")
                        df.to_csv(csv_path, index=False, encoding="utf-8")
                        converted_files.append(csv_path)
                        print(f"✅ {file} → CSV")
                    except Exception as e:
                        print(f"❌ {file}: {e}")
    
    print(f"\n✅ {len(converted_files)} fichier(s) converti(s)\n")
    
    # Retourner via XCom pour le 1er DAG
    return {"converted_files": converted_files}


with DAG(
    dag_id="01_process_json_xml",
    default_args=default_args,
    description="Convertit JSON et XML en CSV",
    schedule_interval=None,
    start_date=datetime(2026, 3, 26),
    catchup=False,
    tags=["anidata"],
) as dag:

    task_convert = PythonOperator(
        task_id="convert_to_csv",
        python_callable=convert_files,
    )
