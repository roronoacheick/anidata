#!/usr/bin/env python3
"""
Script pour déclencher le DAG 02 (Scraper) via Airflow
Appelé par le CI GitHub après les tests OK
"""
import subprocess
import sys
import os
from datetime import datetime

DAG_ID = "02_scraper_site_local"

def trigger_via_cli_docker():
    """Déclenche via docker-compose exec"""
    try:
        print("🔄 Déclenchement via docker-compose CLI...")
        
        cmd = [
            "docker-compose",
            "exec",
            "-T",  # Disable pseudo-terminal
            "airflow-webserver",
            "airflow",
            "dags",
            "trigger",
            DAG_ID
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=15, cwd="/Users/cheickna/Documents/DIA2/Anita_data/Projet 23-03/anidata-lab")
        
        if result.returncode == 0:
            print("✅ DAG déclenché via CLI (docker-compose)!")
            print(result.stdout)
            return True
        else:
            print(f"⚠️  CLI erreur: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"⚠️  Erreur CLI: {e}")
        return False

def trigger_via_api():
    """Essaye de déclencher via l'API REST"""
    try:
        import requests
        import json
        
        AIRFLOW_HOST = os.getenv("AIRFLOW_HOST", "http://localhost:8080")
        AIRFLOW_USER = os.getenv("AIRFLOW_USER", "admin")
        AIRFLOW_PASSWORD = os.getenv("AIRFLOW_PASSWORD", "admin")
        
        url = f"{AIRFLOW_HOST}/api/v1/dags/{DAG_ID}/dagRuns"
        payload = {
            "conf": {
                "triggered_by": "github_actions",
                "timestamp": datetime.now().isoformat(),
            }
        }
        
        print(f"📍 Tentative API: {url}")
        response = requests.post(
            url,
            json=payload,
            headers={"Content-Type": "application/json"},
            auth=(AIRFLOW_USER, AIRFLOW_PASSWORD),
            timeout=10,
            verify=False
        )
        
        if response.status_code in [200, 201]:
            print("✅ DAG déclenché via API!")
            return True
        else:
            print(f"⚠️  API réponse {response.status_code}")
            return False
            
    except Exception as e:
        print(f"⚠️  Erreur API: {e}")
        return False

def trigger_dag_scraper():
    """Déclenche le DAG 02"""
    print(f"🔄 Tentative de déclenchement du DAG '{DAG_ID}'...")
    print(f"🔗 SHA Git: {os.getenv('GITHUB_SHA', 'unknown')}")
    print(f"🔗 Ref Git: {os.getenv('GITHUB_REF', 'unknown')}")
    
    # Essayer CLI d'abord (si dans Docker)
    if trigger_via_cli_docker():
        return 0
    
    # Fallback à API
    if trigger_via_api():
        return 0
    
    print("❌ Impossible de déclencher le DAG")
    return 1

if __name__ == "__main__":
    exit_code = trigger_dag_scraper()
    sys.exit(exit_code)
