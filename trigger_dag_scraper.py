#!/usr/bin/env python3
"""
Script pour déclencher le DAG 3 (Scraper) via l'API Airflow
Appelé par le CI GitHub après les tests OK
"""
#tessss
import requests
import json
import sys
import os
from datetime import datetime

# Configuration depuis variables d'environnement (GitHub Actions)
AIRFLOW_HOST = os.getenv("AIRFLOW_HOST", "http://localhost:8080")
AIRFLOW_USER = os.getenv("AIRFLOW_USER", "admin")
AIRFLOW_PASSWORD = os.getenv("AIRFLOW_PASSWORD", "admin")
DAG_ID = "02_scraper_site_local"

def trigger_dag_scraper():
    """Déclenche le DAG 2 via l'API Airflow"""
    
    # URL de l'API Airflow pour créer un DAG run
    url = f"{AIRFLOW_HOST}/api/v1/dags/{DAG_ID}/dagRuns"
    
    # Payload pour le DAG run
    payload = {
        "conf": {
            "triggered_by": "github_actions",
            "timestamp": datetime.now().isoformat(),
            "git_sha": os.getenv("GITHUB_SHA", "unknown"),
            "git_ref": os.getenv("GITHUB_REF", "unknown"),
        }
    }
    
    # Headers pour l'authentification
    headers = {
        "Content-Type": "application/json"
    }
    
    print(f"🔄 Tentative de déclenchement du DAG '{DAG_ID}'...")
    print(f"📍 URL: {url}")
    print(f"🔗 SHA Git: {os.getenv('GITHUB_SHA', 'unknown')}")
    print(f"🔗 Ref Git: {os.getenv('GITHUB_REF', 'unknown')}")
    
    try:
        # Essayer SANS authentification d'abord
        response = requests.post(
            url,
            json=payload,
            headers=headers,
            timeout=10,
            verify=False
        )
        
        # Check de la réponse
        if response.status_code in [200, 201]:
            print("✅ DAG déclenché avec succès!")
            print(f"Response: {json.dumps(response.json(), indent=2)}")
            return 0
        elif response.status_code == 401:
            # Si erreur 401, essayer avec auth
            print("⚠️  Authentification requise, tentative avec credentials...")
            response = requests.post(
                url,
                json=payload,
                headers=headers,
                auth=(AIRFLOW_USER, AIRFLOW_PASSWORD),
                timeout=10,
                verify=False
            )
            
            if response.status_code in [200, 201]:
                print("✅ DAG déclenché avec succès!")
                print(f"Response: {json.dumps(response.json(), indent=2)}")
                return 0
            else:
                print(f"❌ Erreur {response.status_code}")
                print(f"Response: {response.text}")
                return 1
        else:
            print(f"❌ Erreur {response.status_code}")
            print(f"Response: {response.text}")
            return 1
            
    except requests.exceptions.ConnectionError:
        print(f"❌ Impossible de se connecter à Airflow ({AIRFLOW_HOST})")
        print("   Lanche: docker-compose up -d")
        return 1
    except Exception as e:
        print(f"❌ Erreur: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit_code = trigger_dag_scraper()
    sys.exit(exit_code)
#teste
