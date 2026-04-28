import requests
import time
import subprocess

OWNER = "roronoacheick"
REPO = "anidata"
TOKEN = "TON_TOKEN"

URL = f"https://api.github.com/repos/{OWNER}/{REPO}/actions/runs"

headers = {
    "Authorization": f"Bearer {TOKEN}"
}

def get_status():
    r = requests.get(URL, headers=headers)
    data = r.json()
    run = data["workflow_runs"][0]
    return run["status"], run["conclusion"]

while True:
    status, conclusion = get_status()
    print("Status:", status, "| Conclusion:", conclusion)

    if status == "completed" and conclusion == "success":
        print("✅ CI OK → déclenchement Airflow")

        # appelle le script 
        subprocess.run([
            "docker", "exec", "airflow-webserver",
            "airflow", "dags", "trigger", "dag3_scraper"
        ])
        break

    else:
        print("⏳ En attente...")
        time.sleep(30)

def main():
    pass