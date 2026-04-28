#!/usr/bin/env python3


from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

import requests


# --- Chargement du .env (sans dépendre de python-dotenv) ----------------------
def load_env_file(path: Path) -> None:
    """Charge un fichier .env basique dans os.environ s'il existe."""
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


load_env_file(Path(__file__).parent / ".env")

# --- Configuration ------------------------------------------------------------
OWNER = os.getenv("GITHUB_OWNER", "roronoacheick")
REPO = os.getenv("GITHUB_REPO", "anidata")
TOKEN = os.getenv("GITHUB_TOKEN")
BRANCH = os.getenv("GITHUB_BRANCH", "main")
WORKFLOW = os.getenv("GITHUB_WORKFLOW_FILE", "ci-cd.yml")  # ou "ci.yml"
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "30"))

if not TOKEN:
    print("❌ GITHUB_TOKEN absent. Mets-le dans .env ou exporte-le.")
    sys.exit(2)

# On filtre par branche ET par workflow pour ne pas attraper un run d'une
# autre branche ou d'un workflow différent.
URL = (
    f"https://api.github.com/repos/{OWNER}/{REPO}"
    f"/actions/workflows/{WORKFLOW}/runs?branch={BRANCH}&per_page=1"
)

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28",
}


def get_latest_run() -> tuple[str | None, str | None, int | None]:
    """Renvoie (status, conclusion, run_id) du run le plus récent."""
    r = requests.get(URL, headers=HEADERS, timeout=10)
    r.raise_for_status()
    data = r.json()
    runs = data.get("workflow_runs") or []
    if not runs:
        return None, None, None
    run = runs[0]
    return run.get("status"), run.get("conclusion"), run.get("id")


def trigger_airflow_dag() -> int:
    """Lance trigger_dag_scraper.py qui fait le POST sur l'API Airflow."""
    script = Path(__file__).parent / "trigger_dag_scraper.py"
    print(f"🚀 Lancement de {script.name}…")
    return subprocess.run([sys.executable, str(script)], check=False).returncode


def main() -> int:
    print(f"👀 Monitoring {OWNER}/{REPO} — workflow={WORKFLOW} branch={BRANCH}")
    last_triggered_run_id: int | None = None

    while True:
        try:
            status, conclusion, run_id = get_latest_run()
        except requests.RequestException as exc:
            print(f"⚠️  Erreur API GitHub : {exc}")
            time.sleep(POLL_INTERVAL)
            continue

        if run_id is None:
            print("ℹ️  Aucun run trouvé sur cette branche. Patiente…")
            time.sleep(POLL_INTERVAL)
            continue

        print(f"#{run_id} → status={status} | conclusion={conclusion}")

        if status == "completed":
            if conclusion == "success" and run_id != last_triggered_run_id:
                print("✅ CI verte → déclenchement DAG 3 sur Airflow")
                rc = trigger_airflow_dag()
                last_triggered_run_id = run_id
                if rc != 0:
                    print(f"⚠️  trigger_dag_scraper.py exit code = {rc}")
            elif conclusion != "success":
                print(f"❌ CI {conclusion} — pas de déclenchement Airflow")

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    sys.exit(main() or 0)
