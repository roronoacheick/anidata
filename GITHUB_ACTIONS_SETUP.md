# 🚀 Configuration de l'Automatisation GitHub Actions

## Objectif
Après un push sur GitHub, le pipeline se déclenche automatiquement :
```
GitHub Push → GitHub Actions → Déclenche DAG 02 → Déclenche DAG 03 automatiquement
```

---

## 📋 Étapes de Configuration

### 1️⃣ Configurer les Secrets GitHub

Tu dois ajouter **3 secrets** au repository GitHub :

#### **À faire sur GitHub.com :**
1. Va sur le repository GitHub
2. **Settings** → **Secrets and variables** → **Actions**
3. Clique sur **"New repository secret"**

Ajoute ces 3 secrets :

| Secret Name | Valeur | Exemple |
|---|---|---|
| `AIRFLOW_HOST` | URL de ton Airflow | `http://localhost:8080` |
| `AIRFLOW_USER` | Username Airflow | `admin` |
| `AIRFLOW_PASSWORD` | Password Airflow | `admin` |

---

### 2️⃣ Exemple Concret

**Ajout du Secret AIRFLOW_HOST :**
```
Name:  AIRFLOW_HOST
Value: http://localhost:8080
```

**Ajout du Secret AIRFLOW_USER :**
```
Name:  AIRFLOW_USER
Value: admin
```

**Ajout du Secret AIRFLOW_PASSWORD :**
```
Name:  AIRFLOW_PASSWORD
Value: admin
```

---

### 3️⃣ Vérifier la Configuration

Après avoir ajouté les secrets, tu dois voir :
```
✅ AIRFLOW_HOST
✅ AIRFLOW_USER
✅ AIRFLOW_PASSWORD
```

---

## 🔄 Flux d'Exécution Complet

### **Sur un Push (ex: `git push origin Test`)**

```yaml
┌─────────────────────────────────────────────────────────────┐
│             GitHub Actions Déclenché                        │
│          (event: push sur Test, cheickna, etc.)             │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
            ┌────────────────┐
            │  JOB 1: LINT   │
            │  ✅ ruff check │
            └────────┬───────┘
                     │
                     ▼
            ┌────────────────────┐
            │  JOB 2: TESTS      │
            │  ✅ pytest runner  │
            └────────┬───────────┘
                     │
                     ▼
            ┌──────────────────────┐
            │  JOB 3: BUILD DOCKER │
            │  ✅ Build & Push     │
            └────────┬─────────────┘
                     │
                     ▼
            ┌─────────────────────────────────┐
            │  JOB 4: TRIGGER DAG             │
            │  ✅ Appelle API Airflow         │
            │  → POST /api/v1/dags/.../dagRuns│
            └────────┬────────────────────────┘
                     │
                     ▼
        ┌────────────────────────────────┐
        │  DAG 02 S'EXÉCUTE (Scraping)   │
        │  1. start_scraping()           │
        │  2. scrape_site()              │
        │  3. verify_scraped_data()      │
        └────────┬─────────────────────┘
                 │ (DAG 02 terminé ✅)
                 ▼
        ┌────────────────────────────────┐
        │  DAG 03 S'EXÉCUTE AUTO          │
        │  (ExternalTaskSensor trigger)  │
        │  1. wait_for_scraper()         │
        │  2. process_scraped_json()     │
        │  3. clean_data()               │
        │  4. summary()                  │
        └────────────────────────────────┘
```

---

## 🧪 Tester la Configuration

### **Test 1 : Vérifier les Secrets**
```bash
# Sur GitHub, voir les secrets
GitHub Repo → Settings → Secrets → Actions
# Doit afficher : ✅ AIRFLOW_HOST, ✅ AIRFLOW_USER, ✅ AIRFLOW_PASSWORD
```

### **Test 2 : Faire un Push**
```bash
git add .
git commit -m "Test CI/CD automation"
git push origin Test  # ou ta branche
```

### **Test 3 : Regarder l'Exécution**
1. Va sur **GitHub** → **Actions**
2. Tu verras le workflow "CI/CD" en cours d'exécution
3. Clique dessus pour voir les étapes

### **Test 4 : Vérifier Airflow**
1. Ouvre http://localhost:8080
2. Cherche **DAG 02 (02_scraper_site_local)**
3. Tu devrais voir une nouvelle exécution "DAG Run" qui vient d'être créée

---

## ❌ Dépannage

### **Problème : "Impossible de se connecter à Airflow"**

**Cause possible :**
- Airflow n'est pas accessible depuis GitHub Actions
- Les variables d'environnement ne sont pas configurées
- L'URL Airflow est incorrecte

**Solution :**
1. Vérifie que Airflow s'exécute : `docker-compose ps`
2. Teste l'URL : `curl http://localhost:8080/health`
3. Revérifie les secrets GitHub

### **Problème : "Authentication failed (401)"**

**Cause :**
- AIRFLOW_USER ou AIRFLOW_PASSWORD incorrect

**Solution :**
1. Vérifie les secrets GitHub
2. Assure-toi que l'utilisateur existe dans Airflow
3. Réinitialise le password si nécessaire

### **Problème : "DAG not found"**

**Cause :**
- DAG 02 n'existe pas dans Airflow

**Solution :**
1. Vérifie que le fichier `02_scraper_site_local.py` existe
2. Redémarre Airflow : `docker-compose restart airflow-scheduler`

---

## 📝 Variables d'Environnement Utilisées

Ces variables sont **automatiquement injectées** dans GitHub Actions :

```yaml
env:
  AIRFLOW_HOST: ${{ secrets.AIRFLOW_HOST }}        # De ta config
  AIRFLOW_USER: ${{ secrets.AIRFLOW_USER }}        # De ta config
  AIRFLOW_PASSWORD: ${{ secrets.AIRFLOW_PASSWORD }} # De ta config
  GITHUB_SHA: ${{ github.sha }}                     # SHA du commit
  GITHUB_REF: ${{ github.ref }}                     # Branche (ex: refs/heads/Test)
```

---

## ✅ Checklist Finale

- [ ] Tu as créé le secret `AIRFLOW_HOST`
- [ ] Tu as créé le secret `AIRFLOW_USER`
- [ ] Tu as créé le secret `AIRFLOW_PASSWORD`
- [ ] DAG 02 existe dans `airflow/dags/02_scraper_site_local.py`
- [ ] DAG 03 existe dans `airflow/dags/03_nettoyage_dag.py`
- [ ] Airflow s'exécute (docker-compose up -d)
- [ ] Le fichier `.github/workflows/ci-cd.yml` contient le job `trigger-dag`

---

## 🎯 Après Configuration

Dès que tu configures tout, le flux automatique fonctionne ainsi :

```bash
# Tu fais un push
git push origin Test

# GitHub Actions se déclenche automatiquement
# → Lint + Tests + Docker Build + DAG 02 Trigger

# DAG 02 scrape les données
# DAG 03 nettoie les données (automatique)

# Tu regardes les résultats dans Airflow UI
# http://localhost:8080
```

---

**Questions ? 🚀**

Dis-moi si tu rencontres des problèmes avec la configuration !
