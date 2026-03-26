
---

# 1. Install Miniconda (Arch Linux)

Download Miniconda:

```bash
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
```

Install:

```bash
bash Miniconda3-latest-Linux-x86_64.sh
```

Restart terminal, then check:

```bash
conda --version
```

---

# 2. Create Conda Environment for Project

Your project requires **Python 3.10+** (from your requirements file).


Create environment:

```bash
conda create -n nea-pipeline python=3.10
```

Activate:

```bash
conda activate nea-pipeline
```

---

# 3. Install All Python Packages

Inside your project folder:

```bash
pip install -r requirements.txt
```

Your requirements already include:

* pandas
* numpy
* sqlalchemy
* psycopg2
* airflow
* matplotlib
* reportlab
* jinja2
* prometheus
* etc.


So this single command installs everything.

---

# 4. Install PostgreSQL in Arch Linux

Install:

```bash
sudo pacman -S postgresql
```

Initialize database:

```bash
sudo -iu postgres
initdb -D /var/lib/postgres/data
exit
```

Start PostgreSQL:

```bash
sudo systemctl start postgresql
sudo systemctl enable postgresql
```

Open PostgreSQL:

```bash
sudo -u postgres psql
```

Create database:

```sql
CREATE DATABASE nea_electricity_db;
CREATE USER nea_admin WITH PASSWORD 'password';
ALTER ROLE nea_admin SET client_encoding TO 'utf8';
ALTER ROLE nea_admin SET default_transaction_isolation TO 'read committed';
ALTER ROLE nea_admin SET timezone TO 'Asia/Kathmandu';
GRANT ALL PRIVILEGES ON DATABASE nea_electricity_db TO nea_admin;
\q
```

This matches your config file:


---

# 5. Create Database Tables

Inside project folder:

```bash
psql -U nea_admin -d nea_electricity_db -f database/schema.sql
```

---

# 6. Setup Apache Airflow (Scheduler)

Activate environment first:

```bash
conda activate nea-pipeline
```

Initialize Airflow:

```bash
export AIRFLOW_HOME=~/airflow
airflow db init
```

Create user:

```bash
airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password admin
```

Copy DAG:

```bash
cp scheduler/airflow_dag.py ~/airflow/dags/
```

Start Airflow:

```bash
terminal 1:
airflow webserver --port 8080
if it fails: airflow webserver --port 8080 --workers 1
terminal 2:
airflow scheduler
```

Open browser:

```
http://localhost:8080
```

---

# 7. Setup Logging Folder

Create directories:

```bash
mkdir logs
mkdir reports/output
mkdir dashboard
```

Your config already defines logging:


---

# 8. Report Generation (PDF / Excel / HTML)

Your project supports:

* PDF → reportlab
* Excel → openpyxl / xlsxwriter
* HTML → jinja2


Example report generation script:

## reports/monthly_report.py

```python
import pandas as pd
from reportlab.platypus import SimpleDocTemplate, Paragraph
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet

def generate_report():
    df = pd.read_sql("SELECT * FROM monthly_consumption", con)

    doc = SimpleDocTemplate("reports/output/monthly_report.pdf", pagesize=A4)
    styles = getSampleStyleSheet()

    content = []
    content.append(Paragraph("Monthly Electricity Report", styles['Title']))
    content.append(Paragraph(f"Total Records: {len(df)}", styles['Normal']))

    doc.build(content)

if __name__ == "__main__":
    generate_report()
```

---

# 9. VS Code Setup

Install extensions:

* Python
* Pylance
* PostgreSQL
* Jupyter
* YAML
* Docker (optional)

Select interpreter:

```
Ctrl + Shift + P
Python: Select Interpreter
Choose → conda env → nea-pipeline
```

---

# 10. How To Run Full Project

Every time you start:

```bash
conda activate nea-pipeline
cd electricity-analytics-pipeline
python run_pipeline.py
```

Generate reports:

```bash
python run_pipeline.py --report monthly
```

Run analytics:

```bash
python run_pipeline.py --analytics all
```

---

# 11. Final System Architecture

project pipeline architecture:

```
Data Sources
    ↓
Extract (Python)
    ↓
Transform (Pandas)
    ↓
Load (PostgreSQL)
    ↓
Data Warehouse
    ↓
Analytics Scripts
    ↓
Reports (PDF / Excel / Dashboard)
    ↓
Scheduler (Airflow)
    ↓
Monitoring & Logging
```

---

# 12. Full Tech Stack Used


| Layer          | Technology          |
| -------------- | ------------------- |
| Programming    | Python              |
| ETL            | Pandas              |
| Database       | PostgreSQL          |
| Data Warehouse | PostgreSQL          |
| Scheduler      | Apache Airflow      |
| Visualization  | Plotly / Matplotlib |
| Reports        | ReportLab / Excel   |
| Monitoring     | Prometheus          |
| Logging        | Loguru              |
| Environment    | Conda               |
| OS             | Arch Linux          |
| IDE            | VS Code             |