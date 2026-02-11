# MyOdoo MES / ERP Core

A Manufacturing Execution System (MES) built on Odoo 17, designed to aggregate machine telemetry, generate PQO (Product Quality Output) reports, and automate maintenance workflows via MaintainX.

##  Core Objectives:
* **Data Consolidation:** Merging high-frequency machine telemetry with manual operator packing reports into a single Odoo environment.
* **Operator Workspace:** Optimized interface for production task management and real-time equipment monitoring.
* **Hybrid Storage Architecture:** Separation of ERP transactional data (PostgreSQL) and telemetry time-series data (TimescaleDB) to ensure system stability under high load.

##  Technical Stack & Architecture

### 1. Telemetry & TimescaleDB
Machine data collection is handled via IPC + pyads, writing directly to TimescaleDB.
* **PostgreSQL FDW:** Odoo accesses telemetry data using Foreign Data Wrappers, allowing it to treat telemetry as standard Odoo tables without taxing the primary ERP database.
* **Language:** Python 3.10, XML
* **CSV Import:** Integrated wizards support manual raw data uploads to TimescaleDB to maintain historical integrity when automated streams are interrupted.

### 2. Legacy Systems Integration (Gemba OEE)
During the migration phase, the system maintains a direct connection to external MS SQL (Impact Connect) databases using pyodbc. This ensures seamless synchronization of shifts and alarm events from the legacy environment.

### 3. MaintainX Automation
Bi-directional Work Order synchronization via REST API.

* **OCA Queue Job:** All API interactions are handled asynchronously to prevent UI lag and ensure reliable data delivery regardless of order volume.
* **Traffic Prioritization:** Dedicated job channels manage MaintainX traffic to prevent bottlenecks.

---

##    Installation & Setup

### Prerequisites
* Docker Desktop & Git
* Working knowledge of Docker Compose

###  Clone & Build
```bash
git clone --recursive https://github.com/your-repo/MyOdooMES_ERP.git
cd MyOdooMES_ERP
docker-compose up -d --build
```

* **Environment Setup:** Create a .env file from the .env temp template and configure your database credentials and API tokens.

###  Install the Module
Since the module structure has been updated to mes_core, use the following command to install it into a running container:

```bash
docker-compose exec odoo odoo -i mes_core -d Odoo --db_host=db --db_user=odoo --db_password=odoo --stop-after-init
```
(Note: If you are reinstalling after a refactor, you may need to update the module list first: docker-compose exec odoo odoo -u base ...)


##    Project Structure
Plaintext

```text
custom_addons/
└── mes_core/               # Main Module
    ├── models/
    │   ├── mes_machine_performance.py  # OEE Logic
    │   ├── mes_production_report.py    # Packing/QC Logic
    │   ├── mes_tasks.py                # Task Logic
    │   └── mes_dictionaries.py         # Shifts, Defects, etc.
    ├── views/
    │   ├── mes_menus.xml               # Menu Hierarchy
    │   └── ...                         # UI Definitions
    ├── wizard/
    │   └── external_import_wizard.py   # SQL Import Logic
    └── security/                       # Access Rights (User/Manager)
```

##    License
LGPL-3
