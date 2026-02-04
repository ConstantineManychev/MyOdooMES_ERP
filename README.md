# MES/ERP Core System for Odoo 17

A custom Odoo 17 build designed for Manufacturing Execution Systems (MES). The core feature is a robust integration with MaintainX and detailed Machine Performance (OEE) tracking.

##  mes_core Module:
* **MaintainX Sync:** Two-way synchronization of Work Orders. It uses the OCA Queue Job module to handle API requests in the background, ensuring the UI remains snappy.
* **Machine Performance:** Tracks production output, downtime (alarms), scrap (rejections), and running logs.
* **Shifts & Staff:** Links employees and shifts to specific work centers (machines).

##    Tech Stack

* **Odoo Version:** 17.0 (Community/Enterprise)
* **Language:** Python 3.10, XML
* **Database:** PostgreSQL 15
* **External Connections:**
    * **SQL:** `pyodbc` + Microsoft ODBC Driver 17 (for Gemba/Legacy DB).
    * **API:** REST API integration with MaintainX.
* **Containerization:** Docker & Docker Compose

---

##    Installation & Setup

### Prerequisites
* Docker Desktop & Git
* Working knowledge of Docker Compose

###  Clone & Build
```bash
git clone [https://github.com/your-repo/MyOdooMES_ERP.git](https://github.com/your-repo/MyOdooMES_ERP.git)
cd MyOdooMES_ERP
docker-compose up -d --build
```
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
