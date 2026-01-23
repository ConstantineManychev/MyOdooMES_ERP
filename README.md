# MES Core System for Odoo 17

This project is a comprehensive **Manufacturing Execution System (MES)** module for Odoo 17. It bridges the gap between machine automation (Level 2) and ERP (Level 4), providing tools for OEE tracking, manual production reporting, shop-floor task management, and external integrations.

##      Key Features

###   Machine Performance (Automated OEE)
* **Data Source:** Imports production data from **Gemba / VerifySystems** (MS SQL) and **Beckhoff** PLCs.
* **Shift Logic:** Automatically maps data to "Morning", "Afternoon", and "Night" shifts based on timestamps.
* **Downtime & Scrap:** Tracks alarms and rejection reasons mapped to Odoo Availability/Quality Losses.
* **Smart Merging:** Handles open-ended events and fixes timezone discrepancies between PLC and Odoo.

###   Production Reports (Manual Packing)
* **Digital Shift Report:** Replaces paper logs for packing lines.
* **Team Tracking:** Log start/end times for **Packers** (linked to HR Employees).
* **Output Tracking:**
    * **Shippers:** Pallet/Box tracking with barcode integration.
    * **Outers:** Detailed package contents.
* **Quality Control (QC):** Record QC checks and specific **Defects** found during the shift.
* **Ingredients:** Track raw material batch/lot usage per shift.

###   Task Management & MaintainX Sync
* **Shop-Floor Issues:** Create tasks for maintenance or process issues directly linked to a **Machine**.
* **Two-Way Sync Logic (MaintainX):**
    * **Import:** Automatically pulls "Open" Work Orders from MaintainX.
    * **Updates:** Syncs status changes (Open -> In Progress -> Done) and Assignees history.
    * **Mapping:** Intelligent mapping of Priorities (High/Medium/Low) and Assets (Machines).
* **History Tracking:** detailed log of status changes and assignee updates.
* **Workflow:** `Open` -> `On Hold` -> `In Progress` -> `Done` -> `Cancelled`.
* **Visuals:** Kanban view with priority color coding and MaintainX indicators.

###   Configuration & Master Data
* **Work Shifts:** Custom shift schedules (Start time, Duration).
* **Machine Hierarchy:**
    * **Machines:** Extended Workcenters with 'Imatec Code' and 'MaintainX ID'.
    * **Streams & Wheels:** Configure sub-components for specific production lines.
* **Dictionaries:** Standardized lists for **Defect Types**, **Rejection Reasons**, and **Alarms**.

---

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

##    Configuration Guide
###  External Database Connection
Go to MES System -> Configuration -> Settings:

Gemba Integration: Enter SQL Host, Database, User, and Password.

MaintainX Integration: Enter your private API Token.

###  Master Data Setup
Before importing data, ensure you have configured:

Machines: Set the Imatec Code (e.g., IMA3) on relevant Workcenters.

Shifts: Define your standard shifts (Morning/Night).

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
