# MES Core System for Odoo 17

This project is a comprehensive **Manufacturing Execution System (MES)** module for Odoo 17. It bridges the gap between machine automation (Level 2) and ERP (Level 4), providing tools for OEE tracking, manual production reporting, and shop-floor task management.

## 🚀 Key Features

### 1. 🤖 Machine Performance (Automated OEE)
* **Data Source:** Imports production data from **Gemba / VerifySystems** (MS SQL) and **Beckhoff** PLCs.
* **Shift Logic:** Automatically maps data to "Morning", "Afternoon", and "Night" shifts based on timestamps.
* **Downtime & Scrap:** Tracks alarms and rejection reasons mapped to Odoo Availability/Quality Losses.
* **Smart Merging:** Handles open-ended events and fixes timezone discrepancies between PLC and Odoo.

### 2. 📦 Production Reports (Manual Packing)
* **Digital Shift Report:** Replaces paper logs for packing lines.
* **Team Tracking:** Log start/end times for **Packers** (linked to HR Employees).
* **Output Tracking:**
    * **Shippers:** Pallet/Box tracking with barcode integration.
    * **Outers:** Detailed package contents.
* **Quality Control (QC):** Record QC checks and specific **Defects** found during the shift.
* **Ingredients:** Track raw material batch/lot usage per shift.

### 3. ✅ Task Management
* **Shop-Floor Issues:** Create tasks for maintenance or process issues directly linked to a **Machine**.
* **Workflow:** `New` -> `Assigned` -> `Done` -> `Confirmed` (by Author).
* **Notifications:** Integrated with Odoo Chatter for history tracking and team communication.

### 4. ⚙️ Configuration & Dictionaries
* **Work Shifts:** Custom shift schedules (Start time, Duration).
* **Defect Types:** Standardized list of QC defects.
* **Rejection Reasons:** Codes for machine scrap.
* **Machines:** Extended Odoo Workcenters with 'Imatec Code' for external mapping.

---

## 🛠 Tech Stack

* **Odoo Version:** 17.0 (Community/Enterprise)
* **Language:** Python 3.10, XML
* **Database:** PostgreSQL 15
* **External Connection:** `pyodbc` + Microsoft ODBC Driver 17 for SQL Server
* **Containerization:** Docker & Docker Compose

---

## 🏗 Installation & Setup

### Prerequisites
* Docker Desktop & Git
* Working knowledge of Docker Compose

### 1. Clone & Build
```bash
git clone [https://github.com/your-repo/MyOdooMES_ERP.git](https://github.com/your-repo/MyOdooMES_ERP.git)
cd MyOdooMES_ERP
docker-compose up -d --build
```
### 2. Install the Module
Since the module structure has been updated to mes_core, use the following command to install it into a running container:

```bash
docker-compose exec odoo odoo -i mes_core -d Odoo --db_host=db --db_user=odoo --db_password=odoo --stop-after-init
```
(Note: If you are reinstalling after a refactor, you may need to update the module list first: docker-compose exec odoo odoo -u base ...)

## 💻 Configuration Guide
### 1. External Database Connection
Go to MES System -> Configuration -> Settings:

Enter the Host, Database, User, and Password for the legacy SQL Server (Gemba).

### 2. Master Data Setup
Before importing data, ensure you have configured:

Machines: Set the Imatec Code (e.g., IMA3) on relevant Workcenters.

Shifts: Define your standard shifts (Morning/Night).

## 📂 Project Structure
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

## 📜 License
LGPL-3
