# Gemba OEE Integration for Odoo 17

This project is a custom Odoo ERP module designed to integrate Odoo with **Gemba OEE / VerifySystems** (MES Level 3). It automates the extraction of production data from an external MS SQL Server and synchronizes it with Odoo Workcenters and Productivity Losses.

## 🏗 Architecture & Features

The module follows a **"Fat Model, Thin Controller"** architecture to ensure clean code, transactional safety, and ease of maintenance.

* **MS SQL Integration:** Uses `pyodbc` and the official Microsoft ODBC Driver 17 to connect to legacy MES databases.
* **Automatic Data Mapping:**
    * **Machines:** Automatically maps external Asset IDs to Odoo Workcenters (or creates them if missing).
    * **Shifts:** Recognizes Morning, Afternoon, and Night shifts.
    * **Alarms (Downtime):** Imports downtime events, mapping PLC codes to Odoo Availability Losses.
* **Smart Logic:**
    * Handles open-ended events and clamps them to shift boundaries.
    * Fixes timezone discrepancies between the SQL Server (Local) and Odoo (UTC).
    * Prevents duplicate imports using strict SQL constraints.
* **Secure Configuration:** Connection credentials and API tokens are stored in Odoo System Parameters via a custom Settings UI, not hardcoded.

## 🛠 Tech Stack

* **Odoo Version:** 17.0 (Community/Enterprise)
* **Database:** PostgreSQL 15
* **External Driver:** Microsoft ODBC Driver 17 for SQL Server
* **Containerization:** Docker & Docker Compose
* **Environment:** Ready for GitHub Codespaces

---

## 🚀 Installation & Setup

### Prerequisites

* Docker Desktop (with WSL 2 on Windows)
* Git

### 1. Clone the Repository

```bash
git clone https://github.com/ConstantineManychev/MyOdooMES_ERP.git
cd MyOdooMES_ERP
```
### 2. Build and Start Containers
This process builds the custom Odoo image (installing pyodbc and MS SQL drivers) and starts the database.

```bash
docker-compose up -d --build
```
### 3. Install the Module
Once the container is running, execute the following command to install the integration module and its dependencies:

```bash
docker-compose exec odoo odoo -i mes_core -d Odoo --db_host=db --db_user=odoo --db_password=odoo --stop-after-init
```
(Note: Replace Odoo with your actual database name if it differs).

## 💻 Usage
### 1. Configuration
Log in to Odoo as Administrator.

Go to Settings -> Gemba Integration.

Enter your MS SQL Server credentials:

Host: e.g., 192.168.1.10 or AB-AS03

Database: e.g., Connect

User/Password: Read-only SQL user credentials.

(Optional) Enter the MaintainX API Token.

### 2. Import Data
Navigate to the MES Data menu.

Click Import From Gemba.

Select the Start Date and End Date.

Click Load Data.

The system will fetch data, process time intervals, and generate Production Reports containing all alarms and shift details.

## ☁️ GitHub Codespaces
This repository is configured for Cloud Native Development.

Click Code -> Codespaces -> Create codespace on main.

The environment will automatically install Python extensions, configure Docker, and forward port 8069.

Wait for the container to build, then open the "Ports" tab to access Odoo.

## 📂 Project Structure
```bash
├── config/                 # Odoo server configuration
├── custom_addons/          # Custom modules
│   └── mes_core/  # Main integration module
│       ├── models/         # Business logic (Fat Models)
│       ├── wizard/         # Data fetchers (Thin Controllers)
│       └── views/          # UI definitions (XML)
├── docker-compose.yml      # Container orchestration
└── Dockerfile              # Custom image build steps
```
## 📜 License
LGPL-3
