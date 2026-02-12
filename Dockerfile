FROM odoo:17.0

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    gnupg2 \
    git \
    ca-certificates \
    lsb-release \
    && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && curl https://packages.microsoft.com/config/debian/12/prod.list | tee /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y --no-install-recommends \
    msodbcsql17 \
    unixodbc-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN pip3 install --no-cache-dir pyodbc debugpy pandas

RUN mkdir -p /mnt/oca-addons \
    && git clone -b 17.0 --single-branch --depth 1 https://github.com/OCA/queue.git /mnt/oca-addons/queue \
    && chown -R odoo:odoo /mnt/oca-addons

USER odoo