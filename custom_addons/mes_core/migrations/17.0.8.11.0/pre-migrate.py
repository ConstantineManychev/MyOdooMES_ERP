def migrate(cr, version):
    cr.execute("DROP VIEW IF EXISTS mes_anomaly_report CASCADE")
