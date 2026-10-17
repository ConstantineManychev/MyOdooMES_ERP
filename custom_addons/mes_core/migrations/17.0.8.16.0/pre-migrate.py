def migrate(cr, version):
    cr.execute("SELECT to_regclass('mes_link_outage')")
    if cr.fetchone()[0]:
        cr.execute("DELETE FROM mes_link_outage")
    cr.execute("DELETE FROM ir_config_parameter WHERE key = 'mes_core.link_check_last_run'")
