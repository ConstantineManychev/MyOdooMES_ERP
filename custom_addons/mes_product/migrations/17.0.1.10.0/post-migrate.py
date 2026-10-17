def migrate(cr, version):
    cr.execute("""
        UPDATE mes_product_override
        SET changed_at = COALESCE(write_date, create_date),
            origin = COALESCE(origin, 'backend')
        WHERE changed_at IS NULL OR origin IS NULL
    """)
