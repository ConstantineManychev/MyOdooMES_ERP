{
    'name': 'MES Core System',
    'version': '2.0',
    'summary': 'Machine Performance, Production Reports, Tasks',
    'author': 'Constantine',
    'category': 'Manufacturing/MES',
    'depends': ['base', 'mrp', 'mail', 'hr', 'queue_job'],
    'data': [
        'security/mes_security.xml',
        'security/ir.model.access.csv',

        'data/mes_cron.xml',
        
        'views/mes_menus.xml', 
        
        'views/mes_dictionaries_views.xml',
        'views/mes_machine_performance_view.xml',
        'views/mes_production_report_view.xml',
        'views/mes_task_view.xml',
        'views/res_config_settings_view.xml',
        'views/hr_employee_view.xml',
        
        'views/mes_telemetry_views.xml',
        'wizard/events_import_wizard.xml',
        'wizard/raw_data_csv_import_wizard.xml',
    ],
    'installable': True,
    'application': True,
    'license': 'LGPL-3',
}