{
    'name': 'MES Core System',
    'version': '2.0',
    'summary': 'Machine Performance, Production Reports, Tasks',
    'author': 'Constantine',
    'category': 'Manufacturing/MES',
    'depends': ['base', 'mrp', 'mail', 'hr'],
    'data': [
        'security/mes_security.xml',
        'security/ir.model.access.csv',

        'data/mes_cron.xml',
        
        'views/mes_dictionaries_views.xml',
        'views/mes_machine_performance_view.xml',
        'views/mes_production_report_view.xml',
        'views/mes_task_view.xml',
        'views/res_config_settings_view.xml',
        
        'views/mes_menus.xml',
    ],
    'installable': True,
    'application': True,
    'license': 'LGPL-3',
}