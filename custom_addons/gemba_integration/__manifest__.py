{
    'name': 'Gemba OEE Integration',
    'version': '1.0',
    'summary': 'Connector for Gemba OEE and VerifySystems (MES Level 3)',
    'author': 'Constantine',
    'category': 'Manufacturing/IoT',
    
    # inheritance
    'depends': ['base', 'mrp'], 
    
    'data': [
        # PErmissions
        'security/ir.model.access.csv',
        
        # Views
        'views/mes_views.xml',
        'views/res_config_settings_view.xml',
    ],
    
    'installable': True,
    'application': True,
    'license': 'LGPL-3',
}