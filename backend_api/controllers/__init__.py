# backend_api/controllers/__init__.py
# File này dùng để import và đăng ký các Blueprints

def register_blueprints(app):
    """Đăng ký tất cả các blueprints với Flask app."""
    
    from .main_controller import main_bp
    from .forecast_controller import forecast_bp
    from .storm_controller import storm_bp
    
    app.register_blueprint(main_bp)
    app.register_blueprint(forecast_bp)
    app.register_blueprint(storm_bp)
