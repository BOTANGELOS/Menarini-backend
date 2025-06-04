from flask import Flask
from flask_jwt_extended import JWTManager
from .routes import main_bp
from flask_mail import Mail
from . import  auth
from . import  dn_handler
from . import  ax09   # Import auth

mail = Mail()
def create_app(config_class='config.Config'):
    app = Flask(__name__, instance_relative_config=True)
    app.config.from_object(config_class)

    # Register blueprints
    mail.init_app(app)
    app.register_blueprint(main_bp)
    app.register_blueprint(auth.bp, url_prefix='/auth')
    app.register_blueprint(dn_handler.dn_bp, url_prefix='/dn')
    app.register_blueprint(ax09.ax_bp, url_prefix='/ax09')
    jwt = JWTManager(app)
    return app
