from flask import Flask
from flask_cors import CORS


def create_app():
    app = Flask(__name__)
    app.config.from_mapping(
        SECRET_KEY="your-secret-key",
    )
    CORS(app)
    # Register blueprints or routes
    from .api import main

    app.register_blueprint(main)

    return app
