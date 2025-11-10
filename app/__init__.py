import os
import psycopg2
from flask import Flask


def create_app(test_config=None):
    # create and configure the app
    app = Flask(__name__, instance_relative_config=True)

    # --- Safer Secret Key Logic ---
    SECRET_KEY = os.getenv("SECRET_KEY")
    if not SECRET_KEY:
        print(
            "WARNING: SECRET_KEY environment variable not set. Using a temporary key."
        )
        SECRET_KEY = os.urandom(24)

    app.config.from_mapping(
        SECRET_KEY=SECRET_KEY,
    )

    if test_config is None:
        # load the instance config, if it exists, when not testing
        app.config.from_pyfile("config.py", silent=True)
    else:
        # load the test config if passed in
        app.config.from_mapping(test_config)

    # --- Register Database Functions ---
    from . import db

    db.init_app(app)

    # --- Register Blueprints ---
    from . import routes_core

    app.register_blueprint(routes_core.bp)

    from . import routes_data

    app.register_blueprint(routes_data.bp)

    from . import routes_ops

    app.register_blueprint(routes_ops.bp)

    # Make the home route ('/') available
    app.add_url_rule("/", endpoint="index")

    return app
