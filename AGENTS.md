# Repository Guidelines

## Project Structure & Module Organization

Source lives in `app/`. `__init__.py` creates the Flask app, `db.py` centralizes the PostgreSQL connection, and `models.py` hosts reusable query helpers such as `get_base_ingredients`. HTTP behavior is split across `routes_core.py`, `routes_data.py`, and `routes_ops.py`. Static assets belong in `app/static/` (`style.css`, `app.js`, images), and all HTML templates reside in `app/templates/` with names that mirror their feature (for example `inventory_items.html`, `requirements.html`). The top-level `run.py` is the dev entry point and `.env` supplies secrets.

## Build, Test, and Development Commands

Create an isolated interpreter before making changes:

```bash
python3 -m venv venv && source venv/bin/activate
pip install --upgrade pip flask psycopg2-binary python-dotenv
```

Run the application locally with live reload via `python run.py` (Flask uses the factory in `app/__init__.py`). To troubleshoot database issues, export `DATABASE_URL` and run `FLASK_ENV=development python run.py` so the verbose logging in `app/db.py` appears. Use `FLASK_APP=run.py flask shell` for ad-hoc queries.

## Coding Style & Naming Conventions

Follow the existing 4-space indentation, snake_case identifiers for Python, and prefer small helper functions similar to `_log_inventory_adjustment`. Stick with f-strings for interpolation and keep SQL uppercase for statements while columns stay lowercase. Organize new Jinja templates beside related routes and align CSS/JS names with their template (e.g., `requirements` view manipulates `requirements.html`).

## Testing Guidelines

There is no automated suite yet, so accompany changes with targeted sanity checks: seed the database, run `python run.py`, and exercise affected routes in the browser. When adding tests, place them under a new `app/tests/` package, name files `test_<feature>.py`, and use `pytest -q` so we can later wire CI to the same command.

## Commit & Pull Request Guidelines

Recent commits use concise, imperative summaries (`Added terminal logging for database connection`, `General bug fixes and error handling`). Mirror that style, limit subjects to ~60 characters, and expand on rationale in the body when touching SQL or caching logic. Every PR should describe the user impact, list schema or env changes (e.g., new `DATABASE_URL` requirements), reference related issues, and include screenshots/GIFs for template changes so reviewers can verify UI regressions quickly.

## Security & Configuration Tips

Keep `.env` private; populate `SECRET_KEY` and `DATABASE_URL` before running anything that hits the DB. Never print secrets—`app/__init__.py` already warns when `SECRET_KEY` is missing. Use least-privilege Postgres roles and rotate credentials after debugging in shared environments.
