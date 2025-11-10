import psycopg2
from psycopg2.extras import DictCursor
from flask import (
    Blueprint,
    flash,
    g,
    redirect,
    render_template,
    request,
    url_for,
    session,
)
from collections import defaultdict
import math
from datetime import datetime

from app.db import get_db
from app.models import get_base_ingredients

bp = Blueprint("core", __name__)


@bp.route("/")
def home():
    conn = get_db()
    dashboard_data = {"wip_batches_count": 0, "open_pos_count": 0, "low_stock_count": 0}
    try:
        all_base_ingredients = []
        inventory_levels = {}
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute(
                "SELECT COUNT(id) as count FROM wip_batches WHERE status = 'In Progress';"
            )
            dashboard_data["wip_batches_count"] = cur.fetchone()["count"]
            cur.execute(
                "SELECT COUNT(id) as count FROM purchase_orders WHERE status = 'Placed' OR status = 'Shipped';"
            )
            dashboard_data["open_pos_count"] = cur.fetchone()["count"]
            cur.execute(
                "SELECT id, name, unit, quantity_on_hand, quantity_allocated FROM inventory_items;"
            )
            for item in cur.fetchall():
                inventory_levels[item["id"]] = {
                    "available": float(item.get("quantity_on_hand", 0))
                    - float(item.get("quantity_allocated", 0))
                }
            cur.execute(
                """
                SELECT p.recipe_id, p.jars_per_batch, SUM(sm.min_jars) as total_jars
                FROM stock_minimums sm
                JOIN products p ON sm.product_id = p.id
                JOIN recipes r ON p.recipe_id = r.id
                WHERE r.is_sold_product = TRUE AND p.jars_per_batch IS NOT NULL AND p.jars_per_batch > 0
                GROUP BY p.recipe_id, p.jars_per_batch;
            """
            )
            products_to_make = cur.fetchall()

            recipe_cache = {}

            for prod in products_to_make:
                batches_needed = math.ceil(
                    float(prod["total_jars"]) / float(prod["jars_per_batch"])
                )
                base_ingredients_one_batch = get_base_ingredients(
                    prod["recipe_id"], conn, recipe_cache
                )
                for ing in base_ingredients_one_batch:
                    scaled_ing = dict(ing)
                    scaled_ing["quantity"] = (
                        float(scaled_ing["quantity"]) * batches_needed
                    )
                    all_base_ingredients.append(scaled_ing)
            totals_needed = defaultdict(lambda: {"total_needed": 0})
            for ing in all_base_ingredients:
                inv_item_id = ing.get("inventory_item_id")
                if inv_item_id:
                    totals_needed[inv_item_id]["total_needed"] += float(
                        ing.get("quantity", 0)
                    )
            low_stock_count = 0
            for inv_id, needed_data in totals_needed.items():
                available = inventory_levels.get(inv_id, {"available": 0})["available"]
                net_needed = needed_data["total_needed"] - available
                if net_needed > 0:
                    low_stock_count += 1
            dashboard_data["low_stock_count"] = low_stock_count
    except psycopg2.Error as e:
        flash(f"Error fetching dashboard data: {e}", "error")
        print(f"DB Error fetching dashboard data: {e}")

    return render_template("index.html", dashboard_data=dashboard_data)


# --- NEW ROUTE TO SET THE SESSION VARIABLE ---
@bp.route("/set-forecast", methods=["POST"])
def set_forecast():
    """
    Sets the forecast multiplier in the user's session.
    """
    months = request.form.get("months")
    try:
        # Validate and save to session
        forecast_months = int(months)
        if forecast_months not in [1, 2, 3, 6]:  # Only allow specific values
            raise ValueError("Invalid forecast period")

        session["forecast_months"] = forecast_months
        flash(f"Forecast period set to {forecast_months} month(s).", "success")
    except (ValueError, TypeError):
        flash("Invalid forecast value selected.", "error")
        session["forecast_months"] = 1  # Reset to default on error

    # Redirect back to the requirements page, which will now use the new session value
    return redirect(url_for("core.requirements_page"))


# --- MODIFIED REQUIREMENTS ROUTE ---
@bp.route("/requirements")
def requirements_page():
    conn = get_db()
    sorted_report_data = []

    try:
        # --- GET THE MULTIPLIER FROM THE SESSION (DEFAULT TO 1) ---
        forecast_months = session.get("forecast_months", 1)

        inventory_levels = {}
        product_needs = defaultdict(lambda: {"min_total": 0, "stock_total": 0})

        recipe_cache = {}

        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute(
                "SELECT id, name, unit, quantity_on_hand, quantity_allocated FROM inventory_items;"
            )
            for item in cur.fetchall():
                inventory_levels[item["id"]] = {
                    "name": item["name"],
                    "unit": item["unit"],
                    "on_hand": float(item.get("quantity_on_hand", 0)),
                    "allocated": float(item.get("quantity_allocated", 0)),
                    "available": float(item.get("quantity_on_hand", 0))
                    - float(item.get("quantity_allocated", 0)),
                }

            cur.execute(
                "SELECT product_id, SUM(min_jars) as total_min FROM stock_minimums GROUP BY product_id;"
            )
            for row in cur.fetchall():
                product_needs[row["product_id"]]["min_total"] = float(row["total_min"])

            cur.execute(
                "SELECT product_id, SUM(quantity) as total_stock FROM location_stock GROUP BY product_id;"
            )
            for row in cur.fetchall():
                product_needs[row["product_id"]]["stock_total"] = float(
                    row["total_stock"]
                )

            all_base_ingredients = []
            for product_id, needs in product_needs.items():

                # --- APPLY THE MULTIPLIER HERE ---
                scaled_min_total = float(needs["min_total"]) * forecast_months
                jars_to_produce = max(0, scaled_min_total - needs["stock_total"])

                if jars_to_produce > 0:
                    cur.execute(
                        """
                        SELECT recipe_id, jars_per_batch 
                        FROM products 
                        WHERE id = %s AND jars_per_batch IS NOT NULL AND jars_per_batch > 0;
                    """,
                        (product_id,),
                    )
                    prod_info = cur.fetchone()

                    if prod_info:
                        recipe_id = prod_info["recipe_id"]
                        jars_per_batch = float(prod_info["jars_per_batch"])

                        # --- THIS CALCULATION NOW USES THE SCALED VALUE ---
                        batches_needed = math.ceil(jars_to_produce / jars_per_batch)

                        base_ingredients_one_batch = get_base_ingredients(
                            recipe_id, conn, recipe_cache
                        )
                        for ing in base_ingredients_one_batch:
                            scaled_ing = dict(ing)
                            scaled_ing["quantity"] = (
                                float(scaled_ing["quantity"]) * batches_needed
                            )
                            all_base_ingredients.append(scaled_ing)

            totals_needed = defaultdict(
                lambda: {
                    "name": "",
                    "unit": "",
                    "total_needed": 0,
                    "inventory_item_id": None,
                }
            )
            for ing in all_base_ingredients:
                inv_item_id = ing.get("inventory_item_id")
                if inv_item_id:
                    name = ing.get("name", "Unknown").strip()
                    unit = ing.get("unit", "Unknown").strip()
                    key = inv_item_id
                    totals_needed[key]["name"] = name
                    totals_needed[key]["unit"] = unit
                    totals_needed[key]["inventory_item_id"] = inv_item_id
                    totals_needed[key]["total_needed"] += float(ing.get("quantity", 0))

            report_data = []
            for inv_id, needed_data in totals_needed.items():
                inv_info = inventory_levels.get(
                    inv_id, {"on_hand": 0, "allocated": 0, "available": 0}
                )
                total_needed = needed_data["total_needed"]
                available = inv_info["available"]
                net_needed = max(0, total_needed - available)
                if net_needed > 0:
                    report_data.append(
                        {
                            "name": needed_data["name"],
                            "unit": needed_data["unit"],
                            "total_needed": round(total_needed, 2),
                            "on_hand": round(inv_info["on_hand"], 2),
                            "allocated": round(inv_info["allocated"], 2),
                            "available": round(available, 2),
                            "net_needed": round(net_needed, 2),
                        }
                    )
        sorted_report_data = sorted(report_data, key=lambda x: x["name"])

    except psycopg2.Error as e:
        flash(f"Error generating requirements report: {e}", "error")
        print(f"DB Error requirements page: {e}")

    # --- PASS THE CURRENT VALUE TO THE TEMPLATE ---
    return render_template(
        "requirements.html",
        report_data=sorted_report_data,
        current_months=forecast_months,
    )


@bp.route("/planner", methods=["GET", "POST"])
def production_planner():
    conn = get_db()
    calculated_requirements = None
    inventory_levels = {}
    sellable_products = []

    recipe_cache = {}

    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute(
                "SELECT id, name, unit, quantity_on_hand, quantity_allocated FROM inventory_items;"
            )
            for item in cur.fetchall():
                inventory_levels[item["id"]] = {
                    "name": item["name"],
                    "unit": item["unit"],
                    "on_hand": float(item.get("quantity_on_hand", 0)),
                    "allocated": float(item.get("quantity_allocated", 0)),
                    "available": float(item.get("quantity_on_hand", 0))
                    - float(item.get("quantity_allocated", 0)),
                }
            cur.execute(
                """SELECT p.id, p.sku, p.product_name, p.jars_per_batch, r.id as recipe_id, r.name as recipe_name FROM products p JOIN recipes r ON p.recipe_id = r.id WHERE r.is_sold_product = TRUE AND p.jars_per_batch IS NOT NULL AND p.jars_per_batch > 0 ORDER BY p.product_name;"""
            )
            sellable_products = cur.fetchall()

        if request.method == "POST":
            all_base_ingredients_run = []
            for product in sellable_products:
                jars_to_make_str = request.form.get(f"jars_product_{product['id']}")
                try:
                    jars_to_make = int(jars_to_make_str) if jars_to_make_str else 0
                except ValueError:
                    jars_to_make = 0
                if jars_to_make > 0:
                    batches_needed = math.ceil(
                        jars_to_make / float(product["jars_per_batch"])
                    )
                    base_ingredients_one_batch = get_base_ingredients(
                        product["recipe_id"], conn, recipe_cache
                    )
                    for ing in base_ingredients_one_batch:
                        scaled_ing = dict(ing)
                        scaled_ing["quantity"] = (
                            float(scaled_ing["quantity"]) * batches_needed
                        )
                        all_base_ingredients_run.append(scaled_ing)

            totals_run_needed = defaultdict(
                lambda: {
                    "name": "",
                    "unit": "",
                    "total_needed": 0,
                    "inventory_item_id": None,
                }
            )
            for ing in all_base_ingredients_run:
                inv_item_id = ing.get("inventory_item_id")
                if inv_item_id:
                    name = ing.get("name", "Unknown").strip()
                    unit = ing.get("unit", "Unknown").strip()
                    key = inv_item_id
                    totals_run_needed[key]["name"] = name
                    totals_run_needed[key]["unit"] = unit
                    totals_run_needed[key]["inventory_item_id"] = inv_item_id
                    totals_run_needed[key]["total_needed"] += float(
                        ing.get("quantity", 0)
                    )

            run_report_data = []
            for inv_id, needed_data in totals_run_needed.items():
                inv_info = inventory_levels.get(inv_id, {"available": 0})
                total_needed = needed_data["total_needed"]
                available = inv_info["available"]
                net_needed = max(0, total_needed - available)
                run_report_data.append(
                    {
                        "name": needed_data["name"],
                        "unit": needed_data["unit"],
                        "total_needed": round(total_needed, 2),
                        "available": round(available, 2),
                        "net_needed": round(net_needed, 2),
                    }
                )
            calculated_requirements = sorted(run_report_data, key=lambda x: x["name"])

    except psycopg2.Error as e:
        flash(f"Error in production planner: {e}", "error")
        print(f"DB Error planner page: {e}")

    return render_template(
        "planner.html", products=sellable_products, requirements=calculated_requirements
    )


@bp.route("/totals")
def ingredient_totals():
    conn = get_db()
    sorted_totals = []

    recipe_cache = {}

    try:
        all_base_ingredients = []
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute("SELECT id FROM recipes;")
            all_recipe_ids = cur.fetchall()
            for rec_id in all_recipe_ids:
                all_base_ingredients.extend(
                    get_base_ingredients(rec_id["id"], conn, recipe_cache)
                )

        totals = defaultdict(lambda: {"name": "", "unit": "", "total_quantity": 0})
        for ing in all_base_ingredients:
            name = ing.get("name", "Unknown").strip()
            unit = ing.get("unit", "Unknown").strip()
            key = (name.lower(), unit.lower())
            totals[key]["name"] = name
            totals[key]["unit"] = unit
            totals[key]["total_quantity"] += float(ing.get("quantity", 0))

        sorted_totals = sorted(totals.values(), key=lambda x: x["name"])

    except psycopg2.Error as e:
        flash(f"Error calculating totals: {e}", "error")
        print(f"DB Error ingredient totals: {e}")

    return render_template("totals.html", totals=sorted_totals)
