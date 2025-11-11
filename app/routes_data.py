import psycopg2
from psycopg2.extras import DictCursor, RealDictCursor
from flask import Blueprint, flash, g, redirect, render_template, request, url_for
from collections import defaultdict
import json

from app.db import get_db
from app.models import get_base_ingredients, _log_inventory_adjustment

# --- All data management routes ---
bp = Blueprint("data", __name__)


# --- Helper function for saving recipe ingredients (DRY) ---
def _process_and_save_ingredients(cur, recipe_id):
    """
    Reads ingredient form data from the request and saves it
    to the database, linked to the given recipe_id.
    """
    inventory_item_ids = request.form.getlist("inventory_item_id")
    quantities = request.form.getlist("quantity")
    sub_recipe_ids = request.form.getlist("sub_recipe_id")

    if not (len(quantities) == len(inventory_item_ids) == len(sub_recipe_ids)):
        flash("Form data inconsistency.", "error")
        raise ValueError("Form list lengths mismatch")

    for i in range(len(quantities)):
        inventory_item_id = inventory_item_ids[i] if inventory_item_ids[i] else None
        sub_recipe_id = sub_recipe_ids[i] if sub_recipe_ids[i] else None
        quantity_str = quantities[i]

        if not inventory_item_id and not sub_recipe_id:
            flash(f"Row {i+1} skipped (empty).", "warning")
            continue
        if inventory_item_id and sub_recipe_id:
            flash(
                f"Row {i+1} skipped (cannot be raw material and sub-recipe).", "warning"
            )
            continue
        try:
            quantity_val = float(quantity_str) if quantity_str else 0
            if quantity_val <= 0:
                flash(
                    f"Quantity for row {i+1} must be positive. Row skipped.", "warning"
                )
                continue
        except ValueError:
            flash(f"Invalid quantity for row {i+1}. Row skipped.", "warning")
            continue

        item_name = None
        item_unit = None
        if inventory_item_id:
            cur.execute(
                "SELECT name, unit FROM inventory_items WHERE id = %s;",
                (inventory_item_id,),
            )
            item_data = cur.fetchone()
            if item_data:
                item_name, item_unit = item_data
        elif sub_recipe_id:
            cur.execute("SELECT name FROM recipes WHERE id = %s;", (sub_recipe_id,))
            sub_recipe_data = cur.fetchone()
            if sub_recipe_data:
                item_name = sub_recipe_data[0]
                item_unit = "batch"  # Sub-recipes are measured in batches

        cur.execute(
            """INSERT INTO ingredients (recipe_id, inventory_item_id, sub_recipe_id, quantity, name, unit) 
               VALUES (%s, %s, %s, %s, %s, %s);""",
            (
                recipe_id,
                inventory_item_id,
                sub_recipe_id,
                quantity_val,
                item_name,
                item_unit,
            ),
        )


# --- Recipe Routes ---
@bp.route("/recipes")
def recipe_dashboard():
    recipes_list = []
    conn = get_db()

    # --- Use request-local cache ---
    recipe_cache = {}

    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute("SELECT * FROM recipes ORDER BY name;")
            recipes_from_db = cur.fetchall()
            for recipe in recipes_from_db:
                recipe_dict = dict(recipe)
                cur.execute(
                    """SELECT i.quantity, i.sub_recipe_id, COALESCE(inv.name, r_sub.name, i.name, 'Unknown') as name, COALESCE(inv.unit, i.unit, 'N/A') as unit 
                       FROM ingredients i 
                       LEFT JOIN inventory_items inv ON i.inventory_item_id = inv.id 
                       LEFT JOIN recipes r_sub ON i.sub_recipe_id = r_sub.id 
                       WHERE i.recipe_id = %s;""",
                    (recipe["id"],),
                )
                recipe_dict["ingredients"] = [dict(ing) for ing in cur.fetchall()]
                base_ingredients_for_totals = get_base_ingredients(
                    recipe["id"], conn, recipe_cache
                )

                totals = {"grams": 0, "mLs": 0}
                for ing in base_ingredients_for_totals:
                    unit = ing.get("unit", "").lower()
                    quantity = float(ing.get("quantity", 0))
                    if unit == "grams":
                        totals["grams"] += quantity
                    elif unit == "mls":
                        totals["mLs"] += quantity
                recipe_dict["totals"] = {
                    "grams": round(totals["grams"], 2),
                    "mLs": round(totals["mLs"], 2),
                }
                recipes_list.append(recipe_dict)
    except psycopg2.Error as e:
        flash(f"Error fetching recipes: {e}", "error")
        print(f"DB Error fetching recipes: {e}")

    return render_template("recipes.html", recipes=recipes_list)


@bp.route("/new-recipe")
def new_recipe_form():
    conn = get_db()
    recipes = []
    inventory_items = []
    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute("SELECT id, name FROM recipes ORDER BY name;")
            recipes = cur.fetchall()
            cur.execute("SELECT id, name, unit FROM inventory_items ORDER BY name;")
            inventory_items = cur.fetchall()
    except psycopg2.Error as e:
        flash(f"Error fetching data: {e}", "error")
        print(f"DB Error new recipe form: {e}")

    # --- Use the new unified form template ---
    return render_template(
        "recipe_form.html",
        recipe=None,
        ingredients=[],
        recipes=recipes,
        inventory_items=inventory_items,
    )


@bp.route("/recipes/create", methods=["POST"])
def create_recipe():
    conn = get_db()
    recipe_id = None
    try:
        with conn.cursor() as cur:
            recipe_name = request.form["recipe_name"]
            yield_quantity = request.form.get("yield_quantity") or None
            yield_unit = request.form.get("yield_unit") or None
            is_sold_product = "is_sold_product" in request.form

            # --- NEW: Get tools and instructions ---
            tools_text = request.form.get("tools", "")
            instructions_text = request.form.get("instructions", "")

            # Convert text-area-per-line to a clean list
            tools_list = [
                tool.strip() for tool in tools_text.split("\n") if tool.strip()
            ]
            instructions_list = [
                inst.strip() for inst in instructions_text.split("\n") if inst.strip()
            ]

            cur.execute(
                """INSERT INTO recipes (name, yield_quantity, yield_unit, is_sold_product, tools, instructions) 
                   VALUES (%s, %s, %s, %s, %s, %s) RETURNING id;""",
                (
                    recipe_name,
                    yield_quantity,
                    yield_unit,
                    is_sold_product,
                    json.dumps(tools_list),  # --- NEW
                    json.dumps(instructions_list),  # --- NEW
                ),
            )
            recipe_id = cur.fetchone()[0]

            # --- Use the new helper function ---
            _process_and_save_ingredients(cur, recipe_id)

        conn.commit()
        flash("Recipe created successfully!", "success")
        return redirect(url_for("data.recipe_dashboard"))

    except (psycopg2.Error, ValueError) as e:
        if conn:
            conn.rollback()
        flash(f"Error creating recipe: {e}", "error")
        print(f"Error creating recipe: {e}")

        # --- On failure, re-render the form with the same data ---
        recipes = []
        inventory_items = []
        try:
            with conn.cursor(cursor_factory=DictCursor) as cur_err:
                cur_err.execute("SELECT id, name FROM recipes ORDER BY name;")
                recipes = cur_err.fetchall()
                cur_err.execute(
                    "SELECT id, name, unit FROM inventory_items ORDER BY name;"
                )
                inventory_items = cur_err.fetchall()
        except psycopg2.Error as fetch_e:
            flash(f"Error fetching form data: {fetch_e}", "error")

        return render_template(
            "recipe_form.html",
            recipe=None,
            ingredients=[],
            recipes=recipes,
            inventory_items=inventory_items,
        )


@bp.route("/recipes/edit/<int:recipe_id>")
def edit_recipe_form(recipe_id):
    conn = get_db()
    recipe = None
    ingredients = []
    all_recipes = []
    inventory_items = []
    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute("SELECT * FROM recipes WHERE id = %s;", (recipe_id,))
            recipe = cur.fetchone()
            if not recipe:
                flash(f"Recipe ID {recipe_id} not found.", "error")
                return redirect(url_for("data.recipe_dashboard"))

            cur.execute("SELECT * FROM ingredients WHERE recipe_id = %s;", (recipe_id,))
            ingredients = cur.fetchall()
            cur.execute("SELECT id, name FROM recipes ORDER BY name;")
            all_recipes = cur.fetchall()
            cur.execute("SELECT id, name, unit FROM inventory_items ORDER BY name;")
            inventory_items = cur.fetchall()
    except psycopg2.Error as e:
        flash(f"Error fetching data: {e}", "error")
        print(f"DB Error edit recipe form {recipe_id}: {e}")
        return redirect(url_for("data.recipe_dashboard"))

    # --- Use the new unified form template ---
    return render_template(
        "recipe_form.html",
        recipe=recipe,
        ingredients=ingredients,
        recipes=all_recipes,
        inventory_items=inventory_items,
    )


@bp.route("/recipes/update/<int:recipe_id>", methods=["POST"])
def update_recipe(recipe_id):
    conn = get_db()
    try:
        with conn.cursor() as cur:
            new_name = request.form["recipe_name"]
            yield_quantity = request.form.get("yield_quantity") or None
            yield_unit = request.form.get("yield_unit") or None
            is_sold_product = "is_sold_product" in request.form

            # --- NEW: Get tools and instructions ---
            tools_text = request.form.get("tools", "")
            instructions_text = request.form.get("instructions", "")

            # Convert text-area-per-line to a clean list
            tools_list = [
                tool.strip() for tool in tools_text.split("\n") if tool.strip()
            ]
            instructions_list = [
                inst.strip() for inst in instructions_text.split("\n") if inst.strip()
            ]

            cur.execute(
                """UPDATE recipes 
                   SET name = %s, yield_quantity = %s, yield_unit = %s, 
                       is_sold_product = %s, tools = %s, instructions = %s 
                   WHERE id = %s;""",
                (
                    new_name,
                    yield_quantity,
                    yield_unit,
                    is_sold_product,
                    json.dumps(tools_list),  # --- NEW
                    json.dumps(instructions_list),  # --- NEW
                    recipe_id,
                ),
            )

            # --- Re-use the same ingredient logic ---
            cur.execute("DELETE FROM ingredients WHERE recipe_id = %s;", (recipe_id,))
            _process_and_save_ingredients(cur, recipe_id)

        conn.commit()
        flash("Recipe updated successfully!", "success")
        return redirect(url_for("data.recipe_dashboard"))

    except (psycopg2.Error, ValueError) as e:
        if conn:
            conn.rollback()
        flash(f"Error updating recipe: {e}", "error")
        print(f"DB Error update recipe {recipe_id}: {e}")
        return redirect(url_for("data.edit_recipe_form", recipe_id=recipe_id))


@bp.route("/recipes/delete/<int:recipe_id>", methods=["POST"])
def delete_recipe(recipe_id):
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM recipes WHERE id = %s;", (recipe_id,))
            conn.commit()
            flash("Recipe deleted.", "success")
    except psycopg2.Error as e:
        if conn:
            conn.rollback()
        flash(f"Error deleting recipe: {e.diag.message_primary}", "error")
        print(f"DB Error delete recipe {recipe_id}: {e}")

    return redirect(url_for("data.recipe_dashboard"))


# --- Product Routes ---
@bp.route("/products")
def products_page():
    conn = get_db()
    products = []
    recipes = []
    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute(
                """SELECT p.id, p.sku, p.product_name, p.jars_per_batch, r.name as recipe_name 
                   FROM products p 
                   LEFT JOIN recipes r ON p.recipe_id = r.id 
                   ORDER BY p.product_name, p.sku;"""
            )
            products = cur.fetchall()
            cur.execute(
                "SELECT id, name FROM recipes WHERE is_sold_product = TRUE ORDER BY name;"
            )
            recipes = cur.fetchall()
    except psycopg2.Error as e:
        flash(f"Error fetching products: {e}", "error")
        print(f"DB Error products page: {e}")

    return render_template("products.html", products=products, recipes=recipes)


@bp.route("/products/add", methods=["POST"])
def add_product():
    conn = get_db()
    try:
        sku = request.form["sku"]
        product_name = request.form["product_name"]
        recipe_id = request.form["recipe_id"]
        jars_per_batch = request.form.get("jars_per_batch") or None
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO products (sku, product_name, recipe_id, jars_per_batch) VALUES (%s, %s, %s, %s);",
                (sku, product_name, recipe_id, jars_per_batch),
            )
            conn.commit()
            flash("Product added.", "success")
    except psycopg2.Error as e:
        if conn:
            conn.rollback()
        flash(f"Error adding product: {e}", "error")
        print(f"DB Error add product: {e}")

    return redirect(url_for("data.products_page"))


@bp.route("/products/edit/<int:id>")
def edit_product(id):
    conn = get_db()
    product = None
    recipes = []
    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute("SELECT * FROM products WHERE id = %s;", (id,))
            product = cur.fetchone()
            if not product:
                flash(f"Product ID {id} not found.", "error")
                return redirect(url_for("data.products_page"))
            cur.execute(
                "SELECT id, name FROM recipes WHERE is_sold_product = TRUE ORDER BY name;"
            )
            recipes = cur.fetchall()
    except psycopg2.Error as e:
        flash(f"Error fetching product data: {e}", "error")
        print(f"DB Error edit product GET {id}: {e}")
        return redirect(url_for("data.products_page"))

    return render_template("edit_product.html", product=product, recipes=recipes)


@bp.route("/products/update/<int:id>", methods=["POST"])
def update_product(id):
    conn = get_db()
    try:
        sku = request.form["sku"]
        product_name = request.form["product_name"]
        recipe_id = request.form["recipe_id"]
        jars_per_batch = request.form.get("jars_per_batch") or None
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE products SET sku = %s, product_name = %s, recipe_id = %s, jars_per_batch = %s WHERE id = %s;",
                (sku, product_name, recipe_id, jars_per_batch, id),
            )
            conn.commit()
            flash("Product updated.", "success")
    except psycopg2.Error as e:
        if conn:
            conn.rollback()
        flash(f"Error updating product: {e}", "error")
        print(f"DB Error update product {id}: {e}")

    return redirect(url_for("data.products_page"))


@bp.route("/products/delete/<int:id>", methods=["POST"])
def delete_product(id):
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM products WHERE id = %s;", (id,))
            conn.commit()
            flash("Product deleted.", "success")
    except psycopg2.Error as e:
        if conn:
            conn.rollback()
        flash(f"Error deleting product: {e.diag.message_primary}", "error")
        print(f"DB Error delete product {id}: {e}")

    return redirect(url_for("data.products_page"))


# --- Location Routes ---
@bp.route("/locations", methods=["GET", "POST"])
def locations_page():
    conn = get_db()
    locations = []
    try:
        if request.method == "POST":
            location_name = request.form["name"]
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO locations (name) VALUES (%s);", (location_name,)
                )
                conn.commit()
                flash("Location added.", "success")
            return redirect(url_for("data.locations_page"))

        # GET request
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute("SELECT * FROM locations ORDER BY name;")
            locations = cur.fetchall()
    except psycopg2.Error as e:
        if conn and request.method == "POST":
            conn.rollback()
        flash(f"Error accessing locations: {e}", "error")
        print(f"DB Error locations page: {e}")

    return render_template("locations.html", locations=locations)


@bp.route("/locations/delete/<int:id>", methods=["POST"])
def delete_location(id):
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM locations WHERE id = %s;", (id,))
            conn.commit()
            flash("Location deleted.", "success")
    except psycopg2.Error as e:
        if conn:
            conn.rollback()
        flash(f"Error deleting location: {e.diag.message_primary}", "error")
        print(f"DB Error delete location {id}: {e}")

    return redirect(url_for("data.locations_page"))


# --- Supplier Routes ---
@bp.route("/suppliers", methods=["GET"])
def suppliers_page():
    conn = get_db()
    suppliers = []
    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute("SELECT * FROM suppliers ORDER BY name;")
            suppliers = cur.fetchall()
    except psycopg2.Error as e:
        flash(f"Error fetching suppliers: {e}", "error")
        print(f"DB Error suppliers page GET: {e}")

    return render_template("suppliers.html", suppliers=suppliers)


@bp.route("/suppliers/add", methods=["POST"])
def add_supplier():
    conn = get_db()
    try:
        name = request.form["name"]
        contact = request.form.get("contact_person")
        email = request.form.get("email")
        phone = request.form.get("phone")
        website = request.form.get("website")
        notes = request.form.get("notes")
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM suppliers WHERE LOWER(name) = LOWER(%s);", (name,)
            )
            existing = cur.fetchone()
            if existing:
                flash(f"Supplier '{name}' already exists.", "warning")
            else:
                cur.execute(
                    "INSERT INTO suppliers (name, contact_person, email, phone, website, notes) VALUES (%s, %s, %s, %s, %s, %s);",
                    (name, contact, email, phone, website, notes),
                )
                conn.commit()
                flash("Supplier added.", "success")
    except psycopg2.Error as e:
        if conn:
            conn.rollback()
        flash(f"Database error adding supplier: {e}", "error")
        print(f"DB Error add supplier POST: {e}")

    return redirect(url_for("data.suppliers_page"))


@bp.route("/suppliers/edit/<int:id>", methods=["GET"])
def edit_supplier(id):
    conn = get_db()
    supplier = None
    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute("SELECT * FROM suppliers WHERE id = %s;", (id,))
            supplier = cur.fetchone()
        if supplier is None:
            flash(f"Supplier ID {id} not found.", "error")
            return redirect(url_for("data.suppliers_page"))
    except psycopg2.Error as e:
        flash(f"Error fetching supplier: {e}", "error")
        print(f"DB Error edit supplier GET {id}: {e}")
        return redirect(url_for("data.suppliers_page"))

    return render_template("edit_supplier.html", supplier=supplier)


@bp.route("/suppliers/update/<int:id>", methods=["POST"])
def update_supplier(id):
    conn = get_db()
    try:
        name = request.form["name"]
        contact = request.form.get("contact_person")
        email = request.form.get("email")
        phone = request.form.get("phone")
        website = request.form.get("website")
        notes = request.form.get("notes")
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM suppliers WHERE LOWER(name) = LOWER(%s) AND id != %s;",
                (name, id),
            )
            existing = cur.fetchone()
            if existing:
                flash(
                    f"Another supplier with the name '{name}' already exists.", "error"
                )
                return redirect(url_for("data.edit_supplier", id=id))
            else:
                cur.execute(
                    """UPDATE suppliers SET name=%s, contact_person=%s, email=%s, phone=%s, website=%s, notes=%s WHERE id=%s;""",
                    (name, contact, email, phone, website, notes, id),
                )
                conn.commit()
                flash("Supplier updated.", "success")
    except psycopg2.Error as e:
        if conn:
            conn.rollback()
        flash(f"Database error updating supplier: {e}", "error")
        print(f"DB Error update supplier POST {id}: {e}")
        return redirect(url_for("data.edit_supplier", id=id))

    return redirect(url_for("data.suppliers_page"))


@bp.route("/suppliers/delete/<int:id>", methods=["POST"])
def delete_supplier(id):
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM suppliers WHERE id = %s;", (id,))
            conn.commit()
            flash("Supplier deleted.", "success")
    except psycopg2.Error as e:
        if conn:
            conn.rollback()
        if hasattr(e, "pgcode") and e.pgcode == "23503":
            flash(f"Cannot delete supplier: Linked to purchase orders.", "error")
        else:
            flash(f"Error deleting supplier: {e}", "error")
        print(f"DB Error delete supplier {id}: {e}")

    return redirect(url_for("data.suppliers_page"))


# --- Inventory Item Routes ---
@bp.route("/inventory", methods=["GET", "POST"])
def inventory_items_page():
    conn = get_db()
    inventory_items = []
    try:
        if request.method == "POST":
            name = request.form["name"]
            unit = request.form["unit"]
            qty_on_hand_str = request.form.get("quantity_on_hand")
            try:
                qty_on_hand = float(qty_on_hand_str) if qty_on_hand_str else 0.0
            except ValueError:
                qty_on_hand = 0.0
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id FROM inventory_items WHERE LOWER(name) = LOWER(%s) AND LOWER(unit) = LOWER(%s);",
                    (name, unit),
                )
                existing = cur.fetchone()
                if not existing:
                    cur.execute(
                        "INSERT INTO inventory_items (name, unit, quantity_on_hand) VALUES (%s, %s, %s);",
                        (name, unit, qty_on_hand),
                    )
                    conn.commit()
                    flash("Item added.", "success")
                else:
                    flash("Item with this name/unit already exists.", "warning")
            return redirect(url_for("data.inventory_items_page"))

        # GET
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute(
                "SELECT *, (quantity_on_hand - quantity_allocated) as quantity_available FROM inventory_items ORDER BY name;"
            )
            inventory_items = cur.fetchall()
    except psycopg2.Error as e:
        if conn and request.method == "POST":
            conn.rollback()
        flash(f"Error accessing inventory: {e}", "error")
        print(f"DB Error inventory page: {e}")

    return render_template("inventory_items.html", inventory_items=inventory_items)


@bp.route("/inventory/edit/<int:id>", methods=["GET"])
def edit_inventory_item(id):
    conn = get_db()
    item = None
    recipes = []
    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute("SELECT * FROM inventory_items WHERE id = %s;", (id,))
            item = cur.fetchone()
            if item is None:
                flash(f"Item ID {id} not found.", "error")
                return redirect(url_for("data.inventory_items_page"))
            cur.execute("SELECT id, name FROM recipes ORDER BY name;")
            recipes = cur.fetchall()
    except psycopg2.Error as e:
        flash(f"Error fetching item: {e}", "error")
        print(f"DB Error edit inventory item GET {id}: {e}")
        return redirect(url_for("data.inventory_items_page"))

    return render_template("edit_inventory_item.html", item=item, recipes=recipes)


@bp.route("/inventory/update/<int:id>", methods=["POST"])
def update_inventory_item(id):
    conn = get_db()
    try:
        name = request.form["name"]
        unit = request.form["unit"]
        qty_on_hand_str = request.form.get("quantity_on_hand")
        linked_recipe_id = request.form.get("linked_recipe_id") or None
        try:
            qty_on_hand = float(qty_on_hand_str) if qty_on_hand_str else 0.0
        except ValueError:
            qty_on_hand = 0.0

        with conn.cursor() as cur:
            cur.execute(
                """UPDATE inventory_items 
                SET name = %s, unit = %s, quantity_on_hand = %s, linked_recipe_id = %s 
                WHERE id = %s;""",
                (name, unit, qty_on_hand, linked_recipe_id, id),
            )
        conn.commit()
        flash("Item updated successfully.", "success")
    except psycopg2.Error as e:
        conn.rollback()
        flash(f"Error updating item: {e}", "error")
        print(f"DB Error update inventory item {id}: {e}")

    return redirect(url_for("data.inventory_items_page"))


@bp.route("/inventory/delete/<int:id>", methods=["POST"])
def delete_inventory_item(id):
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM inventory_items WHERE id = %s;", (id,))
        conn.commit()
        flash("Item deleted.", "success")
    except psycopg2.Error as e:
        conn.rollback()
        print(f"Error delete item {id}: {e}")
        flash(f"Cannot delete item: {e.diag.message_primary}", "error")

    return redirect(url_for("data.inventory_items_page"))
