import psycopg2
from psycopg2.extras import DictCursor, RealDictCursor
from flask import Blueprint, flash, g, redirect, render_template, request, url_for
from collections import defaultdict
import math
from datetime import datetime

from app.db import get_db
from app.models import get_base_ingredients, _log_inventory_adjustment

# --- All operational routes ---
bp = Blueprint("ops", __name__)


# --- Stock Minimums Routes ---
@bp.route("/stock-minimums", methods=["GET", "POST"])
def stock_minimums_page():
    conn = get_db()
    locations = []
    products = []
    minimums = []
    try:
        if request.method == "POST":
            location_id = request.form["location_id"]
            product_id = request.form["product_id"]
            min_jars = request.form["min_jars"]
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO stock_minimums (location_id, product_id, min_jars) 
                       VALUES (%s, %s, %s) 
                       ON CONFLICT (product_id, location_id) DO UPDATE SET min_jars = EXCLUDED.min_jars;""",
                    (location_id, product_id, min_jars),
                )
                conn.commit()
                flash("Stock minimum set/updated.", "success")
            return redirect(url_for("ops.stock_minimums_page"))

        # GET
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute("SELECT * FROM locations ORDER BY name;")
            locations = cur.fetchall()
            cur.execute(
                """SELECT p.id, p.sku, p.product_name, r.name as recipe_name 
                   FROM products p JOIN recipes r ON p.recipe_id = r.id 
                   WHERE r.is_sold_product = TRUE ORDER BY p.product_name, p.sku;"""
            )
            products = cur.fetchall()
            cur.execute(
                """SELECT sm.id, l.name as location_name, p.sku, p.product_name, r.name as recipe_name, sm.min_jars 
                   FROM stock_minimums sm 
                   JOIN locations l ON sm.location_id = l.id 
                   JOIN products p ON sm.product_id = p.id 
                   JOIN recipes r ON p.recipe_id = r.id 
                   ORDER BY l.name, p.sku;"""
            )
            minimums = cur.fetchall()
    except psycopg2.Error as e:
        if conn and request.method == "POST":
            conn.rollback()
        flash(f"Error accessing stock minimums: {e}", "error")
        print(f"DB Error stock minimums page: {e}")

    return render_template(
        "stock_minimums.html", locations=locations, products=products, minimums=minimums
    )


@bp.route("/stock-minimums/delete/<int:id>", methods=["POST"])
def delete_stock_minimum(id):
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM stock_minimums WHERE id = %s;", (id,))
            conn.commit()
            flash("Stock minimum deleted.", "success")
    except psycopg2.Error as e:
        if conn:
            conn.rollback()
        flash(f"Error deleting stock minimum: {e}", "error")
        print(f"DB Error delete stock min {id}: {e}")

    return redirect(url_for("ops.stock_minimums_page"))


# --- Location Stock Routes ---
@bp.route("/location-stock", methods=["GET", "POST"])
def location_stock_page():
    conn = get_db()
    locations = []
    products = []
    current_stock = []

    try:
        if request.method == "POST":
            location_id = request.form["location_id"]
            product_id = request.form["product_id"]
            quantity = request.form.get("quantity", 0)

            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO location_stock (location_id, product_id, quantity) 
                    VALUES (%s, %s, %s) 
                    ON CONFLICT (product_id, location_id) 
                    DO UPDATE SET quantity = EXCLUDED.quantity;
                """,
                    (location_id, product_id, quantity),
                )
                conn.commit()
                flash("Finished stock quantity updated.", "success")
            return redirect(url_for("ops.location_stock_page"))

        # GET request
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute("SELECT * FROM locations ORDER BY name;")
            locations = cur.fetchall()
            cur.execute(
                """
                SELECT p.id, p.product_name, p.sku 
                FROM products p
                JOIN recipes r ON p.recipe_id = r.id
                WHERE r.is_sold_product = TRUE 
                ORDER BY p.product_name;
            """
            )
            products = cur.fetchall()
            cur.execute(
                """
                SELECT ls.id, l.name as location_name, p.product_name, p.sku, ls.quantity
                FROM location_stock ls
                JOIN locations l ON ls.location_id = l.id
                JOIN products p ON ls.product_id = p.id
                ORDER BY l.name, p.product_name;
            """
            )
            current_stock = cur.fetchall()

    except psycopg2.Error as e:
        if conn and request.method == "POST":
            conn.rollback()
        flash(f"Error accessing location stock: {e}", "error")
        print(f"DB Error location stock page: {e}")

    return render_template(
        "location_stock.html",
        locations=locations,
        products=products,
        current_stock=current_stock,
    )


@bp.route("/stock-transfer", methods=["GET", "POST"])
def stock_transfer():
    conn = get_db()
    locations = []
    products = []
    current_stock = []

    try:
        if request.method == "POST":
            product_id = request.form["product_id"]
            from_location_id = request.form["from_location_id"]
            to_location_id = request.form["to_location_id"]
            quantity_str = request.form.get("quantity", 0)

            if from_location_id == to_location_id:
                flash("Source and Destination locations cannot be the same.", "error")
                raise ValueError("Same location")

            try:
                quantity = int(quantity_str)
                if quantity <= 0:
                    flash("Quantity must be a positive number.", "error")
                    raise ValueError("Non-positive quantity")
            except ValueError:
                flash("Invalid quantity.", "error")
                raise ValueError("Invalid quantity")

            with conn.cursor(cursor_factory=DictCursor) as cur:
                cur.execute(
                    "SELECT quantity FROM location_stock WHERE product_id = %s AND location_id = %s FOR UPDATE;",
                    (product_id, from_location_id),
                )
                source_stock = cur.fetchone()

                if not source_stock or source_stock["quantity"] < quantity:
                    flash(
                        f"Not enough stock at source location. Available: {source_stock['quantity'] if source_stock else 0}",
                        "error",
                    )
                    raise Exception("Insufficient stock")

                cur.execute(
                    "UPDATE location_stock SET quantity = quantity - %s WHERE product_id = %s AND location_id = %s;",
                    (quantity, product_id, from_location_id),
                )
                cur.execute(
                    """INSERT INTO location_stock (product_id, location_id, quantity)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (product_id, location_id)
                    DO UPDATE SET quantity = location_stock.quantity + EXCLUDED.quantity;""",
                    (product_id, to_location_id, quantity),
                )
                conn.commit()
                flash(f"Successfully transferred {quantity} units.", "success")
            return redirect(url_for("ops.stock_transfer"))

        # GET request logic
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute("SELECT * FROM locations ORDER BY name;")
            locations = cur.fetchall()
            cur.execute(
                """
                SELECT p.id, p.product_name, p.sku 
                FROM products p
                JOIN recipes r ON p.recipe_id = r.id
                WHERE r.is_sold_product = TRUE 
                ORDER BY p.product_name;
            """
            )
            products = cur.fetchall()
            cur.execute(
                """
                SELECT ls.id, l.name as location_name, p.product_name, p.sku, ls.quantity
                FROM location_stock ls
                JOIN locations l ON ls.location_id = l.id
                JOIN products p ON ls.product_id = p.id
                WHERE ls.quantity > 0
                ORDER BY l.name, p.product_name;
            """
            )
            current_stock = cur.fetchall()

    except Exception as e:
        if conn:
            conn.rollback()
        if "Insufficient stock" not in str(e):
            flash(f"Error processing transfer: {e}", "error")
        print(f"DB Error stock transfer page: {e}")

    return render_template(
        "stock_transfer.html",
        locations=locations,
        products=products,
        current_stock=current_stock,
    )


# --- Inventory Adjustment Routes (Manual) ---
@bp.route("/inventory/adjust/<int:id>", methods=["GET"])
def adjust_inventory_item(id):
    conn = get_db()
    item = None
    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute("SELECT * FROM inventory_items WHERE id = %s;", (id,))
            item = cur.fetchone()
        if item is None:
            flash(f"Inventory item ID {id} not found.", "error")
            return redirect(url_for("data.inventory_items_page"))
    except psycopg2.Error as e:
        flash(f"Error fetching item: {e}", "error")
        return redirect(url_for("data.inventory_items_page"))

    return render_template("adjust_inventory_item.html", item=item)


@bp.route("/inventory/process-adjustment/<int:id>", methods=["POST"])
def process_adjustment(id):
    conn = get_db()
    try:
        adjustment_qty_str = request.form.get("adjustment_quantity")
        reason = request.form.get("reason") or "Manual Adjustment"

        if not adjustment_qty_str:
            flash("Adjustment quantity is required.", "error")
            return redirect(url_for("ops.adjust_inventory_item", id=id))
        try:
            adjustment_quantity = float(adjustment_qty_str)
        except ValueError:
            flash("Invalid quantity. Please enter a number.", "error")
            return redirect(url_for("ops.adjust_inventory_item", id=id))
        if adjustment_quantity == 0:
            flash("Adjustment quantity cannot be zero.", "warning")
            return redirect(url_for("ops.adjust_inventory_item", id=id))

        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute(
                "SELECT quantity_on_hand FROM inventory_items WHERE id = %s FOR UPDATE;",
                (id,),
            )
            item = cur.fetchone()
            if item is None:
                flash("Item not found.", "error")
                raise Exception("Item not found during adjustment")

            cur.execute(
                "UPDATE inventory_items SET quantity_on_hand = quantity_on_hand + %s WHERE id = %s RETURNING quantity_on_hand;",
                (adjustment_quantity, id),
            )
            updated_qty_row = cur.fetchone()
            new_quantity_on_hand = updated_qty_row[0] if updated_qty_row else 0

            _log_inventory_adjustment(
                cur, id, adjustment_quantity, reason, new_quantity_on_hand
            )
            conn.commit()
            flash(
                f"Inventory adjusted by {adjustment_quantity}. New QOH: {round(new_quantity_on_hand, 2)}",
                "success",
            )
    except Exception as e:
        if conn:
            conn.rollback()
        if "negative stock" in str(e):
            flash(str(e), "error")
        else:
            flash(f"Error processing adjustment: {e}", "error")
        print(f"Error process adjustment {id}: {e}")
        return redirect(url_for("ops.adjust_inventory_item", id=id))

    return redirect(url_for("data.inventory_items_page"))


@bp.route("/inventory-log")
def inventory_log():
    conn = get_db()
    adjustments = []
    inventory_items = []
    filter_item_id_str = request.args.get("inventory_item_id")
    filter_item_id = None
    if filter_item_id_str:
        try:
            filter_item_id = int(filter_item_id_str)
        except ValueError:
            flash("Invalid item ID for filtering.", "warning")
            filter_item_id = None

    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("SELECT id, name, unit FROM inventory_items ORDER BY name;")
        inventory_items = cursor.fetchall()
        query_sql = """
            SELECT 
                ia.id, ia.created_at AS adjustment_date, ii.name AS item_name,
                ia.adjustment_quantity AS change_quantity, ia.new_quantity, ia.reason
            FROM inventory_adjustments ia
            JOIN inventory_items ii ON ia.inventory_item_id = ii.id
        """
        params = []
        if filter_item_id:
            query_sql += " WHERE ia.inventory_item_id = %s"
            params.append(filter_item_id)
        query_sql += " ORDER BY ia.created_at DESC"
        cursor.execute(query_sql, tuple(params))
        adjustments = cursor.fetchall()
        cursor.close()
    except Exception as e:
        flash(f"Error fetching inventory log: {e}", "danger")
        print(f"Error fetching inventory log: {e}")

    return render_template(
        "inventory_log.html",
        adjustments=adjustments,
        inventory_items=inventory_items,
        selected_item_id=filter_item_id,
    )


# --- WIP (Work In Progress) Routes ---
@bp.route("/wip", methods=["GET", "POST"])
def wip_batches_page():
    conn = get_db()
    wip_batches = []
    sellable_products = []
    producible_items = []
    locations = []

    try:
        if request.method == "POST":
            batch_type = request.form.get("batch_type")

            with conn.cursor(cursor_factory=DictCursor) as cur:
                if batch_type == "PRODUCT":
                    product_id = request.form.get("product_id")
                    location_id = request.form.get("location_id")
                    if not product_id or not location_id:
                        flash("Product and Location are required.", "error")
                        raise ValueError("Missing product or location")
                    cur.execute(
                        "SELECT recipe_id FROM products WHERE id = %s;", (product_id,)
                    )
                    prod_data = cur.fetchone()
                    if not prod_data:
                        flash("Product not found.", "error")
                        raise ValueError("Product not found")
                    recipe_id = prod_data["recipe_id"]
                    cur.execute(
                        """INSERT INTO wip_batches (recipe_id, product_id, location_id, batch_type, status)
                           VALUES (%s, %s, %s, %s, %s);""",
                        (recipe_id, product_id, location_id, "PRODUCT", "In Progress"),
                    )
                    flash("New Product Batch started.", "success")

                elif batch_type == "INTERMEDIATE":
                    inventory_item_id = request.form.get("inventory_item_id")
                    if not inventory_item_id:
                        flash("Item is required.", "error")
                        raise ValueError("Missing item")
                    cur.execute(
                        "SELECT linked_recipe_id FROM inventory_items WHERE id = %s;",
                        (inventory_item_id,),
                    )
                    item_data = cur.fetchone()
                    if not item_data or not item_data["linked_recipe_id"]:
                        flash("Item is not linked to a producible recipe.", "error")
                        raise ValueError("Item not producible")
                    recipe_id = item_data["linked_recipe_id"]
                    cur.execute(
                        """INSERT INTO wip_batches (recipe_id, inventory_item_id, batch_type, status)
                           VALUES (%s, %s, %s, %s);""",
                        (recipe_id, inventory_item_id, "INTERMEDIATE", "In Progress"),
                    )
                    flash("New Intermediate Batch started.", "success")
                else:
                    flash("Invalid batch type.", "error")
                    raise ValueError("Invalid batch type")

            conn.commit()
            return redirect(url_for("ops.wip_batches_page"))

        # GET Request Logic
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute(
                """
                SELECT 
                    w.id, w.created_at, w.batch_type,
                    r.name as recipe_name,
                    p.product_name,
                    i.name as item_name
                FROM wip_batches w 
                JOIN recipes r ON w.recipe_id = r.id
                LEFT JOIN products p ON w.product_id = p.id
                LEFT JOIN inventory_items i ON w.inventory_item_id = i.id
                WHERE w.status = 'In Progress' ORDER BY w.created_at DESC;
            """
            )
            wip_batches = cur.fetchall()
            cur.execute(
                """
                SELECT p.id, p.product_name, p.sku 
                FROM products p 
                JOIN recipes r ON p.recipe_id = r.id 
                WHERE r.is_sold_product = TRUE AND p.jars_per_batch IS NOT NULL AND p.jars_per_batch > 0 
                ORDER BY p.product_name;
            """
            )
            sellable_products = cur.fetchall()
            cur.execute(
                """
                SELECT i.id, i.name, i.unit
                FROM inventory_items i
                WHERE i.linked_recipe_id IS NOT NULL
                ORDER BY i.name;
            """
            )
            producible_items = cur.fetchall()
            cur.execute("SELECT id, name FROM locations ORDER BY name;")
            locations = cur.fetchall()

    except (psycopg2.Error, ValueError) as e:
        if conn:
            conn.rollback()
        # --- FIX: Replaced string matching with explicit catch ---
        if isinstance(e, ValueError):
            # These are validation errors, just flash them
            pass
        else:
            # These are unexpected DB errors
            flash(f"Error accessing WIP batches: {e}", "error")
            print(f"DB Error WIP page: {e}")

    return render_template(
        "wip_batches.html",
        wip_batches=wip_batches,
        sellable_products=sellable_products,
        producible_items=producible_items,
        locations=locations,
    )


@bp.route("/wip/<int:batch_id>")
def wip_batch_detail(batch_id):
    conn = get_db()
    batch = None
    ingredient_summary = []
    yield_label = "Actual Yield"

    recipe_cache = {}

    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute(
                """
                SELECT 
                    w.*, r.name as recipe_name, p.product_name,
                    l.name as location_name, i.name as item_name, i.unit as item_unit
                FROM wip_batches w 
                JOIN recipes r ON w.recipe_id = r.id 
                LEFT JOIN products p ON w.product_id = p.id 
                LEFT JOIN locations l ON w.location_id = l.id
                LEFT JOIN inventory_items i ON w.inventory_item_id = i.id
                WHERE w.id = %s;
            """,
                (batch_id,),
            )
            batch = cur.fetchone()
            if not batch:
                flash(f"WIP Batch {batch_id} not found.", "error")
                return redirect(url_for("ops.wip_batches_page"))

            if batch["batch_type"] == "PRODUCT":
                yield_label = "Actual Jars Produced"
            elif batch["batch_type"] == "INTERMEDIATE":
                yield_label = f"Actual Yield ({batch['item_unit']})"

            required_ingredients = defaultdict(
                lambda: {
                    "name": "",
                    "unit": "",
                    "total_needed": 0,
                    "inventory_item_id": None,
                }
            )
            base_ingredients_one_batch = get_base_ingredients(
                batch["recipe_id"], conn, recipe_cache
            )
            batches_needed_float = 1.0

            for ing in base_ingredients_one_batch:
                inv_item_id = ing.get("inventory_item_id")
                if inv_item_id:
                    name = ing.get("name", "?").strip()
                    unit = ing.get("unit", "?").strip()
                    key = inv_item_id
                    required_ingredients[key]["name"] = name
                    required_ingredients[key]["unit"] = unit
                    required_ingredients[key]["inventory_item_id"] = inv_item_id
                    required_ingredients[key]["total_needed"] += (
                        float(ing.get("quantity", 0)) * batches_needed_float
                    )

            cur.execute(
                """SELECT inventory_item_id, SUM(quantity_allocated) as total_allocated 
                   FROM wip_allocations 
                   WHERE wip_batch_id = %s GROUP BY inventory_item_id;""",
                (batch_id,),
            )
            allocations_raw = cur.fetchall()
            current_allocations = {
                alloc["inventory_item_id"]: alloc["total_allocated"]
                for alloc in allocations_raw
            }

            # --- NEW: Get all available inventory in one go ---
            cur.execute(
                "SELECT id, (quantity_on_hand - quantity_allocated) as available FROM inventory_items;"
            )
            available_stock_raw = cur.fetchall()
            available_stock_map = {
                item["id"]: float(item["available"]) for item in available_stock_raw
            }

            for inv_id, req in required_ingredients.items():
                allocated = float(current_allocations.get(inv_id, 0))
                remaining = float(req["total_needed"]) - allocated

                # --- NEW: Find the available stock for this item ---
                available = available_stock_map.get(inv_id, 0)

                ingredient_summary.append(
                    {
                        "inventory_item_id": inv_id,
                        "name": req["name"],
                        "unit": req["unit"],
                        "needed": round(float(req["total_needed"]), 2),
                        "allocated": round(allocated, 2),
                        "remaining": round(remaining, 2),
                        "available": round(available, 2),  # --- NEWLY ADDED ---
                    }
                )
            ingredient_summary.sort(key=lambda x: x["name"])

    except psycopg2.Error as e:
        flash(f"Error fetching batch details: {e}", "error")
        print(f"DB Error WIP detail {batch_id}: {e}")
        return redirect(url_for("ops.wip_batches_page"))

    return render_template(
        "wip_batch_detail.html",
        batch=batch,
        ingredient_summary=ingredient_summary,
        yield_label=yield_label,
    )


@bp.route("/wip/<int:batch_id>/allocate-bulk", methods=["POST"])
def allocate_bulk(batch_id):
    conn = get_db()

    # 1. Parse the form into a list of tasks
    allocations_to_make = []
    for key, value in request.form.items():
        if key.startswith("alloc-"):
            try:
                item_id = int(key.split("-")[-1])
                quantity = float(value)
                if quantity > 0:
                    allocations_to_make.append({"id": item_id, "qty": quantity})
            except (ValueError, TypeError):
                flash(f"Invalid quantity '{value}' submitted.", "error")
                return redirect(url_for("ops.wip_batch_detail", batch_id=batch_id))

    if not allocations_to_make:
        flash("No quantities specified to allocate.", "warning")
        return redirect(url_for("ops.wip_batch_detail", batch_id=batch_id))

    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            # 2. Check all items for stock within a single transaction
            for task in allocations_to_make:
                cur.execute(
                    "SELECT name, (quantity_on_hand - quantity_allocated) as available FROM inventory_items WHERE id = %s FOR UPDATE;",
                    (task["id"],),
                )
                item = cur.fetchone()
                if not item:
                    raise Exception(f"Item ID {task['id']} not found.")
                if item["available"] < task["qty"]:
                    raise Exception(
                        f"Not enough stock for '{item['name']}'. Available: {item['available']}, Tried to allocate: {task['qty']}"
                    )

            # 3. If all checks passed, perform all allocations
            for task in allocations_to_make:
                # Add to allocation table
                cur.execute(
                    "INSERT INTO wip_allocations (wip_batch_id, inventory_item_id, quantity_allocated) VALUES (%s, %s, %s);",
                    (batch_id, task["id"], task["qty"]),
                )
                # Update inventory allocated quantity
                cur.execute(
                    "UPDATE inventory_items SET quantity_allocated = quantity_allocated + %s WHERE id = %s;",
                    (task["qty"], task["id"]),
                )

            conn.commit()
            flash(
                f"Successfully allocated {len(allocations_to_make)} item(s).", "success"
            )

    except Exception as e:
        conn.rollback()
        flash(f"Allocation failed: {e}", "error")
        print(f"DB Error bulk allocate batch {batch_id}: {e}")

    return redirect(url_for("ops.wip_batch_detail", batch_id=batch_id))


@bp.route("/wip/<int:batch_id>/complete", methods=["POST"])
def complete_wip_batch(batch_id):
    conn = get_db()
    actual_yield_str = request.form.get("actual_yield")

    try:
        actual_yield = float(actual_yield_str)
        if actual_yield < 0:
            raise ValueError("Yield cannot be negative")
    except (ValueError, TypeError):
        flash("Invalid yield. Please enter a valid number.", "error")
        return redirect(url_for("ops.wip_batch_detail", batch_id=batch_id))

    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute(
                "SELECT w.*, i.unit as item_unit FROM wip_batches w "
                "LEFT JOIN inventory_items i ON w.inventory_item_id = i.id "
                "WHERE w.id = %s FOR UPDATE;",
                (batch_id,),
            )
            batch = cur.fetchone()
            if not batch or batch["status"] != "In Progress":
                flash("Batch not found or already completed.", "error")
                raise Exception("Batch not found or not in progress")

            cur.execute(
                "SELECT inventory_item_id, SUM(quantity_allocated) as total_allocated FROM wip_allocations WHERE wip_batch_id = %s GROUP BY inventory_item_id;",
                (batch_id,),
            )
            allocations = cur.fetchall()
            if not allocations:
                flash("Cannot complete batch: No ingredients were allocated.", "error")
                raise Exception("No ingredients allocated")

            for alloc in allocations:
                adj_qty = -alloc["total_allocated"]
                reason = f"WIP Batch #{batch_id} Completed"
                cur.execute(
                    """UPDATE inventory_items 
                    SET quantity_on_hand = quantity_on_hand - %s, 
                    quantity_allocated = quantity_allocated - %s 
                    WHERE id = %s 
                    RETURNING quantity_on_hand;""",
                    (
                        alloc["total_allocated"],
                        alloc["total_allocated"],
                        alloc["inventory_item_id"],
                    ),
                )
                updated_qty_row = cur.fetchone()
                new_qoh = updated_qty_row[0] if updated_qty_row else 0
                _log_inventory_adjustment(
                    cur,
                    alloc["inventory_item_id"],
                    adj_qty,
                    reason,
                    new_qoh,
                    wip_batch_id=batch_id,
                )

            if batch["batch_type"] == "PRODUCT":
                cur.execute(
                    """INSERT INTO location_stock (product_id, location_id, quantity)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (product_id, location_id)
                    DO UPDATE SET quantity = location_stock.quantity + EXCLUDED.quantity;""",
                    (batch["product_id"], batch["location_id"], actual_yield),
                )
                yield_unit = "jars"
                flash_msg = f"Batch {batch_id} completed. {actual_yield} jars added to location stock."

            elif batch["batch_type"] == "INTERMEDIATE":
                reason = f"WIP Batch #{batch_id} Completed (Yield)"
                cur.execute(
                    "UPDATE inventory_items SET quantity_on_hand = quantity_on_hand + %s WHERE id = %s RETURNING quantity_on_hand;",
                    (actual_yield, batch["inventory_item_id"]),
                )
                updated_qty_row = cur.fetchone()
                new_qoh = updated_qty_row[0] if updated_qty_row else 0
                _log_inventory_adjustment(
                    cur,
                    batch["inventory_item_id"],
                    actual_yield,
                    reason,
                    new_qoh,
                    wip_batch_id=batch_id,
                )
                yield_unit = batch["item_unit"]
                flash_msg = f"Batch {batch_id} completed. {actual_yield} {yield_unit} added to raw ingredient stock."

            cur.execute(
                "UPDATE wip_batches SET status = 'Completed', completed_at = NOW(), actual_yield = %s, actual_yield_unit = %s WHERE id = %s;",
                (actual_yield, yield_unit, batch_id),
            )
        conn.commit()
        flash(flash_msg, "success")
    except Exception as e:
        if conn:
            conn.rollback()
        if "No ingredients" not in str(e):
            flash(f"DB error completing batch: {e}", "error")
        print(f"DB Error complete batch {batch_id}: {e}")
        return redirect(url_for("ops.wip_batch_detail", batch_id=batch_id))

    return redirect(url_for("ops.wip_batches_page"))


@bp.route("/wip/delete/<int:batch_id>", methods=["POST"])
def delete_wip_batch(batch_id):
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute("SELECT status FROM wip_batches WHERE id = %s;", (batch_id,))
            batch_status = cur.fetchone()
            if not batch_status:
                flash("Batch not found.", "error")
                conn.rollback()
            elif batch_status["status"] == "Completed":
                flash("Cannot delete completed batch.", "error")
                conn.rollback()
            else:
                cur.execute(
                    "SELECT inventory_item_id, SUM(quantity_allocated) as total_allocated FROM wip_allocations WHERE wip_batch_id = %s GROUP BY inventory_item_id;",
                    (batch_id,),
                )
                allocations_to_reverse = cur.fetchall()
                for alloc in allocations_to_reverse:
                    cur.execute(
                        "UPDATE inventory_items SET quantity_allocated = quantity_allocated - %s WHERE id = %s;",
                        (alloc["total_allocated"], alloc["inventory_item_id"]),
                    )

                # --- FIX: Delete child records first ---
                cur.execute(
                    "DELETE FROM wip_allocations WHERE wip_batch_id = %s;", (batch_id,)
                )
                # --- END FIX ---

                cur.execute("DELETE FROM wip_batches WHERE id = %s;", (batch_id,))
                conn.commit()
                flash(f"WIP Batch {batch_id} deleted.", "success")
    except psycopg2.Error as e:
        conn.rollback()
        flash(f"DB error deleting batch: {e}", "error")
        print(f"DB Error delete batch {batch_id}: {e}")

    return redirect(url_for("ops.wip_batches_page"))


# --- Purchase Order (PO) Routes ---
@bp.route("/purchase-orders", methods=["GET", "POST"])
def purchase_orders_page():
    conn = get_db()
    purchase_orders = []
    suppliers = []
    try:
        if request.method == "POST":
            supplier_id = request.form.get("supplier_id")
            order_date_str = request.form.get("order_date") or None
            expected_delivery_str = request.form.get("expected_delivery_date") or None

            order_date = (
                datetime.strptime(order_date_str, "%Y-%m-%d").date()
                if order_date_str
                else datetime.now().date()
            )
            expected_delivery = (
                datetime.strptime(expected_delivery_str, "%Y-%m-%d").date()
                if expected_delivery_str
                else None
            )

            if not supplier_id:
                flash("Supplier is required.", "error")
                raise ValueError("Supplier not provided")

            with conn.cursor(cursor_factory=DictCursor) as cur:
                cur.execute(
                    """INSERT INTO purchase_orders (supplier_id, order_date, expected_delivery_date, status)
                    VALUES (%s, %s, %s, %s) RETURNING id;""",
                    (supplier_id, order_date, expected_delivery, "Placed"),
                )
                new_po_id = cur.fetchone()["id"]
                conn.commit()
                flash("Purchase Order created. Now add items.", "success")
                return redirect(url_for("ops.po_detail", po_id=new_po_id))

        # GET Request Logic
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute(
                """
                SELECT po.id, s.name as supplier_name, po.order_date, po.expected_delivery_date, po.status
                FROM purchase_orders po JOIN suppliers s ON po.supplier_id = s.id
                ORDER BY po.status, po.order_date DESC, po.id DESC;
            """
            )
            purchase_orders = cur.fetchall()
            cur.execute("SELECT id, name FROM suppliers ORDER BY name;")
            suppliers = cur.fetchall()

    except (psycopg2.Error, ValueError) as e:
        if conn and request.method == "POST":
            conn.rollback()
        flash(f"Error accessing purchase orders: {e}", "error")
        print(f"DB Error PO page: {e}")
        if request.method == "POST" or not suppliers:
            try:
                if not conn or conn.closed:
                    conn = get_db()
                with conn.cursor(cursor_factory=DictCursor) as cur_err:
                    cur_err.execute("SELECT id, name FROM suppliers ORDER BY name;")
                    suppliers = cur_err.fetchall()
            except psycopg2.Error as e_inner:
                print(f"DB Error fetching suppliers for PO form: {e_inner}")

    return render_template(
        "purchase_orders.html",
        purchase_orders=purchase_orders,
        suppliers=suppliers,
        now=datetime.now(),
    )


@bp.route("/po/<int:po_id>")
def po_detail(po_id):
    conn = get_db()
    po = None
    items = []
    inventory_items = []
    total_cost = 0.0
    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute(
                """
                SELECT po.*, s.name as supplier_name
                FROM purchase_orders po JOIN suppliers s ON po.supplier_id = s.id
                WHERE po.id = %s;
            """,
                (po_id,),
            )
            po = cur.fetchone()
            if not po:
                flash(f"Purchase Order ID {po_id} not found.", "error")
                return redirect(url_for("ops.purchase_orders_page"))

            cur.execute(
                """
                SELECT poi.id, inv.name, inv.unit, poi.quantity_ordered, poi.unit_cost
                FROM purchase_order_items poi
                JOIN inventory_items inv ON poi.inventory_item_id = inv.id
                WHERE poi.purchase_order_id = %s ORDER BY inv.name;
            """,
                (po_id,),
            )
            items = cur.fetchall()
            item_subtotal = sum(
                float(item["quantity_ordered"] or 0) * float(item["unit_cost"] or 0)
                for item in items
            )
            total_cost = (
                item_subtotal + float(po["shipping_cost"] or 0) + float(po["tax"] or 0)
            ) - float(po["discount"] or 0)
            cur.execute("SELECT id, name, unit FROM inventory_items ORDER BY name;")
            inventory_items = cur.fetchall()

    except psycopg2.Error as e:
        flash(f"Error fetching PO details: {e}", "error")
        print(f"DB Error PO Detail page GET {po_id}: {e}")
        return redirect(url_for("ops.purchase_orders_page"))

    return render_template(
        "po_detail.html",
        po=po,
        items=items,
        inventory_items=inventory_items,
        item_subtotal=item_subtotal,
        total_cost=total_cost,
    )


@bp.route("/po/<int:po_id>/add-item", methods=["POST"])
def po_add_item(po_id):
    conn = get_db()
    try:
        inventory_item_id = request.form.get("inventory_item_id")
        quantity = request.form.get("quantity_ordered")
        unit_cost = request.form.get("unit_cost") or 0

        if not inventory_item_id or not quantity:
            flash("Item and quantity are required.", "error")
            raise ValueError("Missing item/qty")
        try:
            quantity_float = float(quantity)
            unit_cost_float = float(unit_cost)
            if quantity_float <= 0:
                raise ValueError("Quantity must be positive.")
        except ValueError:
            flash("Invalid quantity or unit cost.", "error")
            raise ValueError("Invalid numbers")

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO purchase_order_items (purchase_order_id, inventory_item_id, quantity_ordered, unit_cost)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (purchase_order_id, inventory_item_id)
                DO UPDATE SET
                    quantity_ordered = purchase_order_items.quantity_ordered + EXCLUDED.quantity_ordered,
                    unit_cost = EXCLUDED.unit_cost; 
            """,
                (po_id, inventory_item_id, quantity_float, unit_cost_float),
            )
        conn.commit()
        flash("Item added/updated successfully.", "success")
    except (psycopg2.Error, ValueError) as e:
        if conn:
            conn.rollback()
        flash(f"Error adding item: {e}", "error")
        print(f"DB Error PO add item {po_id}: {e}")

    return redirect(url_for("ops.po_detail", po_id=po_id))


@bp.route("/po/item/delete/<int:item_id>", methods=["POST"])
def po_remove_item(item_id):
    po_id = request.form.get("po_id")
    if not po_id:
        flash("Error: Missing Purchase Order ID.", "error")
        return redirect(url_for("ops.purchase_orders_page"))
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute("SELECT status FROM purchase_orders WHERE id = %s;", (po_id,))
            po_status_row = cur.fetchone()
            if po_status_row and po_status_row["status"] == "Received":
                flash("Cannot remove items from a received order.", "error")
            else:
                cur.execute(
                    "DELETE FROM purchase_order_items WHERE id = %s;", (item_id,)
                )
                conn.commit()
                flash("Item removed from PO.", "success")
    except psycopg2.Error as e:
        if conn:
            conn.rollback()
        flash(f"Error removing item: {e}", "error")
        print(f"DB Error PO remove item {item_id}: {e}")

    return redirect(url_for("ops.po_detail", po_id=po_id))


@bp.route("/po/<int:po_id>/update-details", methods=["POST"])
def po_update_header(po_id):
    conn = get_db()
    try:
        supplier_id = request.form.get("supplier_id")
        order_date_str = request.form.get("order_date") or None
        expected_delivery_str = request.form.get("expected_delivery_date") or None
        shipping_cost = request.form.get("shipping_cost") or 0
        tax = request.form.get("tax") or 0
        discount = request.form.get("discount") or 0
        notes = request.form.get("notes")
        new_status = request.form.get("status")

        order_date = (
            datetime.strptime(order_date_str, "%Y-%m-%d").date()
            if order_date_str
            else None
        )
        expected_delivery = (
            datetime.strptime(expected_delivery_str, "%Y-%m-%d").date()
            if expected_delivery_str
            else None
        )

        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute("SELECT status FROM purchase_orders WHERE id = %s;", (po_id,))
            current_status_row = cur.fetchone()
            if not current_status_row:
                flash("PO not found.", "error")
                raise Exception("PO not found")
            current_status = current_status_row["status"]

            cur.execute(
                """
                UPDATE purchase_orders
                SET supplier_id = %s, order_date = %s, expected_delivery_date = %s,
                    shipping_cost = %s, tax = %s, discount = %s, notes = %s, status = %s
                WHERE id = %s;
            """,
                (
                    supplier_id,
                    order_date,
                    expected_delivery,
                    shipping_cost,
                    tax,
                    discount,
                    notes,
                    new_status,
                    po_id,
                ),
            )

            if new_status == "Received" and current_status != "Received":
                cur.execute(
                    "SELECT inventory_item_id, quantity_ordered FROM purchase_order_items WHERE purchase_order_id = %s;",
                    (po_id,),
                )
                items_to_receive = cur.fetchall()
                if not items_to_receive:
                    flash("Cannot mark empty order as Received.", "warning")
                    conn.rollback()
                    return redirect(url_for("ops.po_detail", po_id=po_id))

                for item in items_to_receive:
                    adj_qty = item["quantity_ordered"]
                    reason = f"PO #{po_id} Received"
                    cur.execute(
                        "UPDATE inventory_items SET quantity_on_hand = quantity_on_hand + %s WHERE id = %s RETURNING quantity_on_hand;",
                        (adj_qty, item["inventory_item_id"]),
                    )
                    updated_qty_row = cur.fetchone()
                    new_qoh = updated_qty_row[0] if updated_qty_row else 0
                    _log_inventory_adjustment(
                        cur,
                        item["inventory_item_id"],
                        adj_qty,
                        reason,
                        new_qoh,
                        po_id=po_id,
                    )
                cur.execute(
                    "UPDATE purchase_orders SET received_at = NOW() WHERE id = %s;",
                    (po_id,),
                )
                flash("Order marked as Received. Inventory updated.", "success")

            elif new_status != "Received" and current_status == "Received":
                cur.execute(
                    "SELECT inventory_item_id, quantity_ordered FROM purchase_order_items WHERE purchase_order_id = %s;",
                    (po_id,),
                )
                items_to_unreceive = cur.fetchall()
                for item in items_to_unreceive:
                    adj_qty = -item["quantity_ordered"]
                    reason = f"PO #{po_id} Status Reverted (Un-Received)"
                    cur.execute(
                        "UPDATE inventory_items SET quantity_on_hand = quantity_on_hand - %s WHERE id = %s RETURNING quantity_on_hand;",
                        (item["quantity_ordered"], item["inventory_item_id"]),
                    )
                    updated_qty_row = cur.fetchone()
                    new_qoh = updated_qty_row[0] if updated_qty_row else 0
                    _log_inventory_adjustment(
                        cur,
                        item["inventory_item_id"],
                        adj_qty,
                        reason,
                        new_qoh,
                        po_id=po_id,
                    )
                cur.execute(
                    "UPDATE purchase_orders SET received_at = NULL WHERE id = %s;",
                    (po_id,),
                )
                flash(
                    f"Order status changed from Received. Inventory reversed.",
                    "warning",
                )
            else:
                flash("PO details updated.", "success")
        conn.commit()

    except Exception as e:
        if conn:
            conn.rollback()
        flash(f"Database error updating PO: {e}", "error")
        print(f"DB Error PO update {po_id}: {e}")

    return redirect(url_for("ops.po_detail", po_id=po_id))


@bp.route("/po/delete/<int:po_id>", methods=["POST"])
def po_delete(po_id):
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute(
                "SELECT status, received_at FROM purchase_orders WHERE id = %s;",
                (po_id,),
            )
            po = cur.fetchone()
            if not po:
                flash("PO not found.", "error")
                conn.rollback()
            else:
                if po["status"] == "Received" or po["received_at"] is not None:
                    cur.execute(
                        "SELECT inventory_item_id, quantity_ordered FROM purchase_order_items WHERE purchase_order_id = %s;",
                        (po_id,),
                    )
                    items_to_unreceive = cur.fetchall()
                    for item in items_to_unreceive:
                        adj_qty = -item["quantity_ordered"]
                        reason = f"PO #{po_id} Deleted (Reversal)"
                        cur.execute(
                            "UPDATE inventory_items SET quantity_on_hand = quantity_on_hand - %s WHERE id = %s RETURNING quantity_on_hand;",
                            (item["quantity_ordered"], item["inventory_item_id"]),
                        )
                        updated_qty_row = cur.fetchone()
                        new_qoh = updated_qty_row[0] if updated_qty_row else 0
                        _log_inventory_adjustment(
                            cur,
                            item["inventory_item_id"],
                            adj_qty,
                            reason,
                            new_qoh,
                            po_id=po_id,
                        )
                    flash_msg = "PO deleted. Inventory updates have been reversed."
                else:
                    flash_msg = "PO deleted."

                # --- FIX: Delete child records first ---
                cur.execute(
                    "DELETE FROM purchase_order_items WHERE purchase_order_id = %s;",
                    (po_id,),
                )
                # --- END FIX ---

                cur.execute("DELETE FROM purchase_orders WHERE id = %s;", (po_id,))
                conn.commit()
                flash(flash_msg, "success")
    except psycopg2.Error as e:
        if conn:
            conn.rollback()
        flash(f"Error deleting PO: {e.diag.message_primary}", "error")
        print(f"DB Error PO delete {po_id}: {e}")

    return redirect(url_for("ops.purchase_orders_page"))
