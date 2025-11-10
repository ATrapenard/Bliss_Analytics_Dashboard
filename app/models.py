import psycopg2
from psycopg2.extras import DictCursor
from collections import defaultdict
import math

# --- RECURSIVE LOGIC (Refactored with request-local cache) ---


def get_base_ingredients(recipe_id, conn, cache=None):
    """
    Recursively finds all base ingredients for a given recipe.
    A 'cache' dict must be passed in to prevent re-calculation
    during a single request.
    """
    if cache is None:
        cache = {}  # Initialize cache if this is the first call

    if recipe_id in cache:
        return [ing.copy() for ing in cache[recipe_id]]

    with conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute(
            """
            SELECT i.*, inv.id as inventory_item_id, inv.name as inv_name, inv.unit as inv_unit
            FROM ingredients i LEFT JOIN inventory_items inv ON i.inventory_item_id = inv.id
            WHERE i.recipe_id = %s; """,
            (recipe_id,),
        )
        ingredients = cur.fetchall()
        base_ingredients = []

        for ing in ingredients:
            if ing["sub_recipe_id"]:
                # --- THIS IS THE UPDATED LOGIC ---
                cur.execute(
                    "SELECT yield_quantity, yield_unit FROM recipes WHERE id = %s;",
                    (ing["sub_recipe_id"],),
                )
                sub_recipe_yield_row = cur.fetchone()

                scaling_ratio = 1.0
                if sub_recipe_yield_row:
                    yield_qty = sub_recipe_yield_row.get("yield_quantity")
                    yield_unit = sub_recipe_yield_row.get("yield_unit")

                    if yield_unit in ("grams", "mLs"):
                        if yield_qty and float(yield_qty) != 0:
                            scaling_ratio = float(ing["quantity"]) / float(yield_qty)
                    elif yield_unit == "batches":
                        scaling_ratio = float(ing["quantity"])

                # --- Pass the cache during recursion ---
                sub_ingredients = get_base_ingredients(
                    ing["sub_recipe_id"], conn, cache
                )

                for sub_ing in sub_ingredients:
                    scaled_ing = dict(sub_ing)
                    scaled_ing["quantity"] = (
                        float(scaled_ing.get("quantity", 0)) * scaling_ratio
                    )
                    base_ingredients.append(scaled_ing)

            elif ing["inventory_item_id"]:
                raw_ing = {
                    "inventory_item_id": ing["inventory_item_id"],
                    "name": ing["inv_name"],
                    "unit": ing["inv_unit"],
                    "quantity": float(ing.get("quantity", 0)),
                }
                base_ingredients.append(raw_ing)

    cache[recipe_id] = [ing.copy() for ing in base_ingredients]
    return base_ingredients


# --- HELPER FUNCTION ---
def _log_inventory_adjustment(
    cur,
    inventory_item_id,
    adjustment_quantity,
    reason,
    new_quantity_on_hand,
    po_id=None,
    wip_batch_id=None,
):
    """
    Helper function to insert a record into the inventory_adjustments log.
    Assumes the inventory_items table has *already* been updated.
    """
    try:
        cur.execute(
            """INSERT INTO inventory_adjustments 
            (inventory_item_id, adjustment_quantity, new_quantity, reason, purchase_order_id, wip_batch_id) 
            VALUES (%s, %s, %s, %s, %s, %s);""",
            (
                inventory_item_id,
                adjustment_quantity,
                new_quantity_on_hand,
                reason,
                po_id,
                wip_batch_id,
            ),
        )
    except Exception as e:
        print(f"CRITICAL: Failed to log inventory adjustment: {e}")
