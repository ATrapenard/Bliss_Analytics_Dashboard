import os
import psycopg2
from psycopg2.extras import DictCursor
from dotenv import load_dotenv
from flask import Flask, render_template, request, redirect, url_for
from collections import defaultdict
import math

load_dotenv()
app = Flask(__name__)

def get_db_connection():
    conn_string = os.getenv("DATABASE_URL")
    conn = psycopg2.connect(conn_string)
    return conn

# --- RECURSIVE LOGIC ---
resolved_cache = {}
def get_base_ingredients(recipe_id, conn):
    if recipe_id in resolved_cache:
        return resolved_cache[recipe_id]
    with conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute("SELECT * FROM ingredients WHERE recipe_id = %s;", (recipe_id,))
        ingredients = cur.fetchall()
        base_ingredients = []
        for ing in ingredients:
            if ing['sub_recipe_id']:
                cur.execute("SELECT yield_quantity FROM recipes WHERE id = %s;", (ing['sub_recipe_id'],))
                sub_recipe_yield_row = cur.fetchone()
                if sub_recipe_yield_row and sub_recipe_yield_row['yield_quantity'] and float(sub_recipe_yield_row['yield_quantity']) != 0:
                    sub_recipe_yield = sub_recipe_yield_row['yield_quantity']
                    scaling_ratio = float(ing['quantity']) / float(sub_recipe_yield)
                else:
                    scaling_ratio = 1.0
                sub_ingredients = get_base_ingredients(ing['sub_recipe_id'], conn)
                for sub_ing in sub_ingredients:
                    scaled_ing = dict(sub_ing)
                    scaled_ing['quantity'] = float(scaled_ing['quantity']) * scaling_ratio
                    base_ingredients.append(scaled_ing)
            else:
                raw_ing = dict(ing)
                raw_ing['quantity'] = float(raw_ing['quantity'])
                base_ingredients.append(raw_ing)
    resolved_cache[recipe_id] = base_ingredients
    return base_ingredients

# --- Standard Routes ---
@app.route('/')
def home():
    return render_template('index.html')

@app.route('/recipes')
def recipe_dashboard():
    recipes_list = []
    conn = get_db_connection()
    resolved_cache.clear()
    with conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute('SELECT * FROM recipes ORDER BY name;')
        recipes_from_db = cur.fetchall()
        for recipe in recipes_from_db:
            recipe_dict = dict(recipe)
            cur.execute('SELECT name, quantity, unit FROM ingredients WHERE recipe_id = %s;', (recipe['id'],))
            recipe_dict['ingredients'] = [dict(ing) for ing in cur.fetchall()]
            base_ingredients_for_totals = get_base_ingredients(recipe['id'], conn)
            totals = {'grams': 0, 'mLs': 0}
            for ing in base_ingredients_for_totals:
                unit = ing.get('unit', '').lower()
                quantity = float(ing.get('quantity', 0))
                if unit == 'grams':
                    totals['grams'] += quantity
                elif unit == 'mls':
                    totals['mLs'] += quantity
            recipe_dict['totals'] = {
                'grams': round(totals['grams'], 2),
                'mLs': round(totals['mLs'], 2)
            }
            recipes_list.append(recipe_dict)
    conn.close()
    return render_template('recipes.html', recipes=recipes_list)

@app.route('/new')
def new_recipe_form():
    conn = get_db_connection()
    with conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute("SELECT id, name FROM recipes ORDER BY name;")
        recipes = cur.fetchall()
    conn.close()
    return render_template('add_recipe.html', recipes=recipes)

@app.route('/create', methods=['POST'])
def create_recipe():
    conn = get_db_connection()
    with conn.cursor() as cur:
        recipe_name = request.form['recipe_name']
        yield_quantity = request.form.get('yield_quantity') or None
        yield_unit = request.form.get('yield_unit') or None
        is_sold_product = 'is_sold_product' in request.form
        cur.execute('INSERT INTO recipes (name, yield_quantity, yield_unit, is_sold_product) VALUES (%s, %s, %s, %s) RETURNING id;',
                    (recipe_name, yield_quantity, yield_unit, is_sold_product))
        recipe_id = cur.fetchone()[0]
        ingredient_names = request.form.getlist('ingredient_name')
        quantities = request.form.getlist('quantity')
        units = request.form.getlist('unit')
        sub_recipe_ids = request.form.getlist('sub_recipe_id')
        for i in range(len(ingredient_names)):
            sub_recipe_id = sub_recipe_ids[i] if sub_recipe_ids[i] else None
            try: quantity_val = float(quantities[i]) if quantities[i] else 0
            except ValueError: quantity_val = 0
            cur.execute(
                'INSERT INTO ingredients (recipe_id, name, quantity, unit, sub_recipe_id) VALUES (%s, %s, %s, %s, %s);',
                (recipe_id, ingredient_names[i], quantity_val, units[i], sub_recipe_id)
            )
    conn.commit()
    conn.close()
    return redirect(url_for('recipe_dashboard'))

@app.route('/edit/<int:recipe_id>')
def edit_recipe_form(recipe_id):
    conn = get_db_connection()
    with conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute('SELECT * FROM recipes WHERE id = %s;', (recipe_id,))
        recipe = cur.fetchone()
        cur.execute('SELECT * FROM ingredients WHERE recipe_id = %s;', (recipe_id,))
        ingredients = cur.fetchall()
        cur.execute("SELECT id, name FROM recipes ORDER BY name;")
        all_recipes = cur.fetchall()
    conn.close()
    return render_template('edit_recipe.html', recipe=recipe, ingredients=ingredients, recipes=all_recipes)

@app.route('/update/<int:recipe_id>', methods=['POST'])
def update_recipe(recipe_id):
    conn = get_db_connection()
    with conn.cursor() as cur:
        new_name = request.form['recipe_name']
        yield_quantity = request.form.get('yield_quantity') or None
        yield_unit = request.form.get('yield_unit') or None
        is_sold_product = 'is_sold_product' in request.form
        cur.execute('UPDATE recipes SET name = %s, yield_quantity = %s, yield_unit = %s, is_sold_product = %s WHERE id = %s;',
                    (new_name, yield_quantity, yield_unit, is_sold_product, recipe_id))
        cur.execute('DELETE FROM ingredients WHERE recipe_id = %s;', (recipe_id,))
        ingredient_names = request.form.getlist('ingredient_name')
        quantities = request.form.getlist('quantity')
        units = request.form.getlist('unit')
        sub_recipe_ids = request.form.getlist('sub_recipe_id')
        for i in range(len(ingredient_names)):
            sub_recipe_id = sub_recipe_ids[i] if sub_recipe_ids[i] else None
            try: quantity_val = float(quantities[i]) if quantities[i] else 0
            except ValueError: quantity_val = 0
            cur.execute(
                'INSERT INTO ingredients (recipe_id, name, quantity, unit, sub_recipe_id) VALUES (%s, %s, %s, %s, %s);',
                (recipe_id, ingredient_names[i], quantity_val, units[i], sub_recipe_id)
            )
    conn.commit()
    conn.close()
    return redirect(url_for('recipe_dashboard'))

@app.route('/delete/<int:recipe_id>', methods=['POST'])
def delete_recipe(recipe_id):
    conn = get_db_connection()
    with conn.cursor() as cur:
        cur.execute('DELETE FROM recipes WHERE id = %s;', (recipe_id,))
    conn.commit()
    conn.close()
    return redirect(url_for('recipe_dashboard'))

@app.route('/totals')
def ingredient_totals():
    conn = get_db_connection()
    resolved_cache.clear()
    all_base_ingredients = []
    with conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute("SELECT id FROM recipes;")
        all_recipe_ids = cur.fetchall()
        for rec_id in all_recipe_ids:
            all_base_ingredients.extend(get_base_ingredients(rec_id['id'], conn))
    totals = defaultdict(lambda: {'name': '', 'unit': '', 'total_quantity': 0})
    for ing in all_base_ingredients:
        name = ing.get('name', 'Unknown').strip()
        unit = ing.get('unit', 'Unknown').strip()
        key = (name.lower(), unit.lower())
        totals[key]['name'] = name
        totals[key]['unit'] = unit
        totals[key]['total_quantity'] += float(ing.get('quantity', 0))
    conn.close()
    sorted_totals = sorted(totals.values(), key=lambda x: x['name'])
    return render_template('totals.html', totals=sorted_totals)

@app.route('/products')
def products_page():
    conn = get_db_connection()
    with conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute("""
            SELECT p.id, p.sku, p.jars_per_batch, r.name as recipe_name
            FROM products p LEFT JOIN recipes r ON p.recipe_id = r.id ORDER BY p.sku;""")
        products = cur.fetchall()
        cur.execute("SELECT id, name FROM recipes WHERE is_sold_product = TRUE ORDER BY name;")
        recipes = cur.fetchall()
    conn.close()
    return render_template('products.html', products=products, recipes=recipes)

@app.route('/products/add', methods=['POST'])
def add_product():
    sku = request.form['sku']
    recipe_id = request.form['recipe_id']
    jars_per_batch = request.form.get('jars_per_batch') or None
    conn = get_db_connection()
    with conn.cursor() as cur:
        cur.execute('INSERT INTO products (sku, recipe_id, jars_per_batch) VALUES (%s, %s, %s);', (sku, recipe_id, jars_per_batch))
    conn.commit()
    conn.close()
    return redirect(url_for('products_page'))

@app.route('/products/edit/<int:id>')
def edit_product(id):
    conn = get_db_connection()
    with conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute("SELECT * FROM products WHERE id = %s;", (id,))
        product = cur.fetchone()
        cur.execute("SELECT id, name FROM recipes WHERE is_sold_product = TRUE ORDER BY name;")
        recipes = cur.fetchall()
    conn.close()
    return render_template('edit_product.html', product=product, recipes=recipes)

@app.route('/products/update/<int:id>', methods=['POST'])
def update_product(id):
    sku = request.form['sku']
    recipe_id = request.form['recipe_id']
    jars_per_batch = request.form.get('jars_per_batch') or None
    conn = get_db_connection()
    with conn.cursor() as cur:
        cur.execute("UPDATE products SET sku = %s, recipe_id = %s, jars_per_batch = %s WHERE id = %s;", (sku, recipe_id, jars_per_batch, id))
    conn.commit()
    conn.close()
    return redirect(url_for('products_page'))

@app.route('/products/delete/<int:id>', methods=['POST'])
def delete_product(id):
    conn = get_db_connection()
    with conn.cursor() as cur:
        cur.execute("DELETE FROM products WHERE id = %s;", (id,))
    conn.commit()
    conn.close()
    return redirect(url_for('products_page'))

@app.route('/locations', methods=['GET', 'POST'])
def locations_page():
    conn = get_db_connection()
    if request.method == 'POST':
        location_name = request.form['name']
        with conn.cursor() as cur:
            cur.execute("INSERT INTO locations (name) VALUES (%s);", (location_name,))
        conn.commit()
        conn.close()
        return redirect(url_for('locations_page'))
    with conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute("SELECT * FROM locations ORDER BY name;")
        locations = cur.fetchall()
    conn.close()
    return render_template('locations.html', locations=locations)

@app.route('/locations/delete/<int:id>', methods=['POST'])
def delete_location(id):
    conn = get_db_connection()
    with conn.cursor() as cur:
        cur.execute("DELETE FROM locations WHERE id = %s;", (id,))
    conn.commit()
    conn.close()
    return redirect(url_for('locations_page'))

@app.route('/stock-minimums', methods=['GET', 'POST'])
def stock_minimums_page():
    conn = get_db_connection()
    if request.method == 'POST':
        location_id = request.form['location_id']
        product_id = request.form['product_id']
        min_jars = request.form['min_jars']
        with conn.cursor() as cur:
            cur.execute("""INSERT INTO stock_minimums (location_id, product_id, min_jars) VALUES (%s, %s, %s) ON CONFLICT (product_id, location_id) DO UPDATE SET min_jars = EXCLUDED.min_jars;""", (location_id, product_id, min_jars))
        conn.commit()
        conn.close()
        return redirect(url_for('stock_minimums_page'))
    with conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute("SELECT * FROM locations ORDER BY name;")
        locations = cur.fetchall()
        cur.execute("""SELECT p.id, p.sku, r.name as recipe_name FROM products p JOIN recipes r ON p.recipe_id = r.id WHERE r.is_sold_product = TRUE ORDER BY p.sku;""")
        products = cur.fetchall()
        cur.execute("""SELECT sm.id, l.name as location_name, p.sku, r.name as recipe_name, sm.min_jars FROM stock_minimums sm JOIN locations l ON sm.location_id = l.id JOIN products p ON sm.product_id = p.id JOIN recipes r ON p.recipe_id = r.id ORDER BY l.name, p.sku;""")
        minimums = cur.fetchall()
    conn.close()
    return render_template('stock_minimums.html', locations=locations, products=products, minimums=minimums)

@app.route('/stock-minimums/delete/<int:id>', methods=['POST'])
def delete_stock_minimum(id):
    conn = get_db_connection()
    with conn.cursor() as cur:
        cur.execute("DELETE FROM stock_minimums WHERE id = %s;", (id,))
    conn.commit()
    conn.close()
    return redirect(url_for('stock_minimums_page'))

@app.route('/requirements')
def requirements_page():
    conn = get_db_connection()
    resolved_cache.clear()
    all_base_ingredients = []
    with conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute("""SELECT p.recipe_id, p.jars_per_batch, SUM(sm.min_jars) as total_jars FROM stock_minimums sm JOIN products p ON sm.product_id = p.id JOIN recipes r ON p.recipe_id = r.id WHERE r.is_sold_product = TRUE AND p.jars_per_batch IS NOT NULL AND p.jars_per_batch > 0 GROUP BY p.recipe_id, p.jars_per_batch;""")
        products_to_make = cur.fetchall()
        for prod in products_to_make:
            batches_needed = math.ceil(float(prod['total_jars']) / float(prod['jars_per_batch']))
            base_ingredients_one_batch = get_base_ingredients(prod['recipe_id'], conn)
            for ing in base_ingredients_one_batch:
                scaled_ing = dict(ing)
                scaled_ing['quantity'] = float(scaled_ing['quantity']) * batches_needed
                all_base_ingredients.append(scaled_ing)
    totals = defaultdict(lambda: {'name': '', 'unit': '', 'total_quantity': 0})
    for ing in all_base_ingredients:
        name = ing.get('name', 'Unknown').strip()
        unit = ing.get('unit', 'Unknown').strip()
        key = (name.lower(), unit.lower())
        totals[key]['name'] = name
        totals[key]['unit'] = unit
        totals[key]['total_quantity'] += float(ing.get('quantity', 0))
    conn.close()
    sorted_totals = sorted(totals.values(), key=lambda x: x['name'])
    return render_template('requirements.html', totals=sorted_totals)

@app.route('/planner', methods=['GET', 'POST'])
def production_planner():
    conn = get_db_connection()
    resolved_cache.clear()
    calculated_requirements = None
    with conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute("""SELECT p.id, p.sku, p.jars_per_batch, r.id as recipe_id, r.name as recipe_name FROM products p JOIN recipes r ON p.recipe_id = r.id WHERE r.is_sold_product = TRUE AND p.jars_per_batch IS NOT NULL AND p.jars_per_batch > 0 ORDER BY r.name;""")
        sellable_products = cur.fetchall()
    if request.method == 'POST':
        all_base_ingredients_run = []
        for product in sellable_products:
            jars_to_make_str = request.form.get(f"jars_product_{product['id']}")
            try: jars_to_make = int(jars_to_make_str) if jars_to_make_str else 0
            except ValueError: jars_to_make = 0
            if jars_to_make > 0:
                batches_needed = math.ceil(jars_to_make / float(product['jars_per_batch']))
                base_ingredients_one_batch = get_base_ingredients(product['recipe_id'], conn)
                for ing in base_ingredients_one_batch:
                    scaled_ing = dict(ing)
                    scaled_ing['quantity'] = float(scaled_ing['quantity']) * batches_needed
                    all_base_ingredients_run.append(scaled_ing)
        totals_run = defaultdict(lambda: {'name': '', 'unit': '', 'total_quantity': 0})
        for ing in all_base_ingredients_run:
            name = ing.get('name', 'Unknown').strip()
            unit = ing.get('unit', 'Unknown').strip()
            key = (name.lower(), unit.lower())
            totals_run[key]['name'] = name
            totals_run[key]['unit'] = unit
            totals_run[key]['total_quantity'] += float(ing.get('quantity', 0))
        calculated_requirements = sorted(totals_run.values(), key=lambda x: x['name'])
    conn.close()
    return render_template('planner.html', products=sellable_products, requirements=calculated_requirements)

# --- NEW INVENTORY ITEMS ROUTES ---
@app.route('/inventory', methods=['GET', 'POST'])
def inventory_items_page():
    conn = get_db_connection()
    if request.method == 'POST':
        name = request.form['name']
        unit = request.form['unit']
        # Optionally handle initial quantity on hand
        qty_on_hand_str = request.form.get('quantity_on_hand')
        try:
            qty_on_hand = float(qty_on_hand_str) if qty_on_hand_str else 0.0
        except ValueError:
            qty_on_hand = 0.0 # Default to 0 if input is invalid

        with conn.cursor() as cur:
            # Check for existing item with the same name and unit (case-insensitive)
            cur.execute("SELECT id FROM inventory_items WHERE LOWER(name) = LOWER(%s) AND LOWER(unit) = LOWER(%s);", (name, unit))
            existing = cur.fetchone()
            if not existing: # Only insert if it doesn't exist
                 cur.execute("INSERT INTO inventory_items (name, unit, quantity_on_hand) VALUES (%s, %s, %s);",
                             (name, unit, qty_on_hand))
                 conn.commit()
            # We might want to add a flash message here if the item already exists
    # GET Request or after POST redirect
    with conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute("SELECT * FROM inventory_items ORDER BY name;")
        inventory_items = cur.fetchall()
    conn.close()
    return render_template('inventory_items.html', inventory_items=inventory_items)

@app.route('/inventory/edit/<int:id>', methods=['GET'])
def edit_inventory_item(id):
    conn = get_db_connection()
    with conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute("SELECT * FROM inventory_items WHERE id = %s;", (id,))
        item = cur.fetchone()
    conn.close()
    if item is None:
        # Handle item not found, maybe redirect or show error
        return redirect(url_for('inventory_items_page'))
    return render_template('edit_inventory_item.html', item=item)

@app.route('/inventory/update/<int:id>', methods=['POST'])
def update_inventory_item(id):
    name = request.form['name']
    unit = request.form['unit']
    qty_on_hand_str = request.form.get('quantity_on_hand')
    try:
        qty_on_hand = float(qty_on_hand_str) if qty_on_hand_str else 0.0
    except ValueError:
        # How to handle invalid input? Keep old value or set to 0? Let's keep old for now.
        # Fetch old value first to keep it if new one is invalid.
        # This part requires a bit more logic if strict validation is needed.
        # For simplicity now, we might just default to 0 or rely on DB constraints.
        # Let's assume we want to update even if invalid, setting to 0.
        qty_on_hand = 0.0


    conn = get_db_connection()
    with conn.cursor() as cur:
        # Update name, unit, and quantity on hand. Allocation is handled elsewhere.
        cur.execute("UPDATE inventory_items SET name = %s, unit = %s, quantity_on_hand = %s WHERE id = %s;",
                     (name, unit, qty_on_hand, id))
    conn.commit()
    conn.close()
    return redirect(url_for('inventory_items_page'))

@app.route('/inventory/delete/<int:id>', methods=['POST'])
def delete_inventory_item(id):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # Attempt to delete. Will fail if item is allocated (due to FK constraint)
            cur.execute("DELETE FROM inventory_items WHERE id = %s;", (id,))
        conn.commit()
        # Add flash message for success?
    except psycopg2.Error as e:
        conn.rollback() # Important: Rollback the transaction on error
        # Add flash message indicating failure (e.g., "Cannot delete item: it is currently allocated.")
        print(f"Error deleting inventory item {id}: {e}") # Log the error
    finally:
        conn.close()
    return redirect(url_for('inventory_items_page'))
# --- END NEW ROUTES ---


if __name__ == '__main__':
    app.run(debug=True)