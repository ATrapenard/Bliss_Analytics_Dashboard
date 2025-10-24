import os
import psycopg2
from psycopg2.extras import DictCursor
from dotenv import load_dotenv
from flask import Flask, render_template, request, redirect, url_for, flash # Added flash
from collections import defaultdict
import math
from datetime import datetime # Added datetime

load_dotenv()
app = Flask(__name__)
# Secret key needed for flash messages
app.config['SECRET_KEY'] = os.getenv('SECRET_KEY', 'a_default_secret_key_for_development')


def get_db_connection():
    conn_string = os.getenv("DATABASE_URL")
    conn = psycopg2.connect(conn_string)
    return conn

# --- RECURSIVE LOGIC ---
resolved_cache = {}
def get_base_ingredients(recipe_id, conn):
    # (Function unchanged - See previous versions for full code)
    if recipe_id in resolved_cache:
        return resolved_cache[recipe_id]
    with conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute("SELECT i.*, inv.id as inventory_item_id FROM ingredients i LEFT JOIN inventory_items inv ON LOWER(i.name) = LOWER(inv.name) AND LOWER(i.unit) = LOWER(inv.unit) WHERE i.recipe_id = %s;", (recipe_id,))
        ingredients = cur.fetchall()
        base_ingredients = []
        for ing in ingredients:
            if ing['sub_recipe_id']:
                cur.execute("SELECT yield_quantity FROM recipes WHERE id = %s;", (ing['sub_recipe_id'],))
                sub_recipe_yield_row = cur.fetchone()
                if sub_recipe_yield_row and sub_recipe_yield_row['yield_quantity'] and float(sub_recipe_yield_row['yield_quantity']) != 0:
                    scaling_ratio = float(ing['quantity']) / float(sub_recipe_yield_row['yield_quantity'])
                else: scaling_ratio = 1.0
                sub_ingredients = get_base_ingredients(ing['sub_recipe_id'], conn)
                for sub_ing in sub_ingredients:
                    scaled_ing = dict(sub_ing)
                    scaled_ing['quantity'] = float(scaled_ing['quantity']) * scaling_ratio
                    base_ingredients.append(scaled_ing)
            else:
                raw_ing = dict(ing)
                raw_ing['quantity'] = float(raw_ing['quantity'])
                 # Try to find matching inventory item ID based on name/unit if not directly linked yet
                if not raw_ing.get('inventory_item_id'):
                     cur.execute("SELECT id FROM inventory_items WHERE LOWER(name) = LOWER(%s) AND LOWER(unit) = LOWER(%s);", (raw_ing['name'], raw_ing['unit']))
                     match = cur.fetchone()
                     if match:
                         raw_ing['inventory_item_id'] = match['id']

                base_ingredients.append(raw_ing)
    resolved_cache[recipe_id] = base_ingredients
    return base_ingredients


# --- Standard Routes (Recipes, Products, Locations, etc.) ---
# --- (These routes remain unchanged - see previous versions) ---
@app.route('/')
def home(): return render_template('index.html')
@app.route('/recipes')
def recipe_dashboard():
    # ... (Code unchanged)
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
                if unit == 'grams': totals['grams'] += quantity
                elif unit == 'mls': totals['mLs'] += quantity # Corrected 'mls' if needed
            recipe_dict['totals'] = { 'grams': round(totals['grams'], 2), 'mLs': round(totals['mLs'], 2) }
            recipes_list.append(recipe_dict)
    conn.close()
    return render_template('recipes.html', recipes=recipes_list)

@app.route('/new')
def new_recipe_form():
    conn = get_db_connection()
    with conn.cursor(cursor_factory=DictCursor) as cur: cur.execute("SELECT id, name FROM recipes ORDER BY name;")
    recipes = cur.fetchall(); conn.close(); return render_template('add_recipe.html', recipes=recipes)
@app.route('/create', methods=['POST'])
def create_recipe():
    conn = get_db_connection(); # ... (Code unchanged)
    with conn.cursor() as cur:
        recipe_name=request.form['recipe_name']; yield_quantity=request.form.get('yield_quantity') or None; yield_unit=request.form.get('yield_unit') or None; is_sold_product='is_sold_product' in request.form
        cur.execute('INSERT INTO recipes (name, yield_quantity, yield_unit, is_sold_product) VALUES (%s, %s, %s, %s) RETURNING id;',(recipe_name,yield_quantity,yield_unit,is_sold_product)); recipe_id=cur.fetchone()[0]
        ingredient_names=request.form.getlist('ingredient_name'); quantities=request.form.getlist('quantity'); units=request.form.getlist('unit'); sub_recipe_ids=request.form.getlist('sub_recipe_id')
        for i in range(len(ingredient_names)):
            sub_recipe_id = sub_recipe_ids[i] if sub_recipe_ids[i] else None
            try: quantity_val = float(quantities[i]) if quantities[i] else 0
            except ValueError: quantity_val = 0
            cur.execute('INSERT INTO ingredients (recipe_id, name, quantity, unit, sub_recipe_id) VALUES (%s, %s, %s, %s, %s);',(recipe_id,ingredient_names[i],quantity_val,units[i],sub_recipe_id))
    conn.commit(); conn.close(); return redirect(url_for('recipe_dashboard'))
@app.route('/edit/<int:recipe_id>')
def edit_recipe_form(recipe_id):
    conn = get_db_connection(); # ... (Code unchanged)
    with conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute('SELECT * FROM recipes WHERE id = %s;', (recipe_id,)); recipe = cur.fetchone()
        cur.execute('SELECT * FROM ingredients WHERE recipe_id = %s;', (recipe_id,)); ingredients = cur.fetchall()
        cur.execute("SELECT id, name FROM recipes ORDER BY name;"); all_recipes = cur.fetchall()
    conn.close(); return render_template('edit_recipe.html', recipe=recipe, ingredients=ingredients, recipes=all_recipes)
@app.route('/update/<int:recipe_id>', methods=['POST'])
def update_recipe(recipe_id):
    conn = get_db_connection(); # ... (Code unchanged)
    with conn.cursor() as cur:
        new_name=request.form['recipe_name']; yield_quantity=request.form.get('yield_quantity') or None; yield_unit=request.form.get('yield_unit') or None; is_sold_product='is_sold_product' in request.form
        cur.execute('UPDATE recipes SET name = %s, yield_quantity = %s, yield_unit = %s, is_sold_product = %s WHERE id = %s;',(new_name,yield_quantity,yield_unit,is_sold_product,recipe_id))
        cur.execute('DELETE FROM ingredients WHERE recipe_id = %s;', (recipe_id,))
        ingredient_names=request.form.getlist('ingredient_name'); quantities=request.form.getlist('quantity'); units=request.form.getlist('unit'); sub_recipe_ids=request.form.getlist('sub_recipe_id')
        for i in range(len(ingredient_names)):
            sub_recipe_id = sub_recipe_ids[i] if sub_recipe_ids[i] else None
            try: quantity_val = float(quantities[i]) if quantities[i] else 0
            except ValueError: quantity_val = 0
            cur.execute('INSERT INTO ingredients (recipe_id, name, quantity, unit, sub_recipe_id) VALUES (%s, %s, %s, %s, %s);',(recipe_id,ingredient_names[i],quantity_val,units[i],sub_recipe_id))
    conn.commit(); conn.close(); return redirect(url_for('recipe_dashboard'))
@app.route('/delete/<int:recipe_id>', methods=['POST'])
def delete_recipe(recipe_id):
    conn = get_db_connection(); # ... (Code unchanged)
    with conn.cursor() as cur: cur.execute('DELETE FROM recipes WHERE id = %s;', (recipe_id,)); conn.commit(); conn.close(); return redirect(url_for('recipe_dashboard'))
@app.route('/totals')
def ingredient_totals():
    conn = get_db_connection(); # ... (Code unchanged)
    resolved_cache.clear(); all_base_ingredients = []
    with conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute("SELECT id FROM recipes;"); all_recipe_ids = cur.fetchall()
        for rec_id in all_recipe_ids: all_base_ingredients.extend(get_base_ingredients(rec_id['id'], conn))
    totals = defaultdict(lambda: {'name': '', 'unit': '', 'total_quantity': 0})
    for ing in all_base_ingredients:
        name = ing.get('name', 'Unknown').strip(); unit = ing.get('unit', 'Unknown').strip(); key = (name.lower(), unit.lower())
        totals[key]['name'] = name; totals[key]['unit'] = unit; totals[key]['total_quantity'] += float(ing.get('quantity', 0))
    conn.close(); sorted_totals = sorted(totals.values(), key=lambda x: x['name']); return render_template('totals.html', totals=sorted_totals)
@app.route('/products')
def products_page():
    conn = get_db_connection(); # ... (Code unchanged)
    with conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute("""SELECT p.id, p.sku, p.jars_per_batch, r.name as recipe_name FROM products p LEFT JOIN recipes r ON p.recipe_id = r.id ORDER BY p.sku;"""); products = cur.fetchall()
        cur.execute("SELECT id, name FROM recipes WHERE is_sold_product = TRUE ORDER BY name;"); recipes = cur.fetchall()
    conn.close(); return render_template('products.html', products=products, recipes=recipes)
@app.route('/products/add', methods=['POST'])
def add_product():
    conn = get_db_connection(); # ... (Code unchanged)
    sku = request.form['sku']; recipe_id = request.form['recipe_id']; jars_per_batch = request.form.get('jars_per_batch') or None
    with conn.cursor() as cur: cur.execute('INSERT INTO products (sku, recipe_id, jars_per_batch) VALUES (%s, %s, %s);', (sku, recipe_id, jars_per_batch)); conn.commit(); conn.close(); return redirect(url_for('products_page'))
@app.route('/products/edit/<int:id>')
def edit_product(id):
    conn = get_db_connection(); # ... (Code unchanged)
    with conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute("SELECT * FROM products WHERE id = %s;", (id,)); product = cur.fetchone()
        cur.execute("SELECT id, name FROM recipes WHERE is_sold_product = TRUE ORDER BY name;"); recipes = cur.fetchall()
    conn.close(); return render_template('edit_product.html', product=product, recipes=recipes)
@app.route('/products/update/<int:id>', methods=['POST'])
def update_product(id):
    conn = get_db_connection(); # ... (Code unchanged)
    sku = request.form['sku']; recipe_id = request.form['recipe_id']; jars_per_batch = request.form.get('jars_per_batch') or None
    with conn.cursor() as cur: cur.execute("UPDATE products SET sku = %s, recipe_id = %s, jars_per_batch = %s WHERE id = %s;", (sku, recipe_id, jars_per_batch, id)); conn.commit(); conn.close(); return redirect(url_for('products_page'))
@app.route('/products/delete/<int:id>', methods=['POST'])
def delete_product(id):
    conn = get_db_connection(); # ... (Code unchanged)
    with conn.cursor() as cur: cur.execute("DELETE FROM products WHERE id = %s;", (id,)); conn.commit(); conn.close(); return redirect(url_for('products_page'))
@app.route('/locations', methods=['GET', 'POST'])
def locations_page():
    conn = get_db_connection(); # ... (Code unchanged)
    if request.method == 'POST':
        location_name = request.form['name'];
        with conn.cursor() as cur: cur.execute("INSERT INTO locations (name) VALUES (%s);", (location_name,)); conn.commit(); conn.close(); return redirect(url_for('locations_page'))
    with conn.cursor(cursor_factory=DictCursor) as cur: cur.execute("SELECT * FROM locations ORDER BY name;"); locations = cur.fetchall(); conn.close(); return render_template('locations.html', locations=locations)
@app.route('/locations/delete/<int:id>', methods=['POST'])
def delete_location(id):
    conn = get_db_connection(); # ... (Code unchanged)
    with conn.cursor() as cur: cur.execute("DELETE FROM locations WHERE id = %s;", (id,)); conn.commit(); conn.close(); return redirect(url_for('locations_page'))
@app.route('/stock-minimums', methods=['GET', 'POST'])
def stock_minimums_page():
    conn = get_db_connection(); # ... (Code unchanged)
    if request.method == 'POST':
        location_id = request.form['location_id']; product_id = request.form['product_id']; min_jars = request.form['min_jars']
        with conn.cursor() as cur: cur.execute("""INSERT INTO stock_minimums (location_id, product_id, min_jars) VALUES (%s, %s, %s) ON CONFLICT (product_id, location_id) DO UPDATE SET min_jars = EXCLUDED.min_jars;""", (location_id, product_id, min_jars)); conn.commit(); conn.close(); return redirect(url_for('stock_minimums_page'))
    with conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute("SELECT * FROM locations ORDER BY name;"); locations = cur.fetchall()
        cur.execute("""SELECT p.id, p.sku, r.name as recipe_name FROM products p JOIN recipes r ON p.recipe_id = r.id WHERE r.is_sold_product = TRUE ORDER BY p.sku;"""); products = cur.fetchall()
        cur.execute("""SELECT sm.id, l.name as location_name, p.sku, r.name as recipe_name, sm.min_jars FROM stock_minimums sm JOIN locations l ON sm.location_id = l.id JOIN products p ON sm.product_id = p.id JOIN recipes r ON p.recipe_id = r.id ORDER BY l.name, p.sku;"""); minimums = cur.fetchall()
    conn.close(); return render_template('stock_minimums.html', locations=locations, products=products, minimums=minimums)
@app.route('/stock-minimums/delete/<int:id>', methods=['POST'])
def delete_stock_minimum(id):
    conn = get_db_connection(); # ... (Code unchanged)
    with conn.cursor() as cur: cur.execute("DELETE FROM stock_minimums WHERE id = %s;", (id,)); conn.commit(); conn.close(); return redirect(url_for('stock_minimums_page'))
@app.route('/requirements')
def requirements_page():
    conn = get_db_connection(); # ... (Code unchanged)
    resolved_cache.clear(); all_base_ingredients = []
    with conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute("""SELECT p.recipe_id, p.jars_per_batch, SUM(sm.min_jars) as total_jars FROM stock_minimums sm JOIN products p ON sm.product_id = p.id JOIN recipes r ON p.recipe_id = r.id WHERE r.is_sold_product = TRUE AND p.jars_per_batch IS NOT NULL AND p.jars_per_batch > 0 GROUP BY p.recipe_id, p.jars_per_batch;"""); products_to_make = cur.fetchall()
        for prod in products_to_make:
            batches_needed = math.ceil(float(prod['total_jars']) / float(prod['jars_per_batch'])); base_ingredients_one_batch = get_base_ingredients(prod['recipe_id'], conn)
            for ing in base_ingredients_one_batch: scaled_ing = dict(ing); scaled_ing['quantity'] = float(scaled_ing['quantity']) * batches_needed; all_base_ingredients.append(scaled_ing)
    totals = defaultdict(lambda: {'name': '', 'unit': '', 'total_quantity': 0})
    for ing in all_base_ingredients:
        name = ing.get('name', 'Unknown').strip(); unit = ing.get('unit', 'Unknown').strip(); key = (name.lower(), unit.lower())
        totals[key]['name'] = name; totals[key]['unit'] = unit; totals[key]['total_quantity'] += float(ing.get('quantity', 0))
    conn.close(); sorted_totals = sorted(totals.values(), key=lambda x: x['name']); return render_template('requirements.html', totals=sorted_totals)
@app.route('/planner', methods=['GET', 'POST'])
def production_planner():
    conn = get_db_connection(); # ... (Code unchanged)
    resolved_cache.clear(); calculated_requirements = None
    with conn.cursor(cursor_factory=DictCursor) as cur: cur.execute("""SELECT p.id, p.sku, p.jars_per_batch, r.id as recipe_id, r.name as recipe_name FROM products p JOIN recipes r ON p.recipe_id = r.id WHERE r.is_sold_product = TRUE AND p.jars_per_batch IS NOT NULL AND p.jars_per_batch > 0 ORDER BY r.name;"""); sellable_products = cur.fetchall()
    if request.method == 'POST':
        all_base_ingredients_run = []
        for product in sellable_products:
            jars_to_make_str = request.form.get(f"jars_product_{product['id']}");
            try: jars_to_make = int(jars_to_make_str) if jars_to_make_str else 0
            except ValueError: jars_to_make = 0
            if jars_to_make > 0:
                batches_needed = math.ceil(jars_to_make / float(product['jars_per_batch'])); base_ingredients_one_batch = get_base_ingredients(product['recipe_id'], conn)
                for ing in base_ingredients_one_batch: scaled_ing = dict(ing); scaled_ing['quantity'] = float(scaled_ing['quantity']) * batches_needed; all_base_ingredients_run.append(scaled_ing)
        totals_run = defaultdict(lambda: {'name': '', 'unit': '', 'total_quantity': 0})
        for ing in all_base_ingredients_run:
            name = ing.get('name', 'Unknown').strip(); unit = ing.get('unit', 'Unknown').strip(); key = (name.lower(), unit.lower())
            totals_run[key]['name'] = name; totals_run[key]['unit'] = unit; totals_run[key]['total_quantity'] += float(ing.get('quantity', 0))
        calculated_requirements = sorted(totals_run.values(), key=lambda x: x['name'])
    conn.close(); return render_template('planner.html', products=sellable_products, requirements=calculated_requirements)
@app.route('/inventory', methods=['GET', 'POST'])
def inventory_items_page():
    conn = get_db_connection(); # ... (Code unchanged)
    if request.method == 'POST':
        name = request.form['name']; unit = request.form['unit']; qty_on_hand_str = request.form.get('quantity_on_hand')
        try: qty_on_hand = float(qty_on_hand_str) if qty_on_hand_str else 0.0
        except ValueError: qty_on_hand = 0.0
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM inventory_items WHERE LOWER(name) = LOWER(%s) AND LOWER(unit) = LOWER(%s);", (name, unit)); existing = cur.fetchone()
            if not existing: cur.execute("INSERT INTO inventory_items (name, unit, quantity_on_hand) VALUES (%s, %s, %s);", (name, unit, qty_on_hand)); conn.commit()
    with conn.cursor(cursor_factory=DictCursor) as cur: cur.execute("SELECT * FROM inventory_items ORDER BY name;"); inventory_items = cur.fetchall(); conn.close(); return render_template('inventory_items.html', inventory_items=inventory_items)
@app.route('/inventory/edit/<int:id>', methods=['GET'])
def edit_inventory_item(id):
    conn = get_db_connection(); # ... (Code unchanged)
    with conn.cursor(cursor_factory=DictCursor) as cur: cur.execute("SELECT * FROM inventory_items WHERE id = %s;", (id,)); item = cur.fetchone(); conn.close();
    if item is None: return redirect(url_for('inventory_items_page')); return render_template('edit_inventory_item.html', item=item)
@app.route('/inventory/update/<int:id>', methods=['POST'])
def update_inventory_item(id):
    conn = get_db_connection(); # ... (Code unchanged)
    name = request.form['name']; unit = request.form['unit']; qty_on_hand_str = request.form.get('quantity_on_hand')
    try: qty_on_hand = float(qty_on_hand_str) if qty_on_hand_str else 0.0
    except ValueError: qty_on_hand = 0.0 # Defaulting to 0 if invalid
    with conn.cursor() as cur: cur.execute("UPDATE inventory_items SET name = %s, unit = %s, quantity_on_hand = %s WHERE id = %s;", (name, unit, qty_on_hand, id)); conn.commit(); conn.close(); return redirect(url_for('inventory_items_page'))
@app.route('/inventory/delete/<int:id>', methods=['POST'])
def delete_inventory_item(id):
    conn = get_db_connection(); # ... (Code unchanged)
    try:
        with conn.cursor() as cur: cur.execute("DELETE FROM inventory_items WHERE id = %s;", (id,)); conn.commit()
    except psycopg2.Error as e: conn.rollback(); print(f"Error deleting inventory item {id}: {e}") # Basic error logging
    finally: conn.close(); return redirect(url_for('inventory_items_page'))
@app.route('/wip', methods=['GET', 'POST'])
def wip_batches_page():
    conn = get_db_connection(); # ... (Code unchanged)
    if request.method == 'POST':
        recipe_id = request.form['recipe_id']; target_jars = request.form.get('target_jars') or 0
        try:
            target_jars_int = int(target_jars);
            if target_jars_int > 0:
                with conn.cursor() as cur: cur.execute("INSERT INTO wip_batches (recipe_id, target_jars, status) VALUES (%s, %s, %s);", (recipe_id, target_jars_int, 'In Progress')); conn.commit()
        except ValueError: flash('Invalid number entered for Target Jars.', 'error') # Example flash message
        finally: conn.close(); return redirect(url_for('wip_batches_page'))
    with conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute("""SELECT w.id, r.name as recipe_name, w.target_jars, w.status, w.created_at FROM wip_batches w JOIN recipes r ON w.recipe_id = r.id WHERE w.status = 'In Progress' ORDER BY w.created_at DESC;"""); wip_batches = cur.fetchall()
        cur.execute("""SELECT id, name FROM recipes WHERE is_sold_product = TRUE ORDER BY name;"""); sellable_recipes = cur.fetchall()
    conn.close(); return render_template('wip_batches.html', wip_batches=wip_batches, sellable_recipes=sellable_recipes)

# --- NEW WIP BATCH DETAIL & ACTIONS ROUTES ---
@app.route('/wip/<int:batch_id>')
def wip_batch_detail(batch_id):
    conn = get_db_connection()
    resolved_cache.clear()
    with conn.cursor(cursor_factory=DictCursor) as cur:
        # Fetch batch details
        cur.execute("""
            SELECT w.id, w.target_jars, w.status, w.created_at, r.id as recipe_id, r.name as recipe_name, r.yield_quantity as recipe_batch_yield, r.yield_unit as recipe_batch_unit
            FROM wip_batches w
            JOIN recipes r ON w.recipe_id = r.id
            WHERE w.id = %s;
        """, (batch_id,))
        batch = cur.fetchone()

        if not batch:
            flash(f"WIP Batch ID {batch_id} not found.", "error")
            return redirect(url_for('wip_batches_page'))

        # Calculate total base ingredients needed for this batch size
        required_ingredients = defaultdict(lambda: {'name': '', 'unit': '', 'total_needed': 0, 'inventory_item_id': None})
        base_ingredients_one_batch = get_base_ingredients(batch['recipe_id'], conn)

        # Calculate scaling based on target jars and product yield (jars per batch)
        # We need the product linked to this recipe
        cur.execute("SELECT jars_per_batch FROM products WHERE recipe_id = %s LIMIT 1;", (batch['recipe_id'],))
        product_info = cur.fetchone()
        if product_info and product_info['jars_per_batch'] and float(product_info['jars_per_batch']) > 0:
             batches_needed = math.ceil(float(batch['target_jars']) / float(product_info['jars_per_batch']))
        else:
             batches_needed = 1 # Default or error handling if product/yield not found


        for ing in base_ingredients_one_batch:
            name = ing.get('name', 'Unknown').strip()
            unit = ing.get('unit', 'Unknown').strip()
            key = (name.lower(), unit.lower())
            required_ingredients[key]['name'] = name
            required_ingredients[key]['unit'] = unit
            required_ingredients[key]['inventory_item_id'] = ing.get('inventory_item_id') # Store inventory_item_id
            required_ingredients[key]['total_needed'] += float(ing.get('quantity', 0)) * batches_needed

        # Fetch current allocations for this batch
        cur.execute("""
            SELECT inv.id as inventory_item_id, inv.name, inv.unit, SUM(wa.quantity_allocated) as total_allocated
            FROM wip_allocations wa
            JOIN inventory_items inv ON wa.inventory_item_id = inv.id
            WHERE wa.wip_batch_id = %s
            GROUP BY inv.id, inv.name, inv.unit;
        """, (batch_id,))
        allocations_raw = cur.fetchall()

        # Combine required and allocated amounts
        current_allocations = {alloc['inventory_item_id']: alloc['total_allocated'] for alloc in allocations_raw}

        ingredient_summary = []
        for key, req in required_ingredients.items():
             inv_item_id = req.get('inventory_item_id')
             allocated = float(current_allocations.get(inv_item_id, 0)) if inv_item_id else 0
             remaining = float(req['total_needed']) - allocated
             ingredient_summary.append({
                 'inventory_item_id': inv_item_id,
                 'name': req['name'],
                 'unit': req['unit'],
                 'needed': round(float(req['total_needed']), 2),
                 'allocated': round(allocated, 2),
                 'remaining': round(remaining, 2)
             })

        ingredient_summary.sort(key=lambda x: x['name']) # Sort final list

        # Fetch inventory items for the allocation dropdown
        cur.execute("SELECT id, name, unit FROM inventory_items ORDER BY name;")
        inventory_items = cur.fetchall()

    conn.close()
    return render_template('wip_batch_detail.html',
                           batch=batch,
                           ingredient_summary=ingredient_summary,
                           inventory_items=inventory_items)

@app.route('/wip/<int:batch_id>/allocate', methods=['POST'])
def allocate_ingredient(batch_id):
    inventory_item_id = request.form.get('inventory_item_id')
    quantity_str = request.form.get('quantity_allocated')

    # Basic Validation
    if not inventory_item_id or not quantity_str:
        flash("Missing ingredient or quantity.", "error")
        return redirect(url_for('wip_batch_detail', batch_id=batch_id))

    try:
        quantity = float(quantity_str)
        if quantity <= 0:
            raise ValueError("Quantity must be positive.")
    except ValueError:
        flash("Invalid quantity entered.", "error")
        return redirect(url_for('wip_batch_detail', batch_id=batch_id))

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # Check available stock (On Hand - Allocated)
            cur.execute("SELECT (quantity_on_hand - quantity_allocated) as available FROM inventory_items WHERE id = %s;", (inventory_item_id,))
            result = cur.fetchone()
            if result is None or result[0] < quantity:
                flash(f"Not enough available stock for this item (Available: {result[0] if result else 'N/A'}).", "error")
                conn.rollback() # Not strictly needed as no changes made yet, but good practice
            else:
                # 1. Add allocation record
                cur.execute("INSERT INTO wip_allocations (wip_batch_id, inventory_item_id, quantity_allocated) VALUES (%s, %s, %s);",
                            (batch_id, inventory_item_id, quantity))
                # 2. Update inventory_items allocated quantity
                cur.execute("UPDATE inventory_items SET quantity_allocated = quantity_allocated + %s WHERE id = %s;",
                            (quantity, inventory_item_id))
                conn.commit()
                flash("Ingredient allocated successfully.", "success")
    except psycopg2.Error as e:
        conn.rollback()
        flash(f"Database error during allocation: {e}", "error")
        print(f"DB Error allocating to batch {batch_id}: {e}") # Log detailed error
    finally:
        conn.close()

    return redirect(url_for('wip_batch_detail', batch_id=batch_id))


@app.route('/wip/<int:batch_id>/complete', methods=['POST'])
def complete_wip_batch(batch_id):
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
             # Fetch all allocations for this batch
            cur.execute("SELECT inventory_item_id, SUM(quantity_allocated) as total_allocated FROM wip_allocations WHERE wip_batch_id = %s GROUP BY inventory_item_id;", (batch_id,))
            allocations = cur.fetchall()

            # Update inventory: Decrease On Hand, Decrease Allocated
            for alloc in allocations:
                cur.execute("""
                    UPDATE inventory_items
                    SET quantity_on_hand = quantity_on_hand - %s,
                        quantity_allocated = quantity_allocated - %s
                    WHERE id = %s;
                """, (alloc['total_allocated'], alloc['total_allocated'], alloc['inventory_item_id']))

            # Update batch status and completion time
            cur.execute("UPDATE wip_batches SET status = 'Completed', completed_at = NOW() WHERE id = %s;", (batch_id,))

        conn.commit()
        flash(f"Batch {batch_id} marked as complete and inventory updated.", "success")
    except psycopg2.Error as e:
        conn.rollback()
        flash(f"Database error completing batch: {e}", "error")
        print(f"DB Error completing batch {batch_id}: {e}")
    finally:
        conn.close()

    return redirect(url_for('wip_batches_page')) # Redirect to the main WIP list

@app.route('/wip/delete/<int:batch_id>', methods=['POST'])
def delete_wip_batch(batch_id):
    # Deleting a WIP batch should also reverse its allocations
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            # Find allocations to reverse
            cur.execute("SELECT inventory_item_id, SUM(quantity_allocated) as total_allocated FROM wip_allocations WHERE wip_batch_id = %s GROUP BY inventory_item_id;", (batch_id,))
            allocations_to_reverse = cur.fetchall()

            # Reverse the allocations in inventory_items
            for alloc in allocations_to_reverse:
                cur.execute("UPDATE inventory_items SET quantity_allocated = quantity_allocated - %s WHERE id = %s;",
                            (alloc['total_allocated'], alloc['inventory_item_id']))

            # Delete the batch itself (ON DELETE CASCADE handles wip_allocations table)
            cur.execute("DELETE FROM wip_batches WHERE id = %s;", (batch_id,))

        conn.commit()
        flash(f"WIP Batch {batch_id} deleted and allocations reversed.", "success")
    except psycopg2.Error as e:
        conn.rollback()
        flash(f"Database error deleting batch: {e}", "error")
        print(f"DB Error deleting batch {batch_id}: {e}")
    finally:
        conn.close()
    return redirect(url_for('wip_batches_page'))

# --- END NEW WIP ROUTES ---


if __name__ == '__main__':
    app.run(debug=True)