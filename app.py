import os
import psycopg2
from psycopg2.extras import DictCursor
from dotenv import load_dotenv
from flask import Flask, render_template, request, redirect, url_for, flash
from collections import defaultdict
import math
from datetime import datetime

load_dotenv()
app = Flask(__name__)
app.config['SECRET_KEY'] = os.getenv('SECRET_KEY', 'a_default_secret_key_for_development')

def get_db_connection():
    conn_string = os.getenv("DATABASE_URL")
    conn = psycopg2.connect(conn_string)
    return conn

# --- RECURSIVE LOGIC ---
resolved_cache = {}
def get_base_ingredients(recipe_id, conn):
    if recipe_id in resolved_cache:
        return [ing.copy() for ing in resolved_cache[recipe_id]]
    
    with conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute("""
            SELECT i.*, inv.id as inventory_item_id, inv.name as inv_name, inv.unit as inv_unit
            FROM ingredients i LEFT JOIN inventory_items inv ON i.inventory_item_id = inv.id
            WHERE i.recipe_id = %s; """, (recipe_id,))
        ingredients = cur.fetchall()
        base_ingredients = []
        
        for ing in ingredients:
            if ing['sub_recipe_id']:
                # --- THIS IS THE UPDATED LOGIC ---
                # Fetch the sub-recipe's yield quantity AND unit
                cur.execute("SELECT yield_quantity, yield_unit FROM recipes WHERE id = %s;", (ing['sub_recipe_id'],))
                sub_recipe_yield_row = cur.fetchone()
                
                scaling_ratio = 1.0 # Default scaling
                
                if sub_recipe_yield_row:
                    yield_qty = sub_recipe_yield_row.get('yield_quantity')
                    yield_unit = sub_recipe_yield_row.get('yield_unit') # Can be 'grams', 'mLs', 'batches'

                    if yield_unit in ('grams', 'mLs'):
                        # Use weight/volume ratio logic if yield_qty is valid
                        if yield_qty and float(yield_qty) != 0:
                            scaling_ratio = float(ing['quantity']) / float(yield_qty)
                        # else: (if yield 0 or null, ratio remains 1.0 - might need review)
                    elif yield_unit == 'batches':
                        # Use direct batch scaling logic (e.g., 1 "batch" of base, 0.5 "batch")
                        # The quantity of the ingredient IS the scaling ratio
                        scaling_ratio = float(ing['quantity'])
                    # If yield_unit is null or something else, default ratio of 1.0 is used
                
                # --- END OF UPDATED LOGIC ---

                sub_ingredients = get_base_ingredients(ing['sub_recipe_id'], conn)
                for sub_ing in sub_ingredients:
                    scaled_ing = dict(sub_ing)
                    scaled_ing['quantity'] = float(scaled_ing.get('quantity', 0)) * scaling_ratio
                    base_ingredients.append(scaled_ing)
                    
            elif ing['inventory_item_id']:
                raw_ing = {
                    'inventory_item_id': ing['inventory_item_id'],
                    'name': ing['inv_name'],
                    'unit': ing['inv_unit'],
                    'quantity': float(ing.get('quantity', 0))
                }
                base_ingredients.append(raw_ing)
                
    resolved_cache[recipe_id] = [ing.copy() for ing in base_ingredients]
    return base_ingredients

# --- Standard Routes ---
@app.route('/')
def home():
    conn = get_db_connection()
    dashboard_data = {
        'wip_batches_count': 0,
        'open_pos_count': 0,
        'low_stock_count': 0
    }
    try:
        resolved_cache.clear()
        all_base_ingredients = []
        inventory_levels = {}
        with conn.cursor(cursor_factory=DictCursor) as cur:
            # 1. Get count of active WIP batches
            cur.execute("SELECT COUNT(id) as count FROM wip_batches WHERE status = 'In Progress';")
            dashboard_data['wip_batches_count'] = cur.fetchone()['count']

            # 2. Get count of open purchase orders
            cur.execute("SELECT COUNT(id) as count FROM purchase_orders WHERE status = 'Placed' OR status = 'Shipped';")
            dashboard_data['open_pos_count'] = cur.fetchone()['count']

            # 3. Calculate Low Stock Items
            cur.execute("SELECT id, name, unit, quantity_on_hand, quantity_allocated FROM inventory_items;");
            for item in cur.fetchall():
                inventory_levels[item['id']] = {
                    'available': float(item.get('quantity_on_hand', 0)) - float(item.get('quantity_allocated', 0))
                }
            
            cur.execute("""
                SELECT p.recipe_id, p.jars_per_batch, SUM(sm.min_jars) as total_jars
                FROM stock_minimums sm
                JOIN products p ON sm.product_id = p.id
                JOIN recipes r ON p.recipe_id = r.id
                WHERE r.is_sold_product = TRUE AND p.jars_per_batch IS NOT NULL AND p.jars_per_batch > 0
                GROUP BY p.recipe_id, p.jars_per_batch;
            """)
            products_to_make = cur.fetchall()
            
            for prod in products_to_make:
                batches_needed = math.ceil(float(prod['total_jars']) / float(prod['jars_per_batch']))
                base_ingredients_one_batch = get_base_ingredients(prod['recipe_id'], conn)
                for ing in base_ingredients_one_batch:
                    scaled_ing = dict(ing)
                    scaled_ing['quantity'] = float(scaled_ing['quantity']) * batches_needed
                    all_base_ingredients.append(scaled_ing)

            totals_needed = defaultdict(lambda: {'total_needed': 0})
            for ing in all_base_ingredients:
                inv_item_id = ing.get('inventory_item_id')
                if inv_item_id:
                    totals_needed[inv_item_id]['total_needed'] += float(ing.get('quantity', 0))
            
            low_stock_count = 0
            for inv_id, needed_data in totals_needed.items():
                available = inventory_levels.get(inv_id, {'available': 0})['available']
                net_needed = needed_data['total_needed'] - available
                if net_needed > 0:
                    low_stock_count += 1
            
            dashboard_data['low_stock_count'] = low_stock_count

    except psycopg2.Error as e:
        flash(f"Error fetching dashboard data: {e}", "error")
        print(f"DB Error fetching dashboard data: {e}")
    finally:
        if conn: conn.close()

    return render_template('index.html', dashboard_data=dashboard_data)

@app.route('/recipes')
def recipe_dashboard():
    recipes_list = []; conn = None
    try:
        conn = get_db_connection(); resolved_cache.clear()
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute('SELECT * FROM recipes ORDER BY name;'); recipes_from_db = cur.fetchall()
            for recipe in recipes_from_db:
                recipe_dict = dict(recipe)
                cur.execute("""SELECT i.quantity, i.sub_recipe_id, COALESCE(inv.name, r_sub.name, i.name, 'Unknown') as name, COALESCE(inv.unit, i.unit, 'N/A') as unit FROM ingredients i LEFT JOIN inventory_items inv ON i.inventory_item_id = inv.id LEFT JOIN recipes r_sub ON i.sub_recipe_id = r_sub.id WHERE i.recipe_id = %s;""", (recipe['id'],)); recipe_dict['ingredients'] = [dict(ing) for ing in cur.fetchall()]
                base_ingredients_for_totals = get_base_ingredients(recipe['id'], conn)
                totals = {'grams': 0, 'mLs': 0}
                for ing in base_ingredients_for_totals:
                    unit = ing.get('unit', '').lower(); quantity = float(ing.get('quantity', 0))
                    if unit == 'grams': totals['grams'] += quantity
                    elif unit == 'mls': totals['mLs'] += quantity
                recipe_dict['totals'] = { 'grams': round(totals['grams'], 2), 'mLs': round(totals['mLs'], 2) }
                recipes_list.append(recipe_dict)
    except psycopg2.Error as e:
        flash(f"Error fetching recipes: {e}", "error"); print(f"DB Error fetching recipes: {e}")
    finally:
        if conn: conn.close()
    return render_template('recipes.html', recipes=recipes_list)

@app.route('/new')
def new_recipe_form():
    conn = get_db_connection(); recipes = []; inventory_items = []
    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute("SELECT id, name FROM recipes ORDER BY name;"); recipes = cur.fetchall()
            cur.execute("SELECT id, name, unit FROM inventory_items ORDER BY name;"); inventory_items = cur.fetchall()
    except psycopg2.Error as e: flash(f"Error fetching data: {e}", "error"); print(f"DB Error new recipe form: {e}")
    finally:
        if conn: conn.close()
    return render_template('add_recipe.html', recipes=recipes, inventory_items=inventory_items)

@app.route('/create', methods=['POST'])
def create_recipe():
    conn = get_db_connection(); recipe_id = None
    try:
        with conn.cursor() as cur:
            recipe_name = request.form['recipe_name']; yield_quantity = request.form.get('yield_quantity') or None; yield_unit = request.form.get('yield_unit') or None; is_sold_product = 'is_sold_product' in request.form
            cur.execute('INSERT INTO recipes (name, yield_quantity, yield_unit, is_sold_product) VALUES (%s, %s, %s, %s) RETURNING id;',(recipe_name, yield_quantity, yield_unit, is_sold_product)); recipe_id = cur.fetchone()[0]
            inventory_item_ids = request.form.getlist('inventory_item_id'); quantities = request.form.getlist('quantity'); sub_recipe_ids = request.form.getlist('sub_recipe_id')
            
            if not (len(quantities) == len(inventory_item_ids) == len(sub_recipe_ids)):
                flash("Form data inconsistency.", "error"); raise ValueError("Form list lengths mismatch")
            
            for i in range(len(quantities)):
                inventory_item_id = inventory_item_ids[i] if inventory_item_ids[i] else None; sub_recipe_id = sub_recipe_ids[i] if sub_recipe_ids[i] else None; quantity_str = quantities[i]
                if not inventory_item_id and not sub_recipe_id: flash(f"Row {i+1} skipped.", "warning"); continue
                if inventory_item_id and sub_recipe_id: flash(f"Row {i+1} skipped.", "warning"); continue
                
                try: 
                    quantity_val = float(quantity_str) if quantity_str else 0
                    if quantity_val <= 0: flash(f"Quantity row {i+1} invalid. Row skipped.", "warning"); continue
                except ValueError: flash(f"Invalid quantity row {i+1}. Row skipped.", "warning"); continue
                
                item_name = None; item_unit = None; item_data = None; sub_recipe_data = None
                if inventory_item_id: 
                    cur.execute("SELECT name, unit FROM inventory_items WHERE id = %s;", (inventory_item_id,))
                    item_data = cur.fetchone()
                    if item_data: 
                        item_name, item_unit = item_data
                elif sub_recipe_id: 
                    cur.execute("SELECT name FROM recipes WHERE id = %s;", (sub_recipe_id,))
                    sub_recipe_data = cur.fetchone()
                    if sub_recipe_data: 
                        item_name = sub_recipe_data[0]
                        item_unit = 'batch'
                
                cur.execute("""INSERT INTO ingredients (recipe_id, inventory_item_id, sub_recipe_id, quantity, name, unit) VALUES (%s, %s, %s, %s, %s, %s);""", (recipe_id, inventory_item_id, sub_recipe_id, quantity_val, item_name, item_unit))
        
        conn.commit()
        flash("Recipe created successfully!", "success")
        return redirect(url_for('recipe_dashboard'))

    except (psycopg2.Error, ValueError) as e:
        if conn: conn.rollback()
        flash(f"Error creating recipe: {e}", "error")
        print(f"Error creating recipe: {e}")
        recipes = []; inventory_items = []; conn_err = None
        try:
            conn_err = get_db_connection()
            with conn_err.cursor(cursor_factory=DictCursor) as cur_err: 
                cur_err.execute("SELECT id, name FROM recipes ORDER BY name;"); recipes = cur_err.fetchall()
                cur_err.execute("SELECT id, name, unit FROM inventory_items ORDER BY name;"); inventory_items = cur_err.fetchall()
        except psycopg2.Error as fetch_e: 
            flash(f"Error fetching form data after failed save: {fetch_e}", "error")
        finally:
             if conn_err: conn_err.close()
        return render_template('add_recipe.html', recipes=recipes, inventory_items=inventory_items)
    
    finally:
        if conn: conn.close()

@app.route('/edit/<int:recipe_id>')
def edit_recipe_form(recipe_id):
    conn = get_db_connection(); recipe = None; ingredients = []; all_recipes = []; inventory_items = []
    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute('SELECT * FROM recipes WHERE id = %s;', (recipe_id,)); recipe = cur.fetchone()
            if not recipe: flash(f"Recipe ID {recipe_id} not found.", "error"); return redirect(url_for('recipe_dashboard'))
            cur.execute('SELECT * FROM ingredients WHERE recipe_id = %s;', (recipe_id,)); ingredients = cur.fetchall()
            cur.execute("SELECT id, name FROM recipes ORDER BY name;"); all_recipes = cur.fetchall()
            cur.execute("SELECT id, name, unit FROM inventory_items ORDER BY name;"); inventory_items = cur.fetchall()
    except psycopg2.Error as e: 
        flash(f"Error fetching data: {e}", "error"); print(f"DB Error edit recipe form {recipe_id}: {e}")
        return redirect(url_for('recipe_dashboard'))
    finally:
         if conn: conn.close()
    return render_template('edit_recipe.html', recipe=recipe, ingredients=ingredients, recipes=all_recipes, inventory_items=inventory_items)

@app.route('/update/<int:recipe_id>', methods=['POST'])
def update_recipe(recipe_id):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            new_name = request.form['recipe_name']; yield_quantity = request.form.get('yield_quantity') or None; yield_unit = request.form.get('yield_unit') or None; is_sold_product = 'is_sold_product' in request.form
            cur.execute('UPDATE recipes SET name = %s, yield_quantity = %s, yield_unit = %s, is_sold_product = %s WHERE id = %s;', (new_name, yield_quantity, yield_unit, is_sold_product, recipe_id))
            inventory_item_ids = request.form.getlist('inventory_item_id'); quantities = request.form.getlist('quantity'); sub_recipe_ids = request.form.getlist('sub_recipe_id')
            
            if not (len(quantities) == len(inventory_item_ids) == len(sub_recipe_ids)):
                flash("Form data inconsistency.", "error"); raise ValueError("Form list lengths mismatch")
            
            cur.execute('DELETE FROM ingredients WHERE recipe_id = %s;', (recipe_id,))
            
            for i in range(len(quantities)):
                inventory_item_id = inventory_item_ids[i] if inventory_item_ids[i] else None; sub_recipe_id = sub_recipe_ids[i] if sub_recipe_ids[i] else None; quantity_str = quantities[i]
                if not inventory_item_id and not sub_recipe_id: flash(f"Row {i+1} skipped.", "warning"); continue
                if inventory_item_id and sub_recipe_id: flash(f"Row {i+1} skipped.", "warning"); continue
                
                try: 
                    quantity_val = float(quantity_str) if quantity_str else 0
                    if quantity_val <= 0: flash(f"Quantity row {i+1} invalid. Row skipped.", "warning"); continue
                except ValueError: flash(f"Invalid quantity row {i+1}. Row skipped.", "warning"); continue
                
                item_name = None; item_unit = None; item_data = None; sub_recipe_data = None
                if inventory_item_id: 
                    cur.execute("SELECT name, unit FROM inventory_items WHERE id = %s;", (inventory_item_id,))
                    item_data = cur.fetchone()
                    if item_data: 
                        item_name, item_unit = item_data
                elif sub_recipe_id: 
                    cur.execute("SELECT name FROM recipes WHERE id = %s;", (sub_recipe_id,))
                    sub_recipe_data = cur.fetchone()
                    if sub_recipe_data: 
                        item_name = sub_recipe_data[0]
                        item_unit = 'batch'
                
                cur.execute("""INSERT INTO ingredients (recipe_id, inventory_item_id, sub_recipe_id, quantity, name, unit) VALUES (%s, %s, %s, %s, %s, %s);""", (recipe_id, inventory_item_id, sub_recipe_id, quantity_val, item_name, item_unit))
        
        conn.commit()
        flash("Recipe updated successfully!", "success")
        return redirect(url_for('recipe_dashboard'))

    except (psycopg2.Error, ValueError) as e:
        if conn: conn.rollback()
        flash(f"Error updating recipe: {e}", "error")
        print(f"DB Error updating recipe {recipe_id}: {e}")
        return redirect(url_for('edit_recipe_form', recipe_id=recipe_id))
    finally:
        if conn: conn.close()

@app.route('/delete/<int:recipe_id>', methods=['POST'])
def delete_recipe(recipe_id):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur: cur.execute('DELETE FROM recipes WHERE id = %s;', (recipe_id,)); conn.commit(); flash("Recipe deleted.", "success")
    except psycopg2.Error as e: 
        if conn: conn.rollback()
        flash(f"Error deleting recipe: {e.diag.message_primary}", "error"); print(f"DB Error delete recipe {recipe_id}: {e}")
    finally:
        if conn: conn.close()
    return redirect(url_for('recipe_dashboard'))

@app.route('/totals')
def ingredient_totals():
    conn = get_db_connection(); sorted_totals = []
    try:
        resolved_cache.clear(); all_base_ingredients = []
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute("SELECT id FROM recipes;"); all_recipe_ids = cur.fetchall()
            for rec_id in all_recipe_ids: all_base_ingredients.extend(get_base_ingredients(rec_id['id'], conn))
        totals = defaultdict(lambda: {'name': '', 'unit': '', 'total_quantity': 0})
        for ing in all_base_ingredients:
            name = ing.get('name', 'Unknown').strip(); unit = ing.get('unit', 'Unknown').strip(); key = (name.lower(), unit.lower())
            totals[key]['name'] = name; totals[key]['unit'] = unit; totals[key]['total_quantity'] += float(ing.get('quantity', 0))
        sorted_totals = sorted(totals.values(), key=lambda x: x['name'])
    except psycopg2.Error as e: flash(f"Error calculating totals: {e}", "error"); print(f"DB Error ingredient totals: {e}")
    finally:
        if conn: conn.close()
    return render_template('totals.html', totals=sorted_totals)

@app.route('/products')
def products_page():
    conn = get_db_connection(); products = []; recipes = []
    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute("""SELECT p.id, p.sku, p.jars_per_batch, r.name as recipe_name FROM products p LEFT JOIN recipes r ON p.recipe_id = r.id ORDER BY p.sku;"""); products = cur.fetchall()
            cur.execute("SELECT id, name FROM recipes WHERE is_sold_product = TRUE ORDER BY name;"); recipes = cur.fetchall()
    except psycopg2.Error as e: flash(f"Error fetching products: {e}", "error"); print(f"DB Error products page: {e}")
    finally:
        if conn: conn.close()
    return render_template('products.html', products=products, recipes=recipes)

@app.route('/products/add', methods=['POST'])
def add_product():
    conn = get_db_connection()
    try:
        sku = request.form['sku']; recipe_id = request.form['recipe_id']; jars_per_batch = request.form.get('jars_per_batch') or None
        with conn.cursor() as cur: cur.execute('INSERT INTO products (sku, recipe_id, jars_per_batch) VALUES (%s, %s, %s);', (sku, recipe_id, jars_per_batch)); conn.commit(); flash("Product added.", "success")
    except psycopg2.Error as e: 
        if conn: conn.rollback()
        flash(f"Error adding product: {e}", "error"); print(f"DB Error add product: {e}")
    finally:
        if conn: conn.close()
    return redirect(url_for('products_page'))

@app.route('/products/edit/<int:id>')
def edit_product(id):
    conn = get_db_connection(); product = None; recipes = []
    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute("SELECT * FROM products WHERE id = %s;", (id,)); product = cur.fetchone()
            if not product: flash(f"Product ID {id} not found.", "error"); return redirect(url_for('products_page'))
            cur.execute("SELECT id, name FROM recipes WHERE is_sold_product = TRUE ORDER BY name;"); recipes = cur.fetchall()
    except psycopg2.Error as e: 
        flash(f"Error fetching product data: {e}", "error"); print(f"DB Error edit product GET {id}: {e}")
        return redirect(url_for('products_page'))
    finally:
        if conn: conn.close()
    return render_template('edit_product.html', product=product, recipes=recipes)

@app.route('/products/update/<int:id>', methods=['POST'])
def update_product(id):
    conn = get_db_connection()
    try:
        sku = request.form['sku']; recipe_id = request.form['recipe_id']; jars_per_batch = request.form.get('jars_per_batch') or None
        with conn.cursor() as cur: cur.execute("UPDATE products SET sku = %s, recipe_id = %s, jars_per_batch = %s WHERE id = %s;", (sku, recipe_id, jars_per_batch, id)); conn.commit(); flash("Product updated.", "success")
    except psycopg2.Error as e: 
        if conn: conn.rollback()
        flash(f"Error updating product: {e}", "error"); print(f"DB Error update product {id}: {e}")
    finally:
        if conn: conn.close()
    return redirect(url_for('products_page'))

@app.route('/products/delete/<int:id>', methods=['POST'])
def delete_product(id):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur: cur.execute("DELETE FROM products WHERE id = %s;", (id,)); conn.commit(); flash("Product deleted.", "success")
    except psycopg2.Error as e: 
        if conn: conn.rollback()
        flash(f"Error deleting product: {e.diag.message_primary}", "error"); print(f"DB Error delete product {id}: {e}")
    finally:
        if conn: conn.close()
    return redirect(url_for('products_page'))

@app.route('/locations', methods=['GET', 'POST'])
def locations_page():
    conn = get_db_connection(); locations = []
    try:
        if request.method == 'POST':
            location_name = request.form['name']
            with conn.cursor() as cur: cur.execute("INSERT INTO locations (name) VALUES (%s);", (location_name,)); conn.commit(); flash("Location added.", "success")
            if conn: conn.close(); return redirect(url_for('locations_page'))
        with conn.cursor(cursor_factory=DictCursor) as cur: cur.execute("SELECT * FROM locations ORDER BY name;"); locations = cur.fetchall()
    except psycopg2.Error as e:
        if conn and request.method == 'POST': conn.rollback()
        flash(f"Error accessing locations: {e}", "error"); print(f"DB Error locations page: {e}")
    finally:
        if conn: conn.close()
    return render_template('locations.html', locations=locations)

@app.route('/locations/delete/<int:id>', methods=['POST'])
def delete_location(id):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur: cur.execute("DELETE FROM locations WHERE id = %s;", (id,)); conn.commit(); flash("Location deleted.", "success")
    except psycopg2.Error as e: 
        if conn: conn.rollback()
        flash(f"Error deleting location: {e.diag.message_primary}", "error"); print(f"DB Error delete location {id}: {e}")
    finally:
        if conn: conn.close()
    return redirect(url_for('locations_page'))

@app.route('/stock-minimums', methods=['GET', 'POST'])
def stock_minimums_page():
    conn = get_db_connection(); locations = []; products = []; minimums = []
    try:
        if request.method == 'POST':
            location_id = request.form['location_id']; product_id = request.form['product_id']; min_jars = request.form['min_jars']
            with conn.cursor() as cur: cur.execute("""INSERT INTO stock_minimums (location_id, product_id, min_jars) VALUES (%s, %s, %s) ON CONFLICT (product_id, location_id) DO UPDATE SET min_jars = EXCLUDED.min_jars;""", (location_id, product_id, min_jars)); conn.commit(); flash("Stock minimum set/updated.", "success")
            if conn: conn.close(); return redirect(url_for('stock_minimums_page'))
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute("SELECT * FROM locations ORDER BY name;"); locations = cur.fetchall()
            cur.execute("""SELECT p.id, p.sku, r.name as recipe_name FROM products p JOIN recipes r ON p.recipe_id = r.id WHERE r.is_sold_product = TRUE ORDER BY p.sku;"""); products = cur.fetchall()
            cur.execute("""SELECT sm.id, l.name as location_name, p.sku, r.name as recipe_name, sm.min_jars FROM stock_minimums sm JOIN locations l ON sm.location_id = l.id JOIN products p ON sm.product_id = p.id JOIN recipes r ON p.recipe_id = r.id ORDER BY l.name, p.sku;"""); minimums = cur.fetchall()
    except psycopg2.Error as e:
        if conn and request.method == 'POST': conn.rollback()
        flash(f"Error accessing stock minimums: {e}", "error"); print(f"DB Error stock minimums page: {e}")
    finally:
        if conn: conn.close()
    return render_template('stock_minimums.html', locations=locations, products=products, minimums=minimums)

@app.route('/stock-minimums/delete/<int:id>', methods=['POST'])
def delete_stock_minimum(id):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur: cur.execute("DELETE FROM stock_minimums WHERE id = %s;", (id,)); conn.commit(); flash("Stock minimum deleted.", "success")
    except psycopg2.Error as e: 
        if conn: conn.rollback()
        flash(f"Error deleting stock minimum: {e}", "error"); print(f"DB Error delete stock min {id}: {e}")
    finally:
        if conn: conn.close()
    return redirect(url_for('stock_minimums_page'))

@app.route('/requirements')
def requirements_page():
    conn = get_db_connection(); sorted_report_data = []
    try:
        resolved_cache.clear(); all_base_ingredients = []; inventory_levels = {}
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute("SELECT id, name, unit, quantity_on_hand, quantity_allocated FROM inventory_items;");
            for item in cur.fetchall(): inventory_levels[item['id']] = {'name': item['name'], 'unit': item['unit'], 'on_hand': float(item.get('quantity_on_hand', 0)), 'allocated': float(item.get('quantity_allocated', 0)), 'available': float(item.get('quantity_on_hand', 0)) - float(item.get('quantity_allocated', 0))}
            cur.execute("""SELECT p.recipe_id, p.jars_per_batch, SUM(sm.min_jars) as total_jars FROM stock_minimums sm JOIN products p ON sm.product_id = p.id JOIN recipes r ON p.recipe_id = r.id WHERE r.is_sold_product = TRUE AND p.jars_per_batch IS NOT NULL AND p.jars_per_batch > 0 GROUP BY p.recipe_id, p.jars_per_batch;"""); products_to_make = cur.fetchall()
            for prod in products_to_make:
                batches_needed = math.ceil(float(prod['total_jars']) / float(prod['jars_per_batch'])); base_ingredients_one_batch = get_base_ingredients(prod['recipe_id'], conn)
                for ing in base_ingredients_one_batch: scaled_ing = dict(ing); scaled_ing['quantity'] = float(scaled_ing['quantity']) * batches_needed; all_base_ingredients.append(scaled_ing)
        totals_needed = defaultdict(lambda: {'name': '', 'unit': '', 'total_needed': 0, 'inventory_item_id': None})
        for ing in all_base_ingredients:
            inv_item_id = ing.get('inventory_item_id')
            if inv_item_id: name = ing.get('name', 'Unknown').strip(); unit = ing.get('unit', 'Unknown').strip(); key = (inv_item_id); totals_needed[key]['name'] = name; totals_needed[key]['unit'] = unit; totals_needed[key]['inventory_item_id'] = inv_item_id; totals_needed[key]['total_needed'] += float(ing.get('quantity', 0))
        report_data = []
        for inv_id, needed_data in totals_needed.items():
            inv_info = inventory_levels.get(inv_id, {'on_hand': 0, 'allocated': 0, 'available': 0}); total_needed = needed_data['total_needed']; available = inv_info['available']; net_needed = max(0, total_needed - available)
            report_data.append({'name': needed_data['name'], 'unit': needed_data['unit'], 'total_needed': round(total_needed, 2), 'on_hand': round(inv_info['on_hand'], 2), 'allocated': round(inv_info['allocated'], 2), 'available': round(available, 2), 'net_needed': round(net_needed, 2)})
        needed_ids = set(totals_needed.keys())
        for inv_id, inv_info in inventory_levels.items():
            if inv_id not in needed_ids: report_data.append({'name': inv_info['name'], 'unit': inv_info['unit'], 'total_needed': 0, 'on_hand': round(inv_info['on_hand'], 2), 'allocated': round(inv_info['allocated'], 2), 'available': round(inv_info['available'], 2), 'net_needed': 0})
        sorted_report_data = sorted(report_data, key=lambda x: x['name'])
    except psycopg2.Error as e:
        flash(f"Error generating requirements report: {e}", "error"); print(f"DB Error requirements page: {e}")
    finally:
        if conn: conn.close()
    return render_template('requirements.html', report_data=sorted_report_data)

@app.route('/planner', methods=['GET', 'POST'])
def production_planner():
    conn = get_db_connection(); calculated_requirements = None; inventory_levels = {}; sellable_products = []
    try:
        resolved_cache.clear()
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute("SELECT id, name, unit, quantity_on_hand, quantity_allocated FROM inventory_items;");
            for item in cur.fetchall(): inventory_levels[item['id']] = {'name': item['name'], 'unit': item['unit'], 'on_hand': float(item.get('quantity_on_hand', 0)), 'allocated': float(item.get('quantity_allocated', 0)), 'available': float(item.get('quantity_on_hand', 0)) - float(item.get('quantity_allocated', 0))}
            cur.execute("""SELECT p.id, p.sku, p.jars_per_batch, r.id as recipe_id, r.name as recipe_name FROM products p JOIN recipes r ON p.recipe_id = r.id WHERE r.is_sold_product = TRUE AND p.jars_per_batch IS NOT NULL AND p.jars_per_batch > 0 ORDER BY r.name;"""); sellable_products = cur.fetchall()
        if request.method == 'POST':
            all_base_ingredients_run = []
            for product in sellable_products:
                jars_to_make_str = request.form.get(f"jars_product_{product['id']}");
                try: jars_to_make = int(jars_to_make_str) if jars_to_make_str else 0
                except ValueError: jars_to_make = 0
                if jars_to_make > 0:
                    batches_needed = math.ceil(jars_to_make / float(product['jars_per_batch'])); base_ingredients_one_batch = get_base_ingredients(product['recipe_id'], conn)
                    for ing in base_ingredients_one_batch: scaled_ing = dict(ing); scaled_ing['quantity'] = float(scaled_ing['quantity']) * batches_needed; all_base_ingredients_run.append(scaled_ing)
            totals_run_needed = defaultdict(lambda: {'name': '', 'unit': '', 'total_needed': 0, 'inventory_item_id': None})
            for ing in all_base_ingredients_run:
                inv_item_id = ing.get('inventory_item_id')
                if inv_item_id: name = ing.get('name', 'Unknown').strip(); unit = ing.get('unit', 'Unknown').strip(); key = (inv_item_id); totals_run_needed[key]['name'] = name; totals_run_needed[key]['unit'] = unit; totals_run_needed[key]['inventory_item_id'] = inv_item_id; totals_run_needed[key]['total_needed'] += float(ing.get('quantity', 0))
            run_report_data = []
            for inv_id, needed_data in totals_run_needed.items():
                inv_info = inventory_levels.get(inv_id, {'available': 0}); total_needed = needed_data['total_needed']; available = inv_info['available']; net_needed = max(0, total_needed - available)
                run_report_data.append({'name': needed_data['name'], 'unit': needed_data['unit'], 'total_needed': round(total_needed, 2), 'available': round(available, 2), 'net_needed': round(net_needed, 2)})
            calculated_requirements = sorted(run_report_data, key=lambda x: x['name'])
    except psycopg2.Error as e:
        flash(f"Error in production planner: {e}", "error"); print(f"DB Error planner page: {e}")
    finally:
        if conn: conn.close()
    return render_template('planner.html', products=sellable_products, requirements=calculated_requirements)

@app.route('/inventory', methods=['GET', 'POST'])
def inventory_items_page():
    conn = get_db_connection(); inventory_items = []
    try:
        if request.method == 'POST':
            name = request.form['name']; unit = request.form['unit']; qty_on_hand_str = request.form.get('quantity_on_hand')
            try: qty_on_hand = float(qty_on_hand_str) if qty_on_hand_str else 0.0
            except ValueError: qty_on_hand = 0.0
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM inventory_items WHERE LOWER(name) = LOWER(%s) AND LOWER(unit) = LOWER(%s);", (name, unit)); existing = cur.fetchone()
                if not existing: cur.execute("INSERT INTO inventory_items (name, unit, quantity_on_hand) VALUES (%s, %s, %s);", (name, unit, qty_on_hand)); conn.commit(); flash("Item added.", "success")
                else: flash('Item with this name/unit already exists.', 'warning')
            return redirect(url_for('inventory_items_page'))
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute("SELECT *, (quantity_on_hand - quantity_allocated) as quantity_available FROM inventory_items ORDER BY name;"); inventory_items = cur.fetchall()
    except psycopg2.Error as e:
        if conn and request.method == 'POST': conn.rollback()
        flash(f"Error accessing inventory: {e}", "error"); print(f"DB Error inventory page: {e}")
    finally:
        if conn: conn.close()
    return render_template('inventory_items.html', inventory_items=inventory_items)

@app.route('/inventory/edit/<int:id>', methods=['GET'])
def edit_inventory_item(id):
    conn = get_db_connection(); item = None
    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute("SELECT * FROM inventory_items WHERE id = %s;", (id,)); item = cur.fetchone()
        if item is None: flash(f"Item ID {id} not found.", "error"); return redirect(url_for('inventory_items_page'))
    except psycopg2.Error as e: flash(f"Error fetching item: {e}", "error"); print(f"DB Error edit inventory item GET {id}: {e}"); return redirect(url_for('inventory_items_page'))
    finally:
        if conn: conn.close()
    return render_template('edit_inventory_item.html', item=item)

@app.route('/inventory/update/<int:id>', methods=['POST'])
def update_inventory_item(id):
    conn = get_db_connection()
    try:
        name = request.form['name']; unit = request.form['unit']; qty_on_hand_str = request.form.get('quantity_on_hand')
        try: qty_on_hand = float(qty_on_hand_str) if qty_on_hand_str else 0.0
        except ValueError: qty_on_hand = 0.0
        with conn.cursor() as cur:
            cur.execute("UPDATE inventory_items SET name = %s, unit = %s, quantity_on_hand = %s WHERE id = %s;", (name, unit, qty_on_hand, id))
        conn.commit()
        flash("Item updated successfully.", "success")
    except psycopg2.Error as e: conn.rollback(); flash(f"Error updating item: {e}", "error"); print(f"DB Error update inventory item {id}: {e}")
    finally:
        if conn: conn.close()
    return redirect(url_for('inventory_items_page'))

@app.route('/inventory/delete/<int:id>', methods=['POST'])
def delete_inventory_item(id):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM inventory_items WHERE id = %s;", (id,))
        conn.commit(); flash("Item deleted.", "success")
    except psycopg2.Error as e: conn.rollback(); print(f"Error delete item {id}: {e}"); flash(f"Cannot delete item: {e.diag.message_primary}", "error")
    finally: conn.close(); return redirect(url_for('inventory_items_page'))

@app.route('/inventory/adjust/<int:id>', methods=['GET'])
def adjust_inventory_item(id):
    conn = get_db_connection(); item = None
    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute("SELECT * FROM inventory_items WHERE id = %s;", (id,))
            item = cur.fetchone()
        if item is None:
            flash(f"Inventory item ID {id} not found.", "error")
            return redirect(url_for('inventory_items_page'))
    except psycopg2.Error as e:
        flash(f"Error fetching item: {e}", "error")
        if conn: conn.close()
        return redirect(url_for('inventory_items_page'))
    finally:
        if conn: conn.close()
    return render_template('adjust_inventory_item.html', item=item)

@app.route('/inventory/process-adjustment/<int:id>', methods=['POST'])
def process_adjustment(id):
    conn = get_db_connection()
    try:
        adjustment_qty_str = request.form.get('adjustment_quantity')
        reason = request.form.get('reason') or "Manual Adjustment"
        
        if not adjustment_qty_str:
            flash("Adjustment quantity is required.", "error")
            return redirect(url_for('adjust_inventory_item', id=id))

        try:
            adjustment_quantity = float(adjustment_qty_str)
        except ValueError:
            flash("Invalid quantity. Please enter a number.", "error")
            return redirect(url_for('adjust_inventory_item', id=id))

        if adjustment_quantity == 0:
            flash("Adjustment quantity cannot be zero.", "warning")
            return redirect(url_for('adjust_inventory_item', id=id))

        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute(
                "SELECT quantity_on_hand FROM inventory_items WHERE id = %s FOR UPDATE;", (id,)
            )
            item = cur.fetchone()
            if item is None:
                flash("Item not found.", "error")
                raise Exception("Item not found during adjustment")
            
            cur.execute(
                "UPDATE inventory_items SET quantity_on_hand = quantity_on_hand + %s WHERE id = %s RETURNING quantity_on_hand;",
                (adjustment_quantity, id)
            )
            updated_qty = cur.fetchone()
                
            cur.execute(
                "INSERT INTO inventory_adjustments (inventory_item_id, adjustment_quantity, reason) VALUES (%s, %s, %s);",
                (id, adjustment_quantity, reason)
            )
        conn.commit()
        flash(f"Inventory adjusted by {adjustment_quantity}. New QOH: {round(updated_qty[0], 2)}", "success")

    except Exception as e:
        if conn: conn.rollback()
        if "negative stock" in str(e):
             flash(str(e), "error")
        else:
            flash(f"Error processing adjustment: {e}", "error")
        print(f"Error process adjustment {id}: {e}")
        return redirect(url_for('adjust_inventory_item', id=id))
    finally:
        if conn: conn.close()
    return redirect(url_for('inventory_items_page'))

@app.route('/wip', methods=['GET', 'POST'])
def wip_batches_page():
    conn = get_db_connection(); wip_batches = []; sellable_recipes = []
    try:
        if request.method == 'POST':
            recipe_id = request.form['recipe_id']; target_jars = request.form.get('target_jars') or 0
            try: 
                target_jars_int = int(target_jars);
                if target_jars_int > 0:
                    with conn.cursor() as cur: cur.execute("INSERT INTO wip_batches (recipe_id, target_jars, status) VALUES (%s, %s, %s);", (recipe_id, target_jars_int, 'In Progress')); conn.commit(); flash("WIP Batch started.", "success")
                else: flash('Target Jars must be positive.', 'error')
            except ValueError: flash('Invalid number for Target Jars.', 'error')
            if conn: conn.close(); return redirect(url_for('wip_batches_page'))
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute("""SELECT w.id, r.name as recipe_name, w.target_jars, w.status, w.created_at FROM wip_batches w JOIN recipes r ON w.recipe_id = r.id WHERE w.status = 'In Progress' ORDER BY w.created_at DESC;"""); wip_batches = cur.fetchall()
            cur.execute("""SELECT id, name FROM recipes WHERE is_sold_product = TRUE ORDER BY name;"""); sellable_recipes = cur.fetchall()
    except psycopg2.Error as e:
        if conn and request.method == 'POST': conn.rollback()
        flash(f"Error accessing WIP batches: {e}", "error"); print(f"DB Error WIP page: {e}")
    finally:
        if conn: conn.close()
    return render_template('wip_batches.html', wip_batches=wip_batches, sellable_recipes=sellable_recipes)

@app.route('/wip/<int:batch_id>')
def wip_batch_detail(batch_id):
    conn = get_db_connection(); batch = None; ingredient_summary = []; available_inventory_items = []
    try:
        resolved_cache.clear()
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute("""SELECT w.id, w.target_jars, w.status, w.created_at, r.id as recipe_id, r.name as recipe_name FROM wip_batches w JOIN recipes r ON w.recipe_id = r.id WHERE w.id = %s;""", (batch_id,)); batch = cur.fetchone()
            if not batch: flash(f"WIP Batch {batch_id} not found.", "error"); return redirect(url_for('wip_batches_page'))
            required_ingredients = defaultdict(lambda:{'name':'','unit':'','total_needed':0,'inventory_item_id':None})
            base_ingredients_one_batch = get_base_ingredients(batch['recipe_id'], conn)
            cur.execute("SELECT jars_per_batch FROM products WHERE recipe_id = %s LIMIT 1;", (batch['recipe_id'],)); product_info = cur.fetchone()
            if product_info and product_info['jars_per_batch'] and float(product_info['jars_per_batch']) > 0: batches_needed_float = float(batch['target_jars']) / float(product_info['jars_per_batch'])
            else: batches_needed_float = 1.0 # Or error
            for ing in base_ingredients_one_batch:
                inv_item_id = ing.get('inventory_item_id')
                if inv_item_id: name=ing.get('name','?').strip();unit=ing.get('unit','?').strip();key=(inv_item_id);required_ingredients[key]['name']=name;required_ingredients[key]['unit']=unit;required_ingredients[key]['inventory_item_id']=inv_item_id;required_ingredients[key]['total_needed']+=float(ing.get('quantity',0))*batches_needed_float
            cur.execute("""SELECT inventory_item_id, SUM(quantity_allocated) as total_allocated FROM wip_allocations WHERE wip_batch_id = %s GROUP BY inventory_item_id;""", (batch_id,)); allocations_raw = cur.fetchall()
            current_allocations = {alloc['inventory_item_id']: alloc['total_allocated'] for alloc in allocations_raw}
            for inv_id, req in required_ingredients.items():
                 allocated = float(current_allocations.get(inv_id, 0)); remaining = float(req['total_needed']) - allocated
                 ingredient_summary.append({ 'inventory_item_id': inv_id, 'name': req['name'], 'unit': req['unit'], 'needed': round(float(req['total_needed']), 2), 'allocated': round(allocated, 2), 'remaining': round(remaining, 2) })
            ingredient_summary.sort(key=lambda x: x['name'])
            cur.execute("SELECT id, name, unit, (quantity_on_hand - quantity_allocated) as available FROM inventory_items WHERE (quantity_on_hand - quantity_allocated) > 0 ORDER BY name;"); available_inventory_items = cur.fetchall()
    except psycopg2.Error as e: flash(f"Error fetching batch details: {e}", "error"); print(f"DB Error WIP detail {batch_id}: {e}"); return redirect(url_for('wip_batches_page'))
    finally:
        if conn: conn.close()
    return render_template('wip_batch_detail.html', batch=batch, ingredient_summary=ingredient_summary, inventory_items=available_inventory_items)

@app.route('/wip/<int:batch_id>/allocate', methods=['POST'])
def allocate_ingredient(batch_id):
    inventory_item_id = request.form.get('inventory_item_id'); quantity_str = request.form.get('quantity_allocated')
    conn = get_db_connection()
    if not inventory_item_id or not quantity_str:
        flash("Missing ingredient or quantity.", "error")
        if conn: conn.close()
        return redirect(url_for('wip_batch_detail', batch_id=batch_id))
    try:
        quantity = float(quantity_str);
        if quantity <= 0: raise ValueError("Quantity must be positive.")
    except ValueError:
        flash("Invalid quantity entered.", "error")
        if conn: conn.close()
        return redirect(url_for('wip_batch_detail', batch_id=batch_id))
    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute("SELECT quantity_on_hand, quantity_allocated, (quantity_on_hand - quantity_allocated) as available FROM inventory_items WHERE id = %s FOR UPDATE;", (inventory_item_id,)); result = cur.fetchone()
            if result is None: flash("Item not found.", "error"); conn.rollback()
            elif result['available'] < quantity: flash(f"Not enough stock (Available: {result['available']:.2f}). Failed.", "error"); conn.rollback()
            else: cur.execute("INSERT INTO wip_allocations (wip_batch_id, inventory_item_id, quantity_allocated) VALUES (%s, %s, %s);",(batch_id, inventory_item_id, quantity)); cur.execute("UPDATE inventory_items SET quantity_allocated = quantity_allocated + %s WHERE id = %s;", (quantity, inventory_item_id)); conn.commit(); flash("Allocated.", "success")
    except psycopg2.Error as e: conn.rollback(); flash(f"DB error: {e}", "error"); print(f"DB Error allocate batch {batch_id}: {e}")
    finally:
        if conn: conn.close()
    return redirect(url_for('wip_batch_detail', batch_id=batch_id))

@app.route('/wip/<int:batch_id>/complete', methods=['POST'])
def complete_wip_batch(batch_id):
    conn = get_db_connection();
    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute("SELECT status FROM wip_batches WHERE id = %s;", (batch_id,)); batch_status = cur.fetchone()
            if not batch_status or batch_status['status'] != 'In Progress': flash("Batch not found or already completed.", "error"); conn.rollback()
            else:
                cur.execute("SELECT inventory_item_id, SUM(quantity_allocated) as total_allocated FROM wip_allocations WHERE wip_batch_id = %s GROUP BY inventory_item_id;", (batch_id,)); allocations = cur.fetchall()
                for alloc in allocations: cur.execute("""UPDATE inventory_items SET quantity_on_hand = quantity_on_hand - %s, quantity_allocated = quantity_allocated - %s WHERE id = %s;""", (alloc['total_allocated'], alloc['total_allocated'], alloc['inventory_item_id']))
                cur.execute("UPDATE wip_batches SET status = 'Completed', completed_at = NOW() WHERE id = %s;", (batch_id,)); conn.commit(); flash(f"Batch {batch_id} completed.", "success")
    except psycopg2.Error as e: conn.rollback(); flash(f"DB error completing batch: {e}", "error"); print(f"DB Error complete batch {batch_id}: {e}")
    finally: conn.close(); return redirect(url_for('wip_batches_page'))

@app.route('/wip/delete/<int:batch_id>', methods=['POST'])
def delete_wip_batch(batch_id):
    conn = get_db_connection();
    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute("SELECT status FROM wip_batches WHERE id = %s;", (batch_id,)); batch_status = cur.fetchone()
            if not batch_status: flash("Batch not found.", "error"); conn.rollback()
            elif batch_status['status'] == 'Completed': flash("Cannot delete completed batch.", "error"); conn.rollback()
            else:
                cur.execute("SELECT inventory_item_id, SUM(quantity_allocated) as total_allocated FROM wip_allocations WHERE wip_batch_id = %s GROUP BY inventory_item_id;", (batch_id,)); allocations_to_reverse = cur.fetchall()
                for alloc in allocations_to_reverse: cur.execute("UPDATE inventory_items SET quantity_allocated = quantity_allocated - %s WHERE id = %s;", (alloc['total_allocated'], alloc['inventory_item_id']))
                cur.execute("DELETE FROM wip_batches WHERE id = %s;", (batch_id,)); conn.commit(); flash(f"WIP Batch {batch_id} deleted.", "success")
    except psycopg2.Error as e: conn.rollback(); flash(f"DB error deleting batch: {e}", "error"); print(f"DB Error delete batch {batch_id}: {e}")
    finally: conn.close(); return redirect(url_for('wip_batches_page'))

# --- SUPPLIER ROUTES ---
@app.route('/suppliers', methods=['GET'])
def suppliers_page():
    conn = get_db_connection(); suppliers = []
    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute("SELECT * FROM suppliers ORDER BY name;")
            suppliers = cur.fetchall()
    except psycopg2.Error as e:
        flash(f"Error fetching suppliers: {e}", "error"); print(f"DB Error suppliers page GET: {e}")
    finally:
        if conn: conn.close()
    return render_template('suppliers.html', suppliers=suppliers)

@app.route('/suppliers/add', methods=['POST'])
def add_supplier():
    conn = get_db_connection()
    try:
        name = request.form['name']; contact = request.form.get('contact_person'); email = request.form.get('email')
        phone = request.form.get('phone'); website = request.form.get('website'); notes = request.form.get('notes')
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM suppliers WHERE LOWER(name) = LOWER(%s);", (name,))
            existing = cur.fetchone()
            if existing:
                flash(f"Supplier '{name}' already exists.", "warning")
            else:
                cur.execute("INSERT INTO suppliers (name, contact_person, email, phone, website, notes) VALUES (%s, %s, %s, %s, %s, %s);",
                            (name, contact, email, phone, website, notes))
                conn.commit(); flash("Supplier added.", "success")
    except psycopg2.Error as e:
        if conn: conn.rollback()
        flash(f"Database error adding supplier: {e}", "error"); print(f"DB Error add supplier POST: {e}")
    finally:
        if conn: conn.close()
    return redirect(url_for('suppliers_page'))

@app.route('/suppliers/edit/<int:id>', methods=['GET'])
def edit_supplier(id):
    conn = get_db_connection(); supplier = None
    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute("SELECT * FROM suppliers WHERE id = %s;", (id,)); supplier = cur.fetchone()
        if supplier is None:
            flash(f"Supplier ID {id} not found.", "error")
            return redirect(url_for('suppliers_page'))
    except psycopg2.Error as e: flash(f"Error fetching supplier: {e}", "error"); print(f"DB Error edit supplier GET {id}: {e}"); return redirect(url_for('suppliers_page'))
    finally:
        if conn: conn.close()
    return render_template('edit_supplier.html', supplier=supplier)

@app.route('/suppliers/update/<int:id>', methods=['POST'])
def update_supplier(id):
    conn = get_db_connection()
    try:
        name = request.form['name']; contact = request.form.get('contact_person'); email = request.form.get('email')
        phone = request.form.get('phone'); website = request.form.get('website'); notes = request.form.get('notes')
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM suppliers WHERE LOWER(name) = LOWER(%s) AND id != %s;", (name, id))
            existing = cur.fetchone()
            if existing:
                 flash(f"Another supplier with the name '{name}' already exists.", "error")
                 return redirect(url_for('edit_supplier', id=id))
            else:
                cur.execute("""UPDATE suppliers SET name=%s, contact_person=%s, email=%s, phone=%s, website=%s, notes=%s WHERE id=%s;""",
                            (name, contact, email, phone, website, notes, id))
                conn.commit(); flash("Supplier updated.", "success")
    except psycopg2.Error as e:
        if conn: conn.rollback()
        flash(f"Database error updating supplier: {e}", "error"); print(f"DB Error update supplier POST {id}: {e}")
        return redirect(url_for('edit_supplier', id=id))
    finally:
        if conn: conn.close()
    return redirect(url_for('suppliers_page'))

@app.route('/suppliers/delete/<int:id>', methods=['POST'])
def delete_supplier(id):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur: cur.execute("DELETE FROM suppliers WHERE id = %s;", (id,)); conn.commit(); flash("Supplier deleted.", "success")
    except psycopg2.Error as e:
        if conn: conn.rollback()
        if hasattr(e, 'pgcode') and e.pgcode == '23503':
             flash(f"Cannot delete supplier: Linked to purchase orders.", "error")
        else: flash(f"Error deleting supplier: {e}", "error")
        print(f"DB Error delete supplier {id}: {e}")
    finally:
        if conn: conn.close()
    return redirect(url_for('suppliers_page'))
# --- END SUPPLIER ROUTES ---

# --- PURCHASE ORDER ROUTES ---
@app.route('/purchase-orders', methods=['GET', 'POST'])
def purchase_orders_page():
    conn = get_db_connection(); purchase_orders = []; suppliers = []
    try:
        if request.method == 'POST':
            supplier_id = request.form.get('supplier_id')
            order_date_str = request.form.get('order_date') or None
            expected_delivery_str = request.form.get('expected_delivery_date') or None
            
            order_date = datetime.strptime(order_date_str, '%Y-%m-%d').date() if order_date_str else datetime.now().date()
            expected_delivery = datetime.strptime(expected_delivery_str, '%Y-%m-%d').date() if expected_delivery_str else None

            if not supplier_id:
                flash("Supplier is required.", "error")
                raise ValueError("Supplier not provided")

            with conn.cursor(cursor_factory=DictCursor) as cur:
                cur.execute(
                    """INSERT INTO purchase_orders (supplier_id, order_date, expected_delivery_date, status)
                       VALUES (%s, %s, %s, %s) RETURNING id;""",
                    (supplier_id, order_date, expected_delivery, 'Placed')
                )
                new_po_id = cur.fetchone()['id']
                conn.commit()
                flash("Purchase Order created. Now add items.", "success")
                return redirect(url_for('po_detail', po_id=new_po_id))

        # GET Request Logic
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute("""
                SELECT po.id, s.name as supplier_name, po.order_date, po.expected_delivery_date, po.status
                FROM purchase_orders po JOIN suppliers s ON po.supplier_id = s.id
                ORDER BY po.status, po.order_date DESC, po.id DESC;
            """)
            purchase_orders = cur.fetchall()
            cur.execute("SELECT id, name FROM suppliers ORDER BY name;")
            suppliers = cur.fetchall()

    except (psycopg2.Error, ValueError) as e:
        if conn and request.method == 'POST': conn.rollback()
        flash(f"Error accessing purchase orders: {e}", "error")
        print(f"DB Error PO page: {e}")
        if request.method == 'POST' or not suppliers:
             try:
                 if not conn or conn.closed: conn = get_db_connection()
                 with conn.cursor(cursor_factory=DictCursor) as cur_err:
                    cur_err.execute("SELECT id, name FROM suppliers ORDER BY name;"); suppliers = cur_err.fetchall()
             except psycopg2.Error as e_inner:
                 print(f"DB Error fetching suppliers for PO form: {e_inner}")
    finally:
        if conn: conn.close()
    
    return render_template('purchase_orders.html', purchase_orders=purchase_orders, suppliers=suppliers, now=datetime.now())

@app.route('/po/<int:po_id>')
def po_detail(po_id):
    conn = get_db_connection(); po = None; items = []; inventory_items = []; total_cost = 0.0
    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute("""
                SELECT po.*, s.name as supplier_name
                FROM purchase_orders po JOIN suppliers s ON po.supplier_id = s.id
                WHERE po.id = %s;
            """, (po_id,))
            po = cur.fetchone()

            if not po:
                flash(f"Purchase Order ID {po_id} not found.", "error")
                return redirect(url_for('purchase_orders_page'))

            cur.execute("""
                SELECT poi.id, inv.name, inv.unit, poi.quantity_ordered, poi.unit_cost
                FROM purchase_order_items poi
                JOIN inventory_items inv ON poi.inventory_item_id = inv.id
                WHERE poi.purchase_order_id = %s ORDER BY inv.name;
            """, (po_id,))
            items = cur.fetchall()
            
            item_subtotal = sum(float(item['quantity_ordered'] or 0) * float(item['unit_cost'] or 0) for item in items)
            total_cost = (item_subtotal + float(po['shipping_cost'] or 0) + float(po['tax'] or 0)) - float(po['discount'] or 0)

            cur.execute("SELECT id, name, unit FROM inventory_items ORDER BY name;")
            inventory_items = cur.fetchall()
            
    except psycopg2.Error as e:
        flash(f"Error fetching PO details: {e}", "error")
        print(f"DB Error PO Detail page GET {po_id}: {e}")
        return redirect(url_for('purchase_orders_page'))
    finally:
        if conn: conn.close()

    return render_template('po_detail.html',
                           po=po,
                           items=items,
                           inventory_items=inventory_items,
                           item_subtotal=item_subtotal,
                           total_cost=total_cost)

@app.route('/po/<int:po_id>/add-item', methods=['POST'])
def po_add_item(po_id):
    conn = get_db_connection()
    try:
        inventory_item_id = request.form.get('inventory_item_id')
        quantity = request.form.get('quantity_ordered')
        unit_cost = request.form.get('unit_cost') or 0

        if not inventory_item_id or not quantity:
            flash("Item and quantity are required.", "error"); raise ValueError("Missing item/qty")
        try:
             quantity_float = float(quantity); unit_cost_float = float(unit_cost)
             if quantity_float <= 0: raise ValueError("Quantity must be positive.")
        except ValueError:
             flash("Invalid quantity or unit cost.", "error"); raise ValueError("Invalid numbers")

        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO purchase_order_items (purchase_order_id, inventory_item_id, quantity_ordered, unit_cost)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (purchase_order_id, inventory_item_id)
                DO UPDATE SET
                    quantity_ordered = purchase_order_items.quantity_ordered + EXCLUDED.quantity_ordered,
                    unit_cost = EXCLUDED.unit_cost; 
            """, (po_id, inventory_item_id, quantity_float, unit_cost_float))
        conn.commit()
        flash("Item added/updated successfully.", "success")
    except (psycopg2.Error, ValueError) as e:
        if conn: conn.rollback()
        flash(f"Error adding item: {e}", "error")
        print(f"DB Error PO add item {po_id}: {e}")
    finally:
        if conn: conn.close()
    return redirect(url_for('po_detail', po_id=po_id))

@app.route('/po/item/delete/<int:item_id>', methods=['POST'])
def po_remove_item(item_id):
    po_id = request.form.get('po_id')
    if not po_id:
        flash("Error: Missing Purchase Order ID.", "error")
        return redirect(url_for('purchase_orders_page'))
        
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute("SELECT status FROM purchase_orders WHERE id = %s;", (po_id,))
            po_status_row = cur.fetchone()
            if po_status_row and po_status_row['status'] == 'Received':
                flash("Cannot remove items from a received order.", "error")
            else:
                cur.execute("DELETE FROM purchase_order_items WHERE id = %s;", (item_id,))
                conn.commit()
                flash("Item removed from PO.", "success")
    except psycopg2.Error as e:
        if conn: conn.rollback()
        flash(f"Error removing item: {e}", "error")
        print(f"DB Error PO remove item {item_id}: {e}")
    finally:
        if conn: conn.close()
    return redirect(url_for('po_detail', po_id=po_id))

@app.route('/po/<int:po_id>/update-details', methods=['POST'])
def po_update_header(po_id):
    conn = get_db_connection()
    try:
        supplier_id = request.form.get('supplier_id')
        order_date_str = request.form.get('order_date') or None
        expected_delivery_str = request.form.get('expected_delivery_date') or None
        shipping_cost = request.form.get('shipping_cost') or 0
        tax = request.form.get('tax') or 0
        discount = request.form.get('discount') or 0
        notes = request.form.get('notes')
        new_status = request.form.get('status')
        
        order_date = datetime.strptime(order_date_str, '%Y-%m-%d').date() if order_date_str else None
        expected_delivery = datetime.strptime(expected_delivery_str, '%Y-%m-%d').date() if expected_delivery_str else None

        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute("SELECT status FROM purchase_orders WHERE id = %s;", (po_id,))
            current_status_row = cur.fetchone()
            if not current_status_row:
                flash("PO not found.", "error")
                raise Exception("PO not found")
            current_status = current_status_row['status']

            cur.execute("""
                UPDATE purchase_orders
                SET supplier_id = %s, order_date = %s, expected_delivery_date = %s,
                    shipping_cost = %s, tax = %s, discount = %s, notes = %s, status = %s
                WHERE id = %s;
            """, (supplier_id, order_date, expected_delivery, shipping_cost, tax, discount, notes, new_status, po_id))

            if new_status == 'Received' and current_status != 'Received':
                cur.execute("SELECT inventory_item_id, quantity_ordered FROM purchase_order_items WHERE purchase_order_id = %s;", (po_id,))
                items_to_receive = cur.fetchall()
                if not items_to_receive:
                     flash("Cannot mark empty order as Received.", "warning")
                     conn.rollback(); return redirect(url_for('po_detail', po_id=po_id))
                for item in items_to_receive:
                    cur.execute(
                        "UPDATE inventory_items SET quantity_on_hand = quantity_on_hand + %s WHERE id = %s;",
                        (item['quantity_ordered'], item['inventory_item_id'])
                    )
                cur.execute("UPDATE purchase_orders SET received_at = NOW() WHERE id = %s;", (po_id,))
                flash("Order marked as Received. Inventory updated.", "success")
            
            elif new_status != 'Received' and current_status == 'Received':
                cur.execute("SELECT inventory_item_id, quantity_ordered FROM purchase_order_items WHERE purchase_order_id = %s;", (po_id,))
                items_to_unreceive = cur.fetchall()
                for item in items_to_unreceive:
                    cur.execute(
                        "UPDATE inventory_items SET quantity_on_hand = quantity_on_hand - %s WHERE id = %s;",
                        (item['quantity_ordered'], item['inventory_item_id'])
                    )
                cur.execute("UPDATE purchase_orders SET received_at = NULL WHERE id = %s;", (po_id,))
                flash(f"Order status changed from Received. Inventory reversed.", "warning")
            
            else:
                 flash("PO details updated.", "success")
                 
        conn.commit()
            
    except Exception as e:
        if conn: conn.rollback()
        flash(f"Database error updating PO: {e}", "error")
        print(f"DB Error PO update {po_id}: {e}")
    finally:
        if conn: conn.close()
    return redirect(url_for('po_detail', po_id=po_id))

@app.route('/po/delete/<int:po_id>', methods=['POST'])
def po_delete(po_id):
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute("SELECT status, received_at FROM purchase_orders WHERE id = %s;", (po_id,))
            po = cur.fetchone()
            if not po:
                 flash("PO not found.", "error"); conn.rollback()
            else:
                 if po['status'] == 'Received' or po['received_at'] is not None:
                     cur.execute("SELECT inventory_item_id, quantity_ordered FROM purchase_order_items WHERE purchase_order_id = %s;", (po_id,))
                     items_to_unreceive = cur.fetchall()
                     for item in items_to_unreceive:
                         cur.execute(
                             "UPDATE inventory_items SET quantity_on_hand = quantity_on_hand - %s WHERE id = %s;",
                             (item['quantity_ordered'], item['inventory_item_id'])
                         )
                     flash_msg = "PO deleted. Inventory updates have been reversed."
                 else:
                     flash_msg = "PO deleted."
                 
                 cur.execute("DELETE FROM purchase_orders WHERE id = %s;", (po_id,))
                 conn.commit()
                 flash(flash_msg, "success")
    except psycopg2.Error as e:
        if conn: conn.rollback()
        flash(f"Error deleting PO: {e.diag.message_primary}", "error")
        print(f"DB Error PO delete {po_id}: {e}")
    finally:
        if conn: conn.close()
    return redirect(url_for('purchase_orders_page'))
# --- END PURCHASE ORDER ROUTES ---


if __name__ == '__main__':
    app.run(debug=True)