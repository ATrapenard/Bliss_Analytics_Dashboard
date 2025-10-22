import os
import psycopg2
from psycopg2.extras import DictCursor
from dotenv import load_dotenv
from flask import Flask, render_template, request, redirect, url_for

load_dotenv()
app = Flask(__name__)

def get_db_connection():
    conn_string = os.getenv("DATABASE_URL")
    conn = psycopg2.connect(conn_string)
    return conn

# --- RECURSIVE LOGIC (USED BY MULTIPLE ROUTES) ---
resolved_cache = {}

def get_base_ingredients(recipe_id, conn):
    """
    Recursively resolves a recipe down to its base raw ingredients.
    """
    if recipe_id in resolved_cache:
        return resolved_cache[recipe_id]

    with conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute("SELECT * FROM ingredients WHERE recipe_id = %s;", (recipe_id,))
        ingredients = cur.fetchall()
        
        base_ingredients = []
        for ing in ingredients:
            if ing['sub_recipe_id']:
                cur.execute("SELECT yield_quantity FROM recipes WHERE id = %s;", (ing['sub_recipe_id'],))
                sub_recipe_yield = cur.fetchone()['yield_quantity']
                
                if not sub_recipe_yield or sub_recipe_yield == 0:
                    scaling_ratio = 1.0
                else:
                    scaling_ratio = float(ing['quantity']) / float(sub_recipe_yield)

                sub_ingredients = get_base_ingredients(ing['sub_recipe_id'], conn)
                for sub_ing in sub_ingredients:
                    scaled_ing = dict(sub_ing)
                    scaled_ing['quantity'] *= scaling_ratio
                    base_ingredients.append(scaled_ing)
            else:
                base_ingredients.append(dict(ing))
    
    resolved_cache[recipe_id] = base_ingredients
    return base_ingredients

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

            base_ingredients = get_base_ingredients(recipe['id'], conn)
            
            totals = {'grams': 0, 'mLs': 0}
            for ing in base_ingredients:
                unit = ing.get('unit', '').lower()
                if unit == 'grams':
                    totals['grams'] += float(ing['quantity'])
                elif unit == 'mls':
                    totals['mLs'] += float(ing['quantity'])
            
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
        yield_quantity = request.form['yield_quantity'] or None
        yield_unit = request.form['yield_unit'] or None

        cur.execute('INSERT INTO recipes (name, yield_quantity, yield_unit) VALUES (%s, %s, %s) RETURNING id;', 
                    (recipe_name, yield_quantity, yield_unit))
        recipe_id = cur.fetchone()[0]
        
        ingredient_names = request.form.getlist('ingredient_name')
        quantities = request.form.getlist('quantity')
        units = request.form.getlist('unit')
        sub_recipe_ids = request.form.getlist('sub_recipe_id')
        
        for i in range(len(ingredient_names)):
            sub_recipe_id = sub_recipe_ids[i] if sub_recipe_ids[i] else None
            cur.execute(
                'INSERT INTO ingredients (recipe_id, name, quantity, unit, sub_recipe_id) VALUES (%s, %s, %s, %s, %s);',
                (recipe_id, ingredient_names[i], quantities[i], units[i], sub_recipe_id)
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
        yield_quantity = request.form['yield_quantity'] or None
        yield_unit = request.form['yield_unit'] or None

        cur.execute('UPDATE recipes SET name = %s, yield_quantity = %s, yield_unit = %s WHERE id = %s;', 
                    (new_name, yield_quantity, yield_unit, recipe_id))
        
        cur.execute('DELETE FROM ingredients WHERE recipe_id = %s;', (recipe_id,))
        
        ingredient_names = request.form.getlist('ingredient_name')
        quantities = request.form.getlist('quantity')
        units = request.form.getlist('unit')
        sub_recipe_ids = request.form.getlist('sub_recipe_id')

        for i in range(len(ingredient_names)):
            sub_recipe_id = sub_recipe_ids[i] if sub_recipe_ids[i] else None
            cur.execute(
                'INSERT INTO ingredients (recipe_id, name, quantity, unit, sub_recipe_id) VALUES (%s, %s, %s, %s, %s);',
                (recipe_id, ingredient_names[i], quantities[i], units[i], sub_recipe_id)
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
            
    totals = {}
    for ing in all_base_ingredients:
        key = (ing['name'].strip().lower(), ing['unit'].strip().lower())
        if key not in totals:
            totals[key] = {'name': ing['name'], 'unit': ing['unit'], 'total_quantity': 0}
        totals[key]['total_quantity'] += float(ing['quantity'])
        
    conn.close()
    sorted_totals = sorted(totals.values(), key=lambda x: x['name'])
    
    return render_template('totals.html', totals=sorted_totals)

@app.route('/products')
def products_page():
    conn = get_db_connection()
    with conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute("""
            SELECT p.id, p.sku, r.name as recipe_name
            FROM products p
            LEFT JOIN recipes r ON p.recipe_id = r.id
            ORDER BY p.sku;
        """)
        products = cur.fetchall()
        cur.execute("SELECT id, name FROM recipes ORDER BY name;")
        recipes = cur.fetchall()
    conn.close()
    return render_template('products.html', products=products, recipes=recipes)

@app.route('/products/add', methods=['POST'])
def add_product():
    sku = request.form['sku']
    recipe_id = request.form['recipe_id']
    conn = get_db_connection()
    with conn.cursor() as cur:
        cur.execute('INSERT INTO products (sku, recipe_id) VALUES (%s, %s);', (sku, recipe_id))
    conn.commit()
    conn.close()
    return redirect(url_for('products_page'))

# --- NEW LOCATIONS ROUTES ---

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

    # GET Request
    with conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute("SELECT * FROM locations ORDER BY name;")
        locations = cur.fetchall()
    conn.close()
    return render_template('locations.html', locations=locations)

@app.route('/locations/delete/<int:id>', methods=['POST'])
def delete_location(id):
    conn = get_db_connection()
    with conn.cursor() as cur:
        # We should add logic here later to check if locations are in use
        cur.execute("DELETE FROM locations WHERE id = %s;", (id,))
    conn.commit()
    conn.close()
    return redirect(url_for('locations_page'))

# --- END NEW ROUTES ---

if __name__ == '__main__':
    app.run(debug=True)