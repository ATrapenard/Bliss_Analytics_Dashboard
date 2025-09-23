# app.py

import os
import psycopg2
from psycopg2.extras import DictCursor
from dotenv import load_dotenv
from flask import Flask, render_template, request, redirect, url_for

# Load environment variables from .env file
load_dotenv()

app = Flask(__name__)

def get_db_connection():
    """Establishes a connection to the PostgreSQL database."""
    conn_string = os.getenv("DATABASE_URL")
    conn = psycopg2.connect(conn_string)
    return conn

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/recipes')
def recipe_dashboard():
    recipes_list = []
    conn = get_db_connection()
    with conn.cursor(cursor_factory=DictCursor) as cur:
        # Get all base recipes
        cur.execute('SELECT * FROM recipes ORDER BY name;')
        recipes_from_db = cur.fetchall()
        
        for recipe in recipes_from_db:
            recipe_dict = dict(recipe)
            
            # Get the list of individual ingredients for the collapsible view
            cur.execute('SELECT name, quantity, unit FROM ingredients WHERE recipe_id = %s;', (recipe['id'],))
            ingredients = cur.fetchall()
            recipe_dict['ingredients'] = [dict(ing) for ing in ingredients]

            # --- NEW: Calculate totals for grams and mLs ---
            cur.execute("""
                SELECT unit, SUM(quantity) as total_quantity
                FROM ingredients
                WHERE recipe_id = %s AND unit IN ('grams', 'mLs')
                GROUP BY unit;
            """, (recipe['id'],))
            
            totals_data = cur.fetchall()
            # Initialize totals and then fill with data from the query
            totals = {'grams': 0, 'mLs': 0}
            for row in totals_data:
                if row['unit'] == 'grams':
                    totals['grams'] = round(row['total_quantity'], 2)
                elif row['unit'] == 'mLs':
                    totals['mLs'] = round(row['total_quantity'], 2)
            
            recipe_dict['totals'] = totals
            # --- END OF NEW CODE ---

            recipes_list.append(recipe_dict)
            
    conn.close()
    return render_template('recipes.html', recipes=recipes_list)

@app.route('/new')
def new_recipe_form():
    return render_template('add_recipe.html')

@app.route('/create', methods=['POST'])
def create_recipe():
    conn = get_db_connection()
    with conn.cursor() as cur:
        recipe_name = request.form['recipe_name']
        cur.execute('INSERT INTO recipes (name) VALUES (%s) RETURNING id;', (recipe_name,))
        recipe_id = cur.fetchone()[0]

        ingredient_names = request.form.getlist('ingredient_name')
        quantities = request.form.getlist('quantity')
        units = request.form.getlist('unit')

        for i in range(len(ingredient_names)):
            cur.execute('INSERT INTO ingredients (recipe_id, name, quantity, unit) VALUES (%s, %s, %s, %s);',
                         (recipe_id, ingredient_names[i], quantities[i], units[i]))
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
    conn.close()
    return render_template('edit_recipe.html', recipe=recipe, ingredients=ingredients)

@app.route('/update/<int:recipe_id>', methods=['POST'])
def update_recipe(recipe_id):
    conn = get_db_connection()
    with conn.cursor() as cur:
        new_name = request.form['recipe_name']
        cur.execute('UPDATE recipes SET name = %s WHERE id = %s;', (new_name, recipe_id))

        cur.execute('DELETE FROM ingredients WHERE recipe_id = %s;', (recipe_id,))

        ingredient_names = request.form.getlist('ingredient_name')
        quantities = request.form.getlist('quantity')
        units = request.form.getlist('unit')

        for i in range(len(ingredient_names)):
            cur.execute('INSERT INTO ingredients (recipe_id, name, quantity, unit) VALUES (%s, %s, %s, %s);',
                         (recipe_id, ingredient_names[i], quantities[i], units[i]))
    conn.commit()
    conn.close()
    return redirect(url_for('recipe_dashboard'))

@app.route('/delete/<int:recipe_id>', methods=['POST'])
def delete_recipe(recipe_id):
    conn = get_db_connection()
    with conn.cursor() as cur:
        # The ON DELETE CASCADE in our table schema handles deleting the ingredients
        cur.execute('DELETE FROM recipes WHERE id = %s;', (recipe_id,))
    conn.commit()
    conn.close()
    return redirect(url_for('recipe_dashboard'))

@app.route('/totals')
def ingredient_totals():
    conn = get_db_connection()
    with conn.cursor(cursor_factory=DictCursor) as cur:
        # This query groups by both name and unit to get the total for each
        cur.execute("""
            SELECT name, unit, SUM(quantity) as total_quantity
            FROM ingredients
            GROUP BY name, unit
            ORDER BY name, unit;
        """)
        totals = cur.fetchall()
    conn.close()
    return render_template('totals.html', totals=totals)

@app.route('/products')
def products_page():
    conn = get_db_connection()
    with conn.cursor(cursor_factory=DictCursor) as cur:
        # Join products with recipes to get the recipe name
        cur.execute("""
            SELECT p.id, p.sku, r.name as recipe_name
            FROM products p
            LEFT JOIN recipes r ON p.recipe_id = r.id
            ORDER BY p.sku;
        """)
        products = cur.fetchall()
        
        # Also fetch all recipes for the "Add New Product" dropdown
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
        cur.execute('INSERT INTO products (sku, recipe_id) VALUES (%s, %s);',
                     (sku, recipe_id))
    conn.commit()
    conn.close()
    
    return redirect(url_for('products_page'))

if __name__ == '__main__':
    app.run(debug=True)