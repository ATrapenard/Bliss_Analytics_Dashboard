# app.py
import sqlite3
from flask import Flask, render_template, request, redirect, url_for

# Initialize the Flask application
app = Flask(__name__)

# Helper function to get a database connection
def get_db_connection():
    conn = sqlite3.connect('database.db')
    conn.row_factory = sqlite3.Row  # This allows us to access columns by name
    return conn

# Define the route for the landing page
@app.route('/')
def home():
    """Renders the landing page."""
    # This function looks for 'index.html' in the 'templates' folder
    return render_template('index.html')
@app.route('/recipes')
def recipe_dashboard():
    """Renders the recipe dashboard page."""
    conn = get_db_connection()
    recipes_from_db = conn.execute('SELECT * FROM recipes').fetchall()
    recipes_list = []
    for recipe in recipes_from_db:
        recipe_dict = dict(recipe)
        ingredients = conn.execute('SELECT name, quantity, unit FROM ingredients WHERE recipe_id = ?', (recipe['id'],)).fetchall()
        recipe_dict['ingredients'] = ingredients
        recipes_list.append(recipe_dict)
    conn.close()
    return render_template('recipes.html', recipes=recipes_list)

@app.route('/add', methods=['POST'])
def add_recipe():
    name = request.form['name']
    ingredients = request.form['ingredients']
    conn = get_db_connection()
    conn.execute('INSERT INTO recipes (name, ingredients) VALUES (?, ?)', (name, ingredients))
    conn.commit()
    conn.close()
    return redirect(url_for('recipe_dashboard'))

@app.route('/new')
def new_recipe_form():
    return render_template('add_recipe.html')

@app.route('/create', methods=['POST'])
def create_recipe():
    conn = get_db_connection()
    recipe_name = request.form['recipe_name']
    cur = conn.cursor()
    cur.execute('INSERT INTO recipes (name) VALUES (?)', (recipe_name,))
    recipe_id = cur.lastrowid
    ingredient_names = request.form.getlist('ingredient_name')
    quantities = request.form.getlist('quantity')
    units = request.form.getlist('unit')

    for i in range(len(ingredient_names)):
        conn.execute('INSERT INTO ingredients (recipe_id, name, quantity, unit) VALUES (?, ?, ?, ?)', (recipe_id, ingredient_names[i], quantities[i], units[i]))
    conn.commit()
    conn.close()
    return redirect(url_for('recipe_dashboard'))

@app.route('/edit/<int:recipe_id>')
def edit_recipe_form(recipe_id):
    conn = get_db_connection()
    recipe = conn.execute('SELECT * FROM recipes WHERE id = ?', (recipe_id,)).fetchone()
    ingredients = conn.execute('SELECT * FROM ingredients WHERE recipe_id = ?', (recipe_id,)).fetchall()
    conn.close()
    return render_template('edit_recipe.html', recipe=recipe, ingredients=ingredients)

@app.route('/update/<int:recipe_id>', methods=['POST'])
def update_recipe(recipe_id):
    conn = get_db_connection()
    new_name = request.form['recipe_name']
    conn.execute('UPDATE recipes SET name = ? WHERE id = ?', (new_name, recipe_id))
    conn.execute('DELETE FROM ingredients WHERE recipe_id = ?', (recipe_id,))
    ingredient_names = request.form.getlist('ingredient_name')
    quantities = request.form.getlist('quantity')
    units = request.form.getlist('unit')

    for i in range(len(ingredient_names)):
        conn.execute('INSERT INTO ingredients (recipe_id, name, quantity, unit) VALUES (?, ?, ?, ?)', (recipe_id, ingredient_names[i], quantities[i], units[i]))
    conn.commit()
    conn.close()
    return redirect(url_for('recipe_dashboard'))

# This allows you to run the app directly
if __name__ == '__main__':
    app.run(debug=True)