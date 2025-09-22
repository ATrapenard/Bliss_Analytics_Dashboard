import sqlite3

# Establish a connection to the SQLite database
# If the database does not exist, it will be created
connection = sqlite3.connect('database.db')

# Open the schema.sql file and execute the SQL commands to set up the database schema
with open('schema.sql') as f:
    connection.executescript(f.read())

# Create a cursor object to execute SQL commands
cur = connection.cursor()