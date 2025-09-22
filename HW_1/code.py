import sqlite3
import pandas as pd

DB_FILE = "database.sqlite"
try:
    con = sqlite3.connect(DB_FILE)
    print("Connection successful")
except Exception as e:
    print(f"{e}")


# --- Task 1 ---
tables = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table';", con)

print("\n")
print("Tables in the database:")
print("\n")
print(tables)

for table in ["follows", "users", "sqlite_sequence", "reactions", "comments", "posts"]:
    print("\n")
    print("\n")
    print(f"{table} information:")
    print("\n")
    table_info = pd.read_sql_query(f"PRAGMA table_info('{table}');", con)
    print(table_info)

    print("\n")
    table_content = pd.read_sql_query(f"SELECT * FROM {table};", con)
    print(table_content)

# --- Task 2 ---
try:
    lurkers = pd.read_sql_query(f"SELECT users.id, users.username FROM users WHERE users.id NOT IN (SELECT posts.user_id FROM posts) AND users.id NOT IN (SELECT reactions.user_id FROM reactions);", con)

    print("\n")
    print("List of lurkers in the database:")
    print("\n")
    print(lurkers)
except Exception as e:
    print(f"An unexpected error occurred: {e}")

# --- Task 3 ---
try:
    top_5_influencers = pd.read_sql_query("""
    SELECT
        users.id,
        users.username,
        COUNT(reactions.id) AS total_reactions
    FROM
        users
    LEFT JOIN
        posts
    ON
        users.id = posts.user_id
    LEFT JOIN
        reactions
    ON
        posts.id = reactions.post_id
    GROUP BY
        users.id, users.username
    ORDER BY
        total_reactions DESC
    LIMIT 5;
    """, con)
    print("\n")
    print("Top 5 users who have most reactions compined on their posts:")
    print("\n")
    print(top_5_influencers)
except Exception as e:
    print(f"An unexpected error occurred: {e}")


# --- Task 4 ---



con.close()