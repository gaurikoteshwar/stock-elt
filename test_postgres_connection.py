import psycopg2

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="stock_etl",
    user="postgres",
    password="lily1234"
)

cur = conn.cursor()
cur.execute("SELECT version();")
version = cur.fetchone()

print("Connected to:", version)

cur.close()
conn.close()