import os
import psycopg2 as pg2
import multiprocessing as mp
from dotenv import load_dotenv

load_dotenv()

POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
POSTGRES_DB = os.getenv('POSTGRES_DB')
HOST = 'localhost'
PORT = 5432

conn = pg2.connect(host = HOST, user=POSTGRES_USER, password=POSTGRES_PASSWORD, port=PORT, database=POSTGRES_DB)
cur = conn.cursor()
conn.set_session(autocommit=True)
with open(f'./data/low_concurrency/metadata.sql', 'r') as f:
    metadata = f.read()
cur.execute(metadata)
cur.execute('SELECT * from infrastructure_type')
for record in cur:
    print(record)
conn.set_session(autocommit=False)

