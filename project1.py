import os
import argparse
import psycopg2 as pg2
import multiprocessing as mp
from dotenv import load_dotenv

load_dotenv()

POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
POSTGRES_DB = os.getenv('POSTGRES_DB')
HOST = 'localhost'
PORT = 5432

# Global Settings
observation_tables = ['wifiapobservation', 'wemoobservation', 'thermometerobservation']
semantic_tables = ['presence', 'occupancy']
# use with conn.set_session(..., isolation_level=" ")
isolation_levels = ["READ UNCOMMITTED", "READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"]

def getConnection():
    try:
        conn = pg2.connect(host=HOST, user=POSTGRES_USER, password=POSTGRES_PASSWORD, port=PORT, dbname=POSTGRES_DB)
    except:
        raise Exception("Error connecting to PostgresDB.")
    return conn

def insertMetadata(fname):
    conn = getConnection()
    conn.set_session(autocommit=True)
    with conn.cursor() as cur:
        with open(fname, 'r') as f:
            metadata = f.read()
        cur.execute(metadata)
        cur.execute('SELECT * from infrastructure_type')
        for record in cur:
            print(record)
    conn.close()
    return

def main():
    parser = argparse.ArgumentParser(description="Transaction simulator")
    parser.add_argument('-c', '--conc', default='low', help="Concurrency level")
    parser.add_argument('-n', '--mpl', default=1, help="Number of active transactions")
    args = parser.parse_args()
    assert args.conc == 'high' or args.conc == 'low'
    assert args.mpl > 0
    

    metadata_filename = f'./data/{args.conc}_concurrency/metadata.sql'
    query_filename = f'./queries/{args.conc}_concurrency/queries.txt'
    obs_filename = f'./data/{args.conc}_concurrency/observation_{args.conc}_concurrency.sql'
    semobs_filename = f'./data/{args.conc}_concurrency/semantic_observation_{args.conc}_concurrency.sql'

    # Insert Metadata
    insertMetadata(metadata_filename)
    # Preprocess workload
    # Spawn worker processes

if __name__ == "__main__":
    main()