import os
import argparse
import psycopg2 as pg2
import multiprocessing as mp
from dotenv import load_dotenv
from datetime import datetime

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
    conn.close()
    return

def parseInsertFile(fname):
    with open(fname, 'r') as f:
        data = f.readlines()
    data_time = []
    for lines in data:
        if "INSERT" in lines:
            inst_time = lines.split(',')[-2].replace("'", "").strip()
            inst_timestamp = int(datetime.strptime(inst_time, "%Y-%m-%d %H:%M:%S").timestamp())
            data_time.append((inst_timestamp, inst_time, lines.strip()))
    return data_time

def parseQueryFile(fname):
    with open(fname, 'r') as f:
        data = f.read()
    data = data.split("\"")
    print(data)
    print(len(data))

def preprocessWorkload(qfname, ofname, sfname, transaction_size):
    qry_data = parseQueryFile(qfname)
    obs_data = parseInsertFile(ofname)
    sem_data = parseInsertFile(sfname)
    return

def main():
    parser = argparse.ArgumentParser(description="Transaction simulator")
    parser.add_argument('-c', '--conc', default='low', help="Concurrency level")
    parser.add_argument('-m', '--mpl', default=1, help="Number of active transactions")
    parser.add_argument('-n', '--nsize', default=4, help="Size of transaction")
    args = parser.parse_args()
    assert args.conc == 'high' or args.conc == 'low'
    assert args.mpl > 0
    
    metadata_filename = f'./data/{args.conc}_concurrency/metadata.sql'
    query_filename = f'./queries/{args.conc}_concurrency/queries.txt'
    obs_filename = f'./data/{args.conc}_concurrency/observation_{args.conc}_concurrency.sql'
    semobs_filename = f'./data/{args.conc}_concurrency/semantic_observation_{args.conc}_concurrency.sql'

    # Insert Metadata
    #insertMetadata(metadata_filename)
    # Preprocess workload
    preprocessWorkload(query_filename, obs_filename, semobs_filename, args.nsize)
    # Spawn worker processes

if __name__ == "__main__":
    main()