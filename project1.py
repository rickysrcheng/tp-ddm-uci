import os
import argparse
import time
import random
import psycopg2 as pg2
import multiprocessing as mp
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()
random.seed(1234)
# Postgres Global Config
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

# def worker(transaction_q, result_q, )

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
    for line in data:
        if "INSERT" in line:
            inst_time = line.split(',')[-2].replace("'", "").strip()
            inst_timestamp = int(datetime.strptime(inst_time, "%Y-%m-%d %H:%M:%S").timestamp())
            data_time.append((inst_timestamp, inst_time, line.strip()))
    return data_time

def parseQueryFile(fname):
    with open(fname, 'r') as f:
        data = f.read().split("\"")
    data_time = []
    for i in range(0, len(data)-1, 2):
        inst_time = data[i].replace("\n", "").replace("\t", "").replace("T", " ").replace("Z,", "")
        inst_timestamp = int(datetime.strptime(inst_time, "%Y-%m-%d %H:%M:%S").timestamp())
        inst = data[i+1].replace("\n", "")
        data_time.append((inst_timestamp, inst_time, inst))
    return data_time
    
def preprocessWorkload(qfname, ofname, sfname, transaction_size):
    start = time.time()
    qry_data = parseQueryFile(qfname)
    obs_data = parseInsertFile(ofname)
    sem_data = parseInsertFile(sfname)
    print(time.time() - start)

    workload_data = qry_data + obs_data + sem_data
    random.shuffle(workload_data)
    workload_data = sorted(workload_data, key=lambda x: x[0])

    # rescale each day into 60 seconds
    # 24 minutes in original time = 1 second in new timescale
    RESCALE_FACTOR = 60*24 
    min_timestamp = workload_data[0][0]

    # since there are thousands of queries in a day, scale down a day by seconds
    for i in range(len(workload_data)):
        new_timestamp = (workload_data[i][0] - min_timestamp) / RESCALE_FACTOR
        workload_data[i] = tuple([new_timestamp, workload_data[i][1], workload_data[i][2]])
    
    random.shuffle(qry_data)
    qry_data = sorted(qry_data, key=lambda x: x[0])
    min_timestamp = qry_data[0][0]

    for i in range(len(qry_data)):
        new_timestamp = (qry_data[i][0] - min_timestamp) / RESCALE_FACTOR
        qry_data[i] = tuple([new_timestamp, qry_data[i][1], qry_data[i][2]])
    return workload_data, qry_data

def main():
    parser = argparse.ArgumentParser(description="Transaction simulator")
    parser.add_argument('-c', '--conc', default='low', help="Concurrency level")
    parser.add_argument('-m', '--mpl', default=1, help="Number of active transactions")
    parser.add_argument('-n', '--nsize', default=4, help="Size of transaction")
    args = parser.parse_args()
    assert args.conc == 'high' or args.conc == 'low'
    assert args.mpl > 0
    assert args.nsize > 0
    
    metadata_filename = f'./data/{args.conc}_concurrency/metadata.sql'
    query_filename = f'./queries/{args.conc}_concurrency/queries.txt'
    obs_filename = f'./data/{args.conc}_concurrency/observation_{args.conc}_concurrency.sql'
    semobs_filename = f'./data/{args.conc}_concurrency/semantic_observation_{args.conc}_concurrency.sql'

    # Insert Metadata
    # insertMetadata(metadata_filename)
    # Preprocess workload
    workload, qry_workload = preprocessWorkload(query_filename, obs_filename, semobs_filename, args.nsize)
    # Spawn worker processes
    # processes = []
    # for i in range(args.mpl):
    #     processes.append(mp.Process())

if __name__ == "__main__":
    main()