import os
import argparse
import time
import random
import queue
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
TERMINATE_PROCESS = (-1, -1, "TERMINATE")

def worker(transaction_q, result_q, iso_level):
    conn = getConnection()

    conn.set_session(isolation_level=iso_level, autocommit=False)
    
    terminate = False
    results = {'Response Time': []}
    total_time = 0
    with conn.cursor() as cur:
        while True:
            try:
                transaction = transaction_q.get_nowait()
                if transaction == TERMINATE_PROCESS:
                    break
                else:
                    response_start = time.time()
                    cur.execute(transaction[2])
                    cur.fetchall()
                    conn.commit()
                    response = time.time() - response_start
                    results["Response Time"].append(response)
            except mp.Queue.Empty:
                pass
    result_q.put_nowait(results)
    conn.close()

def workerIdea3(transactions, result_q, iso_level, barrier, start_time, id):
    conn = getConnection()
    conn.set_session(isolation_level=iso_level, autocommit=False)
    barrier.wait()
    terminate = False
    results = {'Response Time': []}
    total_time = 0
    with conn.cursor() as cur:
        while time.time() < start_time:
            continue
        #print(f"Process {id} starting")
        for transaction in transactions:
            while time.time() - start_time < transaction[0]:
                continue
            response_start = time.time()
            cur.execute(transaction[2])
            if cur.description != None:
                cur.fetchall()
            conn.commit()
            response = time.time() - response_start
            result_q.put_nowait(response)
            # results["Response Time"].append(response)
    #print(f"Process {id} Ended")
    #result_q.put_nowait(-1)
    conn.close()

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
        inst = data[i+1].replace("\n", " ").replace("\t", " ") + ";"
        data_time.append((inst_timestamp, inst_time, inst))
    return data_time
    
def preprocessWorkload(qfname, ofname, sfname, transaction_size):
    """
        Preprocess the workload. Output is already batched into transactions of size N
    """
    start = time.time()
    qry_data = parseQueryFile(qfname)
    obs_data = parseInsertFile(ofname)
    sem_data = parseInsertFile(sfname)

    workload_data = qry_data + obs_data + sem_data
    random.shuffle(workload_data)
    workload_data = sorted(workload_data, key=lambda x: x[0])

    # rescale each day into 60 seconds
    # 24 minutes in original time = 1 second in new timescale
    RESCALE_FACTOR = 60*24 
    min_timestamp = workload_data[0][0]

    # since there are thousands of queries in a day, scale down a day by seconds
    workload_transactions = []
    for i in range(0, len(workload_data), transaction_size):
        workload_batch = [workload_data[j + i] for j in range(transaction_size) if j + i < len(workload_data)]
        new_timestamp = (workload_batch[-1][0] - min_timestamp) / RESCALE_FACTOR
        workload_inst = ""
        for unit in workload_batch:
            workload_inst += unit[2] + " "
        
        workload_transactions.append(tuple([new_timestamp, workload_batch[-1][1], workload_inst]))
    random.shuffle(qry_data)
    qry_data = sorted(qry_data, key=lambda x: x[0])
    min_timestamp = qry_data[0][0]

    for i in range(len(qry_data)):
        new_timestamp = (qry_data[i][0] - min_timestamp) / RESCALE_FACTOR
        qry_data[i] = tuple([new_timestamp, qry_data[i][1], qry_data[i][2]])
    print(f"Finished preprocessing in {time.time() - start:.2f}")
    return workload_transactions, qry_data

def idea1Parallel():
    pass

def idea3Parallel(workload, mpl, nsize, iso_level):
    worker_transactions = [[] for i in range(mpl)]
    worker = 0
    for i in range(len(workload)):
        worker_transactions[worker].append(workload[i])
        worker += 1
        worker %= mpl
    
    result_queue = mp.Queue()
    barrier = mp.Barrier(mpl + 1)
    processes = []
    start_time = time.time() + 15
    for i in range(mpl):
        processes.append(mp.Process(target=workerIdea3, args=(worker_transactions[i], result_queue, iso_level, barrier, start_time, i)))

    for proc in processes:
        proc.start()
    time.sleep(2)
    start = time.time()
    barrier.wait()

    print("Starting Simulation")
    num_transactions = 0
    total_time = 0
    min_response = 10000000000000000
    max_response = -1
    num_terminate = 0
    while num_terminate != mpl:
        try:
            response_time = result_queue.get_nowait()
            if response_time == -1:
                num_terminate += 1
            else:
                num_transactions += 1
                total_time += response_time
                min_response = min(min_response, response_time)
                max_response = max(max_response, response_time)
                if num_transactions % 100000 == 0:
                    print('----------------------------')
                    print(f"Time Elapsed: {time.time() - start}")
                    print(f"Total Txn Time: {total_time}")
                    print(f"Num Txns: {num_transactions}")
                    print(f"Avg Response Time: {total_time/num_transactions}")
                    print(f"Max Response Time: {max_response}")
                    print('----------------------------')
        except queue.Empty:
            pass
    print(f"Simulation ended in {time.time() - start} seconds.")
    # results_bank = []
    # while True:
    #    if result_queue.empty() == True:
    #        break
    #    results_bank.append(result_queue.get_nowait())
    
    # num_transactions = 0
    # total_time = 0
    # min_response = 10000000000000000
    # max_response = -1
    # for results in results_bank:
    #     num_transactions += len(results["Response Time"])
    #     for response_time in results["Response Time"]:
    #         total_time += response_time
    #         min_response = min(min_response, response_time)
    #         max_response = max(max_response, response_time)

    print(f"Total time: {total_time}")
    print(f"Num Txns: {num_transactions}")
    print(f"Avg Response Time: {total_time/num_transactions}")
    print(f"Max Response Time: {max_response}")

def main():
    parser = argparse.ArgumentParser(description="Transaction simulator")
    parser.add_argument('-c', '--conc', default='low', help="Concurrency level")
    parser.add_argument('-m', '--mpl', type=int, default=1, help="Number of active transactions")
    parser.add_argument('-n', '--nsize', type=int, default=4, help="Size of transaction")
    parser.add_argument('-i', '--isolation', type=int, default=0, help="Isolation level: 0 - Read Uncommitted, 1 - Read Committed, 2 - Repeatable Read, 3 - Serializable")
    args = parser.parse_args()
    assert args.conc == 'high' or args.conc == 'low'
    assert args.mpl > 0
    assert args.nsize > 0
    assert args.isolation in [0, 1, 2, 3]
    
    metadata_filename = f'./data/{args.conc}_concurrency/metadata.sql'
    query_filename = f'./queries/{args.conc}_concurrency/queries.txt'
    obs_filename = f'./data/{args.conc}_concurrency/observation_{args.conc}_concurrency.sql'
    semobs_filename = f'./data/{args.conc}_concurrency/semantic_observation_{args.conc}_concurrency.sql'

    # Insert Metadata
    insertMetadata(metadata_filename)

    # Preprocess workload
    workload, qry_workload = preprocessWorkload(query_filename, obs_filename, semobs_filename, args.nsize)
    
    # Spawn worker processes
    # Idea #1: have main process generate transactions, sleep for necessary amount of time before placing on queue
    # Idea #2: main process generates transactions, worker process sleeps if time does not match up
    # Idea #3: divide transactions amongst N workers, each worker sleeps (advantage: no input queue needed)
    # transaction_queue = mp.Queue()
    # result_queue = mp.Queue()
    # processes = []
    # for i in range(args.mpl):
    #     processes.append(mp.Process(target=worker, args=(transaction_queue, result_queue, isolation_levels[args.isolation])))
    idea3Parallel(workload, args.mpl, args.nsize, isolation_levels[args.isolation])

if __name__ == "__main__":
    main()