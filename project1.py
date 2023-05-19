import os
import argparse
import time
import random
import queue
import csv
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

transactions_list = None

def workerIdea1(transactions, result_q, iso_level, barrier, start_time, id):
    conn = getConnection()
    conn.set_session(isolation_level=iso_level, autocommit=False)
    barrier.wait()
    
    while time.time() < start_time:
        continue
    #print(f"Process {id} starting")
    total_response_time = 0.0
    min_response_time = 100000000000
    max_response_time = -1
    percent = 0.1
    #for idx, transaction in enumerate(transactions[id]):
    while True:
        try:
            transaction = transactions.get_nowait()
            if transaction == TERMINATE_PROCESS:
                break
            while time.time() - start_time < transaction[0]:
                continue
            res = None
            cur = conn.cursor(withhold=False)
            response_start = time.time()
            cur.execute(transaction[2])
            if cur.description != None:
                res = cur.fetchall()
            conn.commit()
            cur.close()
            response = time.time() - response_start
            # result_q.put_nowait(response)
            total_response_time += response
            min_response_time = min(min_response_time, response)
            max_response_time = max(max_response_time, response)
        except queue.Empty:
            pass

    result_q.put_nowait((0, len(transactions), total_response_time, max_response_time, min_response_time))
    conn.close()

def workerIdea3(pipe, result_q, iso_level, barrier, start_time, id):
    conn = getConnection()
    conn.set_session(isolation_level=iso_level, autocommit=False)
    transactions = pipe.recv()
    barrier.wait()
    
    while time.time() < start_time:
        continue
    #print(f"Process {id} starting")
    total_response_time = 0.0
    min_response_time = 100000000000
    max_response_time = -1
    percent = 0.1
    for idx, transaction in enumerate(transactions):
        while time.time() - start_time < transaction[0]:
            continue
        res = None
        cur = conn.cursor(withhold=False)
        response_start = time.time()
        cur.execute(transaction[2])
        if cur.description != None:
            res = cur.fetchall()
        conn.commit()
        cur.close()
        response = time.time() - response_start
        # result_q.put_nowait(response)
        total_response_time += response
        min_response_time = min(min_response_time, response)
        max_response_time = max(max_response_time, response)
        if idx / len(transactions) > percent:
            result_q.put_nowait((-1, id, percent, idx + 1, total_response_time, max_response_time, min_response_time))
            percent += 0.1

    result_q.put_nowait((0, len(transactions), total_response_time, max_response_time, min_response_time))
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
    
def preprocessWorkload(qfname, ofname, sfname, pfname):
    """
        Preprocess the workload. Output is already batched into transactions of size N
    """
    start = time.time()
    try:
        with open(pfname, 'r') as f:
            workload_data = [tuple([float(line[0]), line[1], line[2]]) for line in csv.reader(f)]
    except FileNotFoundError:
        print("No file found. Preprocessing data.")
        qry_data = parseQueryFile(qfname)
        obs_data = parseInsertFile(ofname)
        sem_data = parseInsertFile(sfname)

        workload_data = qry_data + obs_data + sem_data
        random.shuffle(workload_data)
        workload_data = sorted(workload_data, key=lambda x: x[0])
        with open(pfname, 'w') as out:
            csv_out=csv.writer(out)
            for row in workload_data:
                csv_out.writerow(row)
        print(f"Saved preprocessed data into \'{pfname}\'")

    print(f"Finished preprocessing in {time.time() - start:.2f}")
    return workload_data

def prepareTransactions(workload_data, transaction_size):
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

    return workload_transactions

def idea1Parallel(workload, mpl, nsize, iso_level):
    workload_transactions = prepareTransactions(workload, nsize)
    result_queue = mp.Queue()
    input_queue = mp.Queue()
    barrier = mp.Barrier(mpl + 1)
    processes = []
    # 15 second delay for synchronization
    start_time = time.time() + 15
    for i in range(mpl):
        processes.append(mp.Process(target=workerIdea1, args=(input_queue, result_queue, iso_level, barrier, start_time, i)))
    for proc in processes:
        proc.start()
    barrier.wait()

    print("Starting Simulation")

    # Collect data point from queue to avoid excessive RAM usage
    for transaction in workload_transactions:
        input_queue.put_nowait(transaction)
    for i in range(mpl):
        input_queue.put_nowait(TERMINATE_PROCESS)
    print(f"Inserted all {len(workload_transactions)} transactions in {time.time() - start_time:.02f} seconds.")

    for proc in processes:
        proc.join()
    elapsed_time = time.time() - start_time
    print(f"Simulation ended in {elapsed_time} seconds.\n")
    num_transactions = 0
    total_time = 0
    min_response = 10000000000000000
    max_response = -1
    while True:
        try:
            result = result_queue.get_nowait()
            num_transactions += result[1]
            total_time += result[2]
            max_response = max(max_response, result[3])
            min_response = min(min_response, result[4])
        except queue.Empty:
            break
    
    avg_response = total_time/num_transactions
    throughput = num_transactions/elapsed_time
    print("--------------------")
    print(f"Total Response Time: {total_time:.02f}s")
    print(f"Total Clock Time: {elapsed_time:.02f}s")
    print(f"Num Txns: {num_transactions}")
    print(f"Avg Response Time: {avg_response:.07f} s/txn")
    print(f"Avg Throughput: {throughput:.02f} txn/s")
    print(f"Max Response Time: {max_response:.07f}s")
    print(f"Min Response Time: {min_response:.07f}s")
    return total_time, elapsed_time, num_transactions, avg_response, throughput, max_response, min_response

def idea3Parallel(workload, mpl, nsize, iso_level):
    result_queue = mp.Queue()
    barrier = mp.Barrier(mpl + 1)
    processes = []
    # 15 second delay for synchronization
    start_time = time.time() + 30
    pipes = []
    for i in range(mpl):
        pipes.append(mp.Pipe())
    for i in range(mpl):
        processes.append(mp.Process(target=workerIdea3, args=(pipes[i][0], result_queue, iso_level, barrier, start_time, i)))

    for proc in processes:
        proc.start()
    workload_transactions = prepareTransactions(workload, nsize)
    worker_transactions = [[] for i in range(mpl)]
    num = len(workload_transactions)
    worker = 0
    for i in range(len(workload_transactions)):
        worker_transactions[worker].append(workload_transactions[i])
        worker += 1
        worker %= mpl
    
    # RAM blew up a couple times so devising better ways to monitor progress without wrecking pc
    # turns out piping input works slightly better
    for i in range(mpl):
        pipes[i][1].send(worker_transactions[i])
    barrier.wait()

    print("Starting Simulation")

    # Collect data point from queue to avoid excessive RAM usage
    num_transactions = 0
    total_time = 0
    min_response = 10000000000000000
    max_response = -1
    num_terminate = 0
    monitor = [[0 for _ in range(9)] for i in range(5)] # wtf
    while num_terminate != mpl:
        try:
            result = result_queue.get_nowait()
            if result[0] == 0:
                # (0, len(transactions), total_response_time, max_response_time, min_response_time)
                num_transactions += result[1]
                total_time += result[2]
                max_response = max(max_response, result[3])
                min_response = min(min_response, result[4])
                num_terminate += 1
            else:
                # Because I really want to see progress
                # (-1, id, percent, len(transactions), total_response_time, max_response_time, min_response_time)
                idx = int(result[2] * 10) - 1
                monitor[0][idx] += 1
                monitor[1][idx] += result[3] # transactions
                monitor[2][idx] += result[4] # total response time
                monitor[3][idx] = max(monitor[3][idx], result[5]) # max
                monitor[4][idx] = min(monitor[3][idx], result[6]) # min
                if monitor[0][idx] == mpl:
                    curr_elapsed = time.time() - start_time
                    print('----------------------------')
                    print(f"Total Response Time: {monitor[2][idx]:.02f}s")
                    print(f"Total Clock Time: {curr_elapsed:.02f}s")
                    print(f"Num Txns: {monitor[1][idx]}/{num}, {monitor[1][idx]/num*100:.02f}%")
                    print(f"Avg Response Time: {monitor[2][idx]/monitor[1][idx]:.07f} s/txn")
                    print(f"Avg Throughput: {monitor[1][idx]/curr_elapsed:.02f} txn/s")
                    print(f"Max Response Time: {monitor[3][idx]:.07f}s")
                    print(f"Min Response Time: {monitor[4][idx]:.07f}s")
                    print('----------------------------')  
        except queue.Empty:
            pass
    
    elapsed_time = time.time() - start_time
    for proc in processes:
        proc.join()
    print(f"Simulation ended in {elapsed_time} seconds.\n")
    
    avg_response = total_time/num_transactions
    throughput = num_transactions/elapsed_time
    print("--------------------")
    print(f"Total Response Time: {total_time:.02f}s")
    print(f"Total Clock Time: {elapsed_time:.02f}s")
    print(f"Num Txns: {num_transactions}")
    print(f"Avg Response Time: {avg_response:.07f} s/txn")
    print(f"Avg Throughput: {throughput:.02f} txn/s")
    print(f"Max Response Time: {max_response:.07f}s")
    print(f"Min Response Time: {min_response:.07f}s")
    return total_time, elapsed_time, num_transactions, avg_response, throughput, max_response, min_response


def cleanDatabase(createDBfname, dropDBfname):
    conn = getConnection()
    conn.set_session(autocommit=True)
    with conn.cursor() as cur:
        with open(dropDBfname, 'r') as f:
            drop_tables = f.read()
        cur.execute(drop_tables)
        with open(createDBfname, 'r') as f:
            create_tables = f.read()
        cur.execute(create_tables)
    conn.close()
    time.sleep(5)
    return


def main():
    parser = argparse.ArgumentParser(description="Transaction simulator")
    parser.add_argument('-c', '--conc', default='low', help="Concurrency level")
    # parser.add_argument('-m', '--mpl', type=int, default=1, help="Number of active transactions")
    # parser.add_argument('-n', '--nsize', type=int, default=4, help="Size of transaction")
    # parser.add_argument('-i', '--isolation', type=int, default=0, help="Isolation level: 0 - Read Uncommitted, 1 - Read Committed, 2 - Repeatable Read, 3 - Serializable")
    args = parser.parse_args()
    assert args.conc == 'high' or args.conc == 'low'
    # assert args.mpl > 0
    # assert args.nsize > 0
    # assert args.isolation in [0, 1, 2, 3]

    metadata_filename = f'./data/{args.conc}_concurrency/metadata.sql'
    query_filename = f'./queries/{args.conc}_concurrency/queries.txt'
    obs_filename = f'./data/{args.conc}_concurrency/observation_{args.conc}_concurrency.sql'
    semobs_filename = f'./data/{args.conc}_concurrency/semantic_observation_{args.conc}_concurrency.sql'
    preprocessed_filename = f'./preprocessed.csv'

    create_table_filename = f'./Docker/create.sql'
    drop_table_filename = f'./Docker/drop.sql'

    result_file = f'./results/result_{args.conc}.csv'

    fields = ['txn_size', 'mpl', "isolation_level", 'total_response_time', 'elapsed_time', 'num_transactions', 'avg_response', 'throughput', 'max_response', 'min_response']
    with open(result_file, 'w') as f:
        writer = csv.writer(f)
        writer.writerow(fields)

    # Preprocess workload
    workload = preprocessWorkload(query_filename, obs_filename, semobs_filename, preprocessed_filename)
    
    # Spawn worker processes
    # Idea #1: have main process generate transactions, sleep for necessary amount of time before placing on queue
    # Idea #2: main process generates transactions, worker process sleeps if time does not match up
    # Idea #3: divide transactions amongst N workers, each worker sleeps (pro: no input queue needed) (con: some processes may finish a lot earlier than others)
    #idea3Parallel(workload, args.mpl, args.nsize, isolation_levels[args.isolation])
    for mpl in [256, 128, 64, 32, 16, 8]:
        for txn_size in [1, 2, 4, 8, 16, 32]:
            for iso_level in isolation_levels:
                print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
                print(f"TEST {args.conc} concurrency: Parameters - txn size={txn_size}, mpl={mpl}, isolation level={iso_level}")
                cleanDatabase(create_table_filename, drop_table_filename)
                print("Database cleaned.")
                insertMetadata(metadata_filename)
                print("Metadata inserted.")
                total_response_time, elapsed_time, num_transactions, avg_response, throughput, max_response, min_response = idea3Parallel(workload, mpl, txn_size, iso_level)
                result = [txn_size, mpl, iso_level, total_response_time, elapsed_time, num_transactions, avg_response, throughput, max_response, min_response]
                with open(result_file, 'a') as f:
                    writer = csv.writer(f)
                    writer.writerow(result)
                print("\n\n")



if __name__ == "__main__":
    main()