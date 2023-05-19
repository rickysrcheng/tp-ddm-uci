# Transaction Processing

## Dependencies

Python 3.9, additional modules required are listed in requirements.txt. Use pip to install.

Docker

## Setting up scripts

First, run

```{.bash}
chmod +x ./load_dotenv.sh
chmod +x ./docker_refresh.sh
./load_dotenv.sh
```

Then, set up PostgresDB using

```{.bash}
./docker_refresh.sh
```

The script will delete the old database and create a new PostgresDB instance with the empty tables.

### Debugging

If you already have a PostgreSQL instance running on port 5432, then you can either 

1. Modify `PG_HOST_PORT` to another open port in `.env`. 
2. Stop the PostgreSQL service `systemctl stop postgresql` (Linux)
3. Find the PID associated with the port and kill it.
    ```
    sudo ss -lptn 'sport = :5432'
    sudo kill <pid>
    ```

## Running Tests

Tests can be run using the following:

```{.python}
python project1.py -c <high, low>
```

The `-c` accepts either `high` or `low` for testing each workload. Default is `low`.

### Debugging

If you run into `OSError: [Errno 24] Too many open files`, run the following in the shell

```
ulimit -n 2048
```

This raises the number of open file descriptors allowed to 2048 during this shell session. 
