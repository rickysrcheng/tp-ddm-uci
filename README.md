# Transaction Processing

## Dependencies

Python 3.9, psycopg2
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