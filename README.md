# Rigshospitalet ETL

Run with Docker (option 1) or on the host with Virtual Environments (option 2).

Both options assume you have a database (postgres) instance running either locally or otherwise. These details will be needed when running the ETL.

## Get the dev CSV files
In order to run the ETL, you will need some input (dummy) data or source database. This will depend on your project and scan report.

Put these in a directory on your machine somewhere which will need to specified in the input (either in the docker-compose.override.yml or as a command line argument).

## Option 1 - Running with Docker
It assumes that your database is running on the `rigs-net`.
```bash
docker network create rigs-net
```

### Step 1. Check Docker
Make sure Docker is up and running. You can check this by running:
```bash
docker ps
```

### Step 2. Create a docker-compose.override.yml file
```bash
touch docker-compose.override.yml
```

Add the following to your override file:
```bash
version: '3.5'
services:
  etl:
    environment:
      VERBOSITY_LEVEL: "DEBUG"
      SOURCE_SCHEMA: "source" # Add the actual source schema
      DEPARTMENT_SHAK_CODE: "" # Fill with the department specific code
      RELOAD_VOCAB: "FALSE" # Set to TRUE to reload all of the OMOPCDM vocab tables
    volumes:
      - "/path/to/input/data/dir:/data:ro"
```

Make sure you put the correct path to your local data directory (on the host) in the volumes section. In this example `/path/to/input/data/dir` must be changed.
This directory should contain your input csv files.

### Step 3. Create a db.env file
Create a `db.env` file to reflect the correct database connection details.

For example it should look something like:
```bash
DB_DBMS=postgresql
DB_PORT=5432
DB_SERVER=postgres
DB_DBNAME=postgres
DB_SCHEMA=omopcdm
DB_USER=postgres
DB_PASSWORD=admin
```

Note that the server here is postgres (which is different when running on the host). Similarly the port should be mapped to 5432 inside docker, but it will be different on the host.

### Step 4. Set git tags
Fetch the tags via:
```bash
git fetch --all --tags
```

### Step 5. Build the docker image
```bash
GITHUB_TAG=$(git describe --tags) COMMIT_SHA=$(git rev-parse --short HEAD) docker-compose build
```

### Step 6. Run the tests
```bash
docker-compose run --rm -e RUN_TESTS="True" etl
```

### Step 7. Run the ETL in a Docker container
```bash
docker-compose run --rm etl
```

## Option 2 - Running in a Virtual Environment

### Step 1. Setup a virtual environment
To setup a virtual environment for development you can do:
```bash
python3 -m venv venv
source venv/bin/activate
python3 -m pip install -r requirements.txt
python3 -m pip install -r requirements.dev.txt
```

### Step 2. Create a connection.json file
Create a `connection.json` file to reflect the correct database connection details.

For example it should look something like:
```json
{
  "dbms": "postgresql",
  "server": "localhost",
  "port": 5565,
  "dbname": "postgres",
  "schema": "omopcdm",
  "user": "postgres",
  "password": "admin"
}
```

Note the host and port will be different to the Docker setup (option 1).

### Step 3. Run the ETL in a Virtual Environment
Make sure you are in this directory (same directory as the README.md).

```bash
export PYTHONPATH=$PWD
python3 etl/tools/main.py
```

## Tests

A small test suite exists. This contains both unit tests and integration tests. The latter will require an instance of postgres setup that can be connected to.

It is recommended to use `pytest` when running tests, but it not necessary.

To run the unit tests do:
```bash
pytest tests/testsuite.py
```

To run the full test suite with the postgres tests you must set the following variables:
```bash
export ETL_RUN_INTEGRATION_TESTS=ON
export ETL_TEST_TARGET_HOST=<host> 		      # this maybe "localhost" or an aws instance
export ETL_TEST_TARGET_DBNAME=<dbname> 	    # this maybe "postgres" (default) but can be anything
export ETL_TEST_TARGET_USER=<username>	    # the default is "postgres", set this variable to override
export ETL_TEST_TARGET_PASSWORD=<password>  # your db access password
export ETL_TEST_TARGET_PORT=<port>		      # if not default of 5432
```

You can then run the same command but now it will run the integration tests also. Note these will take a bit longer to run!
```bash
pytest tests/testsuite.py
```
