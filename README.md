# Rigshospitalet ETL

Run with Docker (option 1) or on the host with Virtual Environments (option 2).

Both options assume you have a database (postgres) instance running either locally or otherwise. These details will be needed when running the ETL.

## Get the dev CSV files
In order to run the ETL, you will need some input (dummy) data or source database. This will depend on your project and scan report.

Put these in a directory on your machine somewhere which will need to specified in the input (either in the docker-compose.override.yml or as a command line argument).

## Option 1 - Running on the host with Singularity
The singularity container has to be build on the host machine (currently using bitbucket pipelines to build the singularity container directly pulling the github package from this repository). There are no entrypoints available with singularity, so everything has to run through bash commands.

### Step 1. Check files
Check that `rigs-etl.latest.sif` is in the correct location (following examples assume it is at `/users/singularity/rigs-etl.latest.sif`)
Check that a file with all the env variables is in the same location as where the singularity is executed. This file would look like:

```bash
DB_DBMS=duckdb
DB_PORT=none
DB_SERVER=/users/rigs-etl.duckdb
DB_DBNAME=/users/rigs-etl.duckdb
DB_USER=none
DB_PASSWORD=none
VERBOSITY_LEVEL=DEBUG
SOURCE_SCHEMA=source
TARGET_SCHEMA=omopcdm
INCLUDE_UNMAPPED_CODES=FALSE
NUM_THREADS=30
MAX_MEMORY_LIMIT=120gb
```

The path to the database (which will be binded later to /users ) must have this structure:
```
.rigs-etl.duckdb
.output
├── site1
│   ├── course_metadata
│   │   └─ course_metadata.parquet
│   ├── diagnoses_procedures
│   │   └── diagnoses_procedures.parquet
│   ├── drugs
│   │   ├── administrations.parquet
│   │   └── prescriptions.parquet
│   ├── observations
│   │   ├── observations-1.parquet
│   │   ├           .
│   │   └── observations-N.parquet
│   ├── descriptions.parquet
│   └── course_id_cpr_mapping.txt
├── site2
│   ├── course_metadata
│   │   └─ course_metadata.parquet
│   ├── diagnoses_procedures
│   │   └── diagnoses_procedures.parquet
│   ├── drugs
│   │   ├── administrations.parquet
│   │   └── prescriptions.parquet
│   ├── observations
│   │   ├── observations-1.parquet
│   │   ├           .
│   │   └── observations-N.parquet
│   ├── descriptions.parquet
│   └── course_id_cpr_mapping.txt
├── diag.parquet
├── opr.parquet
├── ube.parquet
├── laboratory.parquet
```

### Load the source data into the database
In order to load the souce data into the database you can run the following command (you need to bind the right folder to /users):
```bash
singularity exec --bind /path/to/database:/users --env-file rigs-etl-duckdb.env --pwd /etl --writable /users/singularity/rigs-etl.latest.sif /etl/docker/stage_source_to_duckdb.sh
```

### Load vocab into the database
In order to load the vocab into the database you can run the following command (you need to bind the right folder to /users and /vocab):

```bash
singularity exec --bind /path/to/database:/users --bind /path/to/vocab:/vocab --env-file rigs-etl-duckdb.env --pwd /etl --writable /users/singularity/rigs-etl.latest.sif /etl/docker/stage_vocab_to_duckdb.sh
```

### Run the main ETL 
In order to run the main ETL you can run the following command (you need to bind the right folder to /users):

```bash
singularity exec --bind /path/to/database:/users --env-file rigs-etl-duckdb.env --pwd /etl --writable /users/singularity/rigs-etl.latest.sif /etl/docker/entrypoint.sh
```
If you want to run the tests or add new env vars you can either add them to the env files or passing the --env parameter in the command above.

### Run the merge ETL
In order to run the merge ETL you can run the following command (you need to bind the right folder to /users):

```bash
singularity exec --bind /path/to/database:/users --env-file rigs-etl-duckdb.env --pwd /etl --writable /users/singularity/rigs-etl.latest.sif /etl/docker/entrypoint.merge.sh
```

## Option 2 - Running with Docker
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
      SOURCE_SCHEMA: "source" # Add the actual source schema, ideally site-specific
      TARGET_SCHEMA: "omopcdm" # Add the actual target schema, ideally site-specific
      REGISTRY_SCHEMA: "registries" # Add the actual schema with national registries
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
### Step 8. Run the merge ETL in a Docker container
This will merge all the previous ETLs' target into the new TARGET_SCHEMA. At the moment schemas used as target from the single ETLs are fetched from the database by looking at the which schemas contain the CDM tables. This in the future could be changed to be passed as an environment variable or config file.
The merge docker service uses a different entrypoint - a better approach may be possible but with the current set-up should be straight-forward simply changing the entrypoiny when running with singularity containers.

```bash
docker-compose run --rm -e TARGET_SCHEMA=merge_etl merge
```

## Option 3 - Running in a Virtual Environment

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
  "user": "postgres",
  "password": "admin"
}
```

Note the host and port will be different to the Docker setup (option 1).

### Step 3. Run the ETL in a Virtual Environment
Make sure you are in this directory (same directory as the README.md). Make sure to export the required environment variables:

```bash
export PYTHONPATH=$PWD \
  SOURCE_SCHEMA=name_of_schema \
  TARGET_SCHEMA=name_of_schema \
  REGISTRY_SCHEMA=name_of_name \
  DEPARTMENT_SHAK_CODE=the_code
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
