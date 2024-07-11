#!/bin/bash

exec_command="drop schema if exists $REGISTRY_SCHEMA cascade; \
create schema $REGISTRY_SCHEMA; \

create table $REGISTRY_SCHEMA.diagnoses as select * from read_parquet('/users/output/diag.parquet'); \
create table $REGISTRY_SCHEMA.procedures as select * from read_parquet('/users/output/ube.parquet'); \
create table $REGISTRY_SCHEMA.operations as select * from read_parquet('/users/output/opr.parquet'); \
create table $REGISTRY_SCHEMA.laboratory as select * from read_parquet('/users/output/laboratory.parquet'); "

duckdb $DB_DBNAME -c "$exec_command"
