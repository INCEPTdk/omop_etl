#!/bin/bash

exec_command="set memory_limit='150GB'; \
drop schema if exists $SOURCE_SCHEMA cascade; \
create schema $SOURCE_SCHEMA; \
create table $SOURCE_SCHEMA.course_metadata as \
  select ROW_NUMBER() OVER() AS id, tb1.* \
  from read_parquet('/data/output/$SOURCE_SCHEMA/course_metadata/course_metadata.parquet') as tb1; \
create table $SOURCE_SCHEMA.courseid_cpr_mapping as \
  select ROW_NUMBER() OVER() AS _id, * \
  from read_csv_auto('/data/output/$SOURCE_SCHEMA/course_id_cpr_mapping.txt'); \
create table $SOURCE_SCHEMA.diagnoses_procedures as \
  select ROW_NUMBER() OVER() AS _id, tb1.* \
  replace(concat(tb1.variable, '-', tb2.description) as variable) \
  from read_parquet('/data/output/$SOURCE_SCHEMA/diagnoses_procedures/diagnoses_procedures.parquet') as tb1 \
  inner join (select * from read_parquet('/data/output/$SOURCE_SCHEMA/descriptions.parquet')) as tb2 \
  on tb1.from_file=tb2.filename and tb1.variable=tb2.field; \
create table $SOURCE_SCHEMA.administrations as \
  select ROW_NUMBER() OVER() AS id, tb1.*, tb2.description \
  from read_parquet('/data/output/$SOURCE_SCHEMA/drugs/administrations.parquet') as tb1 \
  inner join (select * from read_parquet('/data/output/$SOURCE_SCHEMA/descriptions.parquet')) as tb2 \
  on tb1.from_file=tb2.filename and tb1.drug_name=tb2.field; \
create table $SOURCE_SCHEMA.prescriptions as \
  select ROW_NUMBER() OVER() AS id, * \
  from read_parquet('/data/output/$SOURCE_SCHEMA/drugs/prescriptions.parquet') as tb1; \
create table $SOURCE_SCHEMA. observations as \
  select ROW_NUMBER() OVER() AS id, tb1.* \
  replace(concat(tb1.variable, '-', tb2.description) as variable) \
  from read_parquet('/data/output/$SOURCE_SCHEMA/observations/observations-*.parquet') as tb1 \
  inner join (select * from read_parquet('/data/output/$SOURCE_SCHEMA/descriptions.parquet')) as tb2 \
  on tb1.from_file=tb2.filename and tb1.variable=tb2.field; \
ALTER TABLE $SOURCE_SCHEMA.administrations alter value type double; \
ALTER TABLE $SOURCE_SCHEMA.administrations alter value0 type double; \
ALTER TABLE $SOURCE_SCHEMA.administrations alter value1 type double; "

duckdb $DB_DBNAME -c "$exec_command"
