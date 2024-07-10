#!/bin/bash

exec_command="create schema if not exists results; \
create schema if not exists $VOCAB_SCHEMA; \
drop table if exists $VOCAB_SCHEMA.concept; \
drop table if exists $VOCAB_SCHEMA.concept_ancestor; \
drop table if exists $VOCAB_SCHEMA.concept_class; \
drop table if exists $VOCAB_SCHEMA.concept_relationship; \
drop table if exists $VOCAB_SCHEMA.concept_synonym; \
drop table if exists $VOCAB_SCHEMA.domain; \
drop table if exists $VOCAB_SCHEMA.drug_strength; \
drop table if exists $VOCAB_SCHEMA.relationship; \
drop table if exists $VOCAB_SCHEMA.vocabulary; \

create table $VOCAB_SCHEMA.concept as select * from read_csv_auto('/vocab/CONCEPT.csv', quote=''); \
create table $VOCAB_SCHEMA.concept_ancestor as select * from read_csv_auto('/vocab/CONCEPT_ANCESTOR.csv', quote=''); \
create table $VOCAB_SCHEMA.concept_class as select * from read_csv_auto('/vocab/CONCEPT_CLASS.csv', quote=''); \
create table $VOCAB_SCHEMA.concept_relationship as select * from read_csv_auto('/vocab/CONCEPT_RELATIONSHIP.csv', quote=''); \
create table $VOCAB_SCHEMA.concept_synonym as select * from read_csv_auto('/vocab/CONCEPT_SYNONYM.csv', quote=''); \
create table $VOCAB_SCHEMA.domain as select * from read_csv_auto('/vocab/DOMAIN.csv', quote=''); \
create table $VOCAB_SCHEMA.drug_strength as select * from read_csv_auto('/vocab/DRUG_STRENGTH.csv', quote=''); \
create table $VOCAB_SCHEMA.relationship as select * from read_csv_auto('/vocab/RELATIONSHIP.csv', quote=''); \
create table $VOCAB_SCHEMA.vocabulary as select * from read_csv_auto('/vocab/VOCABULARY.csv', quote=''); "

duckdb $DB_DBNAME -c "$exec_command"
