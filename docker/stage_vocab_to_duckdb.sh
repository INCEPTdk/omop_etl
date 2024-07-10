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

create table $VOCAB_SCHEMA.concept as select * from '/vocab/CONCEPT.csv'; \
create table $VOCAB_SCHEMA.concept_ancestor as select * from '/vocab/CONCEPT_ANCESTOR.csv'; \
create table $VOCAB_SCHEMA.concept_class as select * from '/vocab/CONCEPT_CLASS.csv'; \
create table $VOCAB_SCHEMA.concept_relationship as select * from '/vocab/CONCEPT_RELATIONSHIP.csv'; \
create table $VOCAB_SCHEMA.concept_synonym as select * from '/vocab/CONCEPT_SYNONYM.csv' ; \
create table $VOCAB_SCHEMA.domain as select * from '/vocab/DOMAIN.csv'; \
create table $VOCAB_SCHEMA.drug_strength as select * from '/vocab/DRUG_STRENGTH.csv'; \
create table $VOCAB_SCHEMA.relationship as select * from '/vocab/RELATIONSHIP.csv'; \
create table $VOCAB_SCHEMA.vocabulary as select * from '/vocab/VOCABULARY. csv'; "

duckdb $DB_DBNAME -c "$exec_command"
