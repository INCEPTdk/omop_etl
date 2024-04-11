""" SQL query string definition for the stem functions"""

from typing import Final

from ..models.omopcdm54.registry import TARGET_SCHEMA
from ..models.source import SOURCE_SCHEMA
from ..models.tempmodels import LOOKUPS_SCHEMA

SQL_FUNCTIONS: Final[
    str
] = f"""
/*
The date_cols function selects all the possible date fields in a specific table,
as well as the unique id of the record in the source that can be used to link
the date to the correct event, if one person has multiple entries in a table
and therefore multiple date values for the same date field. Depending on the
date field that is defined in the semantic mapping document the correct date
will be selected in the stem transform.
*/
DROP FUNCTION IF EXISTS {TARGET_SCHEMA}.date_cols(text, text);
CREATE OR REPLACE FUNCTION {TARGET_SCHEMA}.date_cols(sname text, tname text)
RETURNS void LANGUAGE plpgsql
AS $$
DECLARE
    select_list text;
BEGIN
    drop table if exists temp_date_cols;
    create temp table temp_date_cols (date_name text, date_val text, courseid bigint, _id bigint);
    SELECT string_agg(column_name, ',')
    INTO select_list
    FROM information_schema.columns
    WHERE table_schema = sname
    AND table_name = tname
    AND data_type IN ('date', 'timestamp without time zone',
    'timestamp with time zone');
    IF (select_list IS NOT NULL AND select_list != '') THEN
        EXECUTE format($fmt$
            insert into temp_date_cols
            select (json_each_text(row_to_json(t))).*, courseid, _id
            from (
                select %s, courseid, _id
                from %I.%I
                ) t
            $fmt$, select_list, sname, tname);
    ELSE
        EXECUTE format($fmt$ SELECT 'no_date_found', NULL, NULL::BIGINT,NULL::BIGINT into temp_date_cols $fmt$);
    END IF;
END $$;

/*create a function to create a general pivot that can be passed to a categorical or numerical pivot function*/
DROP FUNCTION IF EXISTS {TARGET_SCHEMA}.pivot_stem(text);
CREATE OR REPLACE FUNCTION {TARGET_SCHEMA}.pivot_stem(source_table text)
RETURNS TABLE (variable varchar(140), value text, col_value varchar(140), person_source_value varchar(50), person_id integer, visit_occurrence_id integer, start_date text, end_date text) LANGUAGE plpgsql
as $$
BEGIN
    RETURN QUERY
    EXECUTE (format(
            'with my_source AS (
    SELECT u.variable,
           u.value,
           pt.person_id,
           pt.person_source_value,
           u.courseid,
           u._id
    FROM {SOURCE_SCHEMA}.%s u
        INNER JOIN {SOURCE_SCHEMA}.courseid_cpr_mapping c
        ON c.courseid = u.courseid
        INNER JOIN {TARGET_SCHEMA}.person pt
        ON (''cpr_enc|''||c.cpr_enc)::VARCHAR = pt.person_source_value
    WHERE pt.person_source_value IS NOT NULL AND value IS NOT NULL
    ),
     my_pivot_pre_join AS (
         SELECT DISTINCT
                ms.variable,
                ms.value,
                case when ma.value_type = ''categorical'' then concat(ms.variable, ''__'', ms.value::TEXT) else ms.variable end as col_value,
                ms.person_source_value,
                ms.courseid,
                ma.start_date,
                ma.end_date,
                ms.person_id,
                ms._id
         FROM my_source ms
                  INNER JOIN {LOOKUPS_SCHEMA}.concept_lookup_stem ma ON LOWER(variable) =
                  LOWER(ma.source_variable)
        WHERE LOWER(ma.datasource) = LOWER(''%s'')
     ), my_pivot AS (
         SELECT mpp.variable,
                mpp.value::TEXT,
                mpp.col_value,
                mpp.person_source_value,
                mpp.person_id,
                v.visit_occurrence_id,
                dt1.date_val as start_date,
                dt2.date_val as end_date
         FROM my_pivot_pre_join mpp
                  LEFT JOIN {TARGET_SCHEMA}.visit_occurrence v
                        ON ''courseid|''||mpp.courseid = v.visit_source_value
                    LEFT JOIN temp_date_cols dt1
                            ON mpp.start_date = dt1.date_name AND mpp.courseid = dt1.courseid
                                AND mpp._id = dt1._id
                  LEFT JOIN temp_date_cols dt2
                            ON mpp.end_date = dt2.date_name AND mpp.courseid = dt2.courseid
                                AND mpp._id = dt2._id
     ) select * from my_pivot;', source_table, source_table, source_table, source_table));
END $$;


/*
The pivot_categorical function performs the stem transformation in case of a
categorical value, which is defined in the semantic mapping document as
'categorical' in the value_type column.
*/
DROP FUNCTION IF EXISTS {TARGET_SCHEMA}.pivot_categorical(text);
CREATE OR REPLACE FUNCTION {TARGET_SCHEMA}.pivot_categorical(source_table text) returns void
    language plpgsql
as
$func$
BEGIN
    EXECUTE (format(
            'with my_merge AS (
         SELECT ma.source_concept_code,
                ma.value_type,
                ma.uid,
                ma.datasource,
                ma.mapped_standard_code,
                ma.std_code_domain,
                ma.value_as_concept_id,
                ma.value_as_string,
                ma.operator_concept_id,
                ma.unit_source_value,
                ma.unit_concept_id,
                ma.modifier_concept_id,
                ma.route_concept_id,
                ma.quantity,
                pi.person_source_value,
                pi.col_value,
                pi.start_date,
                pi.end_date,
                pi.person_id,
                pi.visit_occurrence_id,
                ma.type_concept_id,
                ma.days_supply,
                ma.dose_unit_source_value,
                ma.range_low,
                ma.range_high,
                ma.stop_reason,
                ma.route_source_value
         FROM temp_pivot pi
                  INNER JOIN {LOOKUPS_SCHEMA}.concept_lookup_stem ma
                             ON LOWER(ma.source_concept_code) = LOWER(pi.col_value)
         WHERE (LOWER(ma.value_type) = ''categorical'')
         AND LOWER(ma.datasource) = LOWER(''%s'')
     )
INSERT
INTO {TARGET_SCHEMA}.stem (domain_id,
                   person_id,
                   concept_id,
                   start_date,
                   start_datetime,
                   end_date,
                   end_datetime,
                   type_concept_id,
                   visit_occurrence_id,
                   source_value,
                   source_concept_id,
                   value_as_string,
                   value_as_concept_id,
                   unit_concept_id,
                   unit_source_value,
                   modifier_concept_id,
                   operator_concept_id,
                   range_low,
                   range_high,
                   stop_reason,
                   route_concept_id,
                   route_source_value,
                   datasource
                 )
SELECT DISTINCT
       std_code_domain,
       person_id,
       mapped_standard_code,
       start_date::DATE,
       start_date::TIMESTAMP,
       end_date::DATE,
       end_date::TIMESTAMP,
       type_concept_id::INTEGER,
       visit_occurrence_id,
       col_value,
       uid,
       value_as_string,
       value_as_concept_id::INTEGER,
       unit_concept_id::INTEGER,
       unit_source_value,
       modifier_concept_id::INTEGER,
       operator_concept_id::INTEGER,
       range_low,
       range_high,
       stop_reason,
       route_concept_id::INTEGER,
       route_source_value,
       datasource
FROM my_merge m;',
            source_table));
END
$func$;


/*
The pivot_numerical function performs the stem transformation in case of a
numerical value, which is defined in the semantic mapping document as
'numerical' in the value_type column.
*/
DROP FUNCTION IF EXISTS {TARGET_SCHEMA}.pivot_numerical(text);
CREATE OR REPLACE FUNCTION {TARGET_SCHEMA}.pivot_numerical(source_table text) returns void
    language plpgsql
as
$func$
BEGIN
    EXECUTE (format(
            'with my_merge AS (
         SELECT ma.source_concept_code,
                ma.value_type,
                ma.uid,
                ma.datasource,
                ma.mapped_standard_code,
                ma.std_code_domain,
                ma.value_as_concept_id,
                ma.value_as_number,
                ma.operator_concept_id,
                ma.unit_source_value,
                ma.unit_concept_id,
                ma.modifier_concept_id,
                ma.route_concept_id,
                ma.quantity,
                pi.person_source_value,
                pi.col_value,
                pi.value::double precision,
                pi.start_date,
                pi.end_date,
                pi.person_id,
                pi.visit_occurrence_id,
                ma.type_concept_id,
                ma.days_supply,
                ma.dose_unit_source_value,
                ma.range_low,
                ma.range_high,
                ma.stop_reason,
                ma.route_source_value
         FROM temp_pivot pi
                  INNER JOIN {LOOKUPS_SCHEMA}.concept_lookup_stem ma
                             ON LOWER(ma.source_variable) = LOWER(pi.col_value)
         WHERE (LOWER(ma.value_type) = ''numerical'')
 		 AND ma.mapped_standard_code is not NULL
         AND LOWER(ma.datasource) = LOWER(''%s'')
     )
INSERT
INTO {TARGET_SCHEMA}.stem (domain_id,
                   person_id,
                   concept_id,
                   start_date,
                   start_datetime,
                   end_date,
                   end_datetime,
                   type_concept_id,
                   visit_occurrence_id,
                   source_value,
                   source_concept_id,
                   value_as_number,
                   value_as_concept_id,
                   unit_concept_id,
                   unit_source_value,
                   modifier_concept_id,
                   operator_concept_id,
                   range_low,
                   range_high,
                   stop_reason,
                   route_concept_id,
                   route_source_value,
                   datasource
                 )
SELECT DISTINCT
       std_code_domain,
       person_id,
       mapped_standard_code,
       start_date::DATE,
       start_date::TIMESTAMP,
       end_date::DATE,
       end_date::TIMESTAMP,
       type_concept_id::INTEGER,
       visit_occurrence_id,
       concat(col_value, ''__'', value::TEXT) as source_value,
       uid,
       value,
       value_as_concept_id::INTEGER,
       unit_concept_id::INTEGER,
       unit_source_value,
       modifier_concept_id::INTEGER,
       operator_concept_id::INTEGER,
       range_low,
       range_high,
       stop_reason,
       route_concept_id::INTEGER,
       route_source_value,
       datasource
FROM my_merge m;',
            source_table));
END
$func$;

DROP FUNCTION IF EXISTS {TARGET_SCHEMA}.stem_loop(text, text);
CREATE OR REPLACE FUNCTION {TARGET_SCHEMA}.stem_loop(source_table text) returns void
    language plpgsql
as
$func$
DECLARE
    name TEXT;
BEGIN
    EXECUTE format(
             'SELECT {TARGET_SCHEMA}.date_cols(''{SOURCE_SCHEMA}'', %L);', source_table);
    drop table if exists temp_pivot;
    create temp table temp_pivot as select * from {TARGET_SCHEMA}.pivot_stem(source_table);
    for name in select column_name
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE table_name = source_table
                    AND column_name = 'variable'
        LOOP
            EXECUTE (format(
                     'SELECT {TARGET_SCHEMA}.pivot_categorical(''%s''); SELECT {TARGET_SCHEMA}.pivot_numerical(''%s'');',
                     source_table, source_table));
        END LOOP;
END ;
$func$;
""".strip().replace(
    "\n", " "
)
