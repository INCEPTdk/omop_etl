""" SQL query string definition for the stem functions"""
from typing import Final

from ..models.omopcdm54.registry import SCHEMA_NAME
from ..models.source import SOURCE_SCHEMA

SQL_FUNCTIONS: Final[
    str
] = f"""
/*
The pivot_categorical function performs the stem transformation in case of a
categorical value, which is defined in the semantic mapping document as
'categorical' in the value_type column.
*/
DROP FUNCTION IF EXISTS {SCHEMA_NAME}.pivot_categorical(text);
CREATE OR REPLACE FUNCTION {SCHEMA_NAME}.pivot_categorical(source_table text) returns void
    language plpgsql
as
$func$
BEGIN
    EXECUTE (format(
            'with my_source AS (
    SELECT CONCAT(variable, ''__'', value::TEXT) as col_value,
           pt.person_id,
           pt.person_source_value
           u.courseid
    FROM {SOURCE_SCHEMA}.%s u
        INNER JOIN {SCHEMA_NAME}.course_id_cpr_mapping c
        ON c.courseid = u.courseid
        INNER JOIN {SCHEMA_NAME}.person pt
        ON (c.courseid||c.cpr_enc)::VARCHAR = pt.person_source_value
    WHERE pt.person_source_value IS NOT NULL AND value IS NOT NULL
),
     my_pivot_pre_join AS (
         SELECT DISTINCT
                ms.col_value,
                ms.person_source_value,
                ms.id,
                ma.start_date,
                ma.end_date,
                ms.person_id
         FROM my_source ms
                  INNER JOIN {SCHEMA_NAME}.concept_lookup_stem ma ON LOWER(ms.col_value) =
                  LOWER(ma.source_concept_code)
        WHERE LOWER(ma.datasource) = LOWER(''%s'')
     ),
     my_pivot AS (
         SELECT mpp.col_value,
                mpp.person_source_value,
                mpp.person_id,
                mpp.id as source_id,
                v.visit_occurrence_id
         FROM my_pivot_pre_join mpp
                  JOIN {SCHEMA_NAME}.visit_occurrence v
                        ON ''courseid''||mpp.courseid = v.visit_source_value
     ),
     my_merge AS (
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
                pi.visit_occurrence_id
         FROM my_pivot pi
                  INNER JOIN {SCHEMA_NAME}.concept_lookup_stem ma
                             ON lower(ma.source_concept_code) = lower(pi.col_value)
         WHERE (LOWER(ma.value_type) = ''categorical'')
         AND LOWER(ma.datasource) = LOWER(''%s'')
     )
INSERT
INTO {SCHEMA_NAME}.stem (domain_id,
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
                   days_supply,
                   dose_unit_source_value,
                   modifier_concept_id,
                   operator_concept_id,
                   quantity,
                   range_low,
                   range_high,
                   stop_reason,
                   route_concept_id,
                   route_source_value
                 )
SELECT DISTINCT
       std_code_domain,
       person_id,
       mapped_standard_code,
       start_date::DATE,
       start_date,
       end_date::DATE,
       end_date,
       type_concept_id,
       visit_occurrence_id,
       variable|value,
       uid,
       value_as_string,
       value_as_concept_id,
       unit_concept_id,
       unit_source_value,
       days_supply,
       dose_unit_source_value,
       modifier_concept_id,
       operator_concept_id,
       quantity,
       range_low,
       range_high,
       stop_reason,
       route_concept_id,
       route_source_value
FROM my_merge m;',
            source_table, source_table, source_table, source_table));
END
$func$;
"""
