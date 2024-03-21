""" SQL query string definition for the stem functions"""

from typing import Final

from ..models.omopcdm54.registry import TARGET_SCHEMA

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
RETURNS TABLE(date_name text,date_val text, courseid bigint) LANGUAGE plpgsql
AS $$
DECLARE
    select_list text;
BEGIN
    SELECT string_agg(column_name, ',')
    INTO select_list
    FROM information_schema.columns
    WHERE table_schema = sname
    AND table_name = tname
    AND data_type IN ('date', 'timestamp without time zone',
    'timestamp with time zone');
    IF (select_list IS NOT NULL AND select_list != '') THEN
        RETURN QUERY
        EXECUTE format($fmt$
            select (json_each_text(row_to_json(t))).*, courseid
            from (
                select %s, courseid
                from %I.%I
                ) t
            $fmt$, select_list, sname, tname);
    ELSE
        RETURN QUERY
            SELECT 'no_date_found', NULL, NULL::BIGINT;
    END IF;
END $$;
"""
