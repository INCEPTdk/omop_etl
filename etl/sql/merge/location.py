from typing import List

from etl.util.sql import clean_sql

from ...models.omopcdm54.health_systems import Location


@clean_sql
def get_location_merge_insert(schemas: List[str]):

    select_statements = []
    for schema in schemas:
        statement = f""" SELECT {Location.zip.key},
        {Location.location_source_value.key},
        {Location.country_concept_id.key}
        from {schema}.{Location.__tablename__}
    """
        select_statements.append(statement)

    return f"""INSERT INTO {str(Location.__table__)}
    ({', '.join([Location.zip.key, Location.location_source_value.key, Location.country_concept_id.key])})
    {' UNION ALL '.join(select_statements)};"""
