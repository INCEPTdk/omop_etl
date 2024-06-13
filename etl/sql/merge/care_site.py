"""SQL code to add the correct location_id to the care_site table after merge"""

from sqlalchemy import select, update

from etl.models.omopcdm54.health_systems import CareSite, Location


def add_location_to_care_site():
    """Add a location ID to a CDM table."""
    subquery = (
        select(Location.location_id)
        .where(
            CareSite.care_site_source_value == Location.location_source_value
        )
        .scalar_subquery()
    )

    update_stmt = update(CareSite).values(location_id=subquery)

    return update_stmt
