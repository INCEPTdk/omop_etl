"""A module for sql helpers"""


def clean_sql(func):
    """Decorator for cleaning sql statements generated in functions"""

    def decorated(*args, **kwargs) -> str:
        result = func(*args, **kwargs)
        cleaned_sql = " ".join(
            [s for s in result.replace("\n", " ").split(" ") if s != ""]
        )
        return cleaned_sql.strip()

    return decorated
