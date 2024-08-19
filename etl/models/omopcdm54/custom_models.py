"""custom models for OMOPCDM"""

# pylint: disable=invalid-name
from typing import Final

from ...util.freeze import freeze_instance
from ..modelutils import (
    BigIntField,
    CharField,
    Column,
    DateTimeField,
    PKIdMixin,
)
from .registry import OmopCdmModelBase as ModelBase


@freeze_instance
class CDMSummary(ModelBase, PKIdMixin):
    """
    https://ohdsi.github.io/CommonDataModel/cdm54.html#CDM_SOURCE
    """

    __tablename__: Final[str] = "cdm_summary"

    site: Final[Column] = CharField(255)
    transform_name: Final[Column] = CharField(255, nullable=False)
    start_transform_datetime: Final[Column] = DateTimeField()
    end_transform_datetime: Final[Column] = DateTimeField()
    memory_used: Final[Column] = BigIntField()
    model_row_count: Final[Column] = BigIntField()
