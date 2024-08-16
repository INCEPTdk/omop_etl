"""Collect all model submodules here"""

# flake8: noqa
from typing import Dict, Final, List

# Clinical Data Tables
from .clinical import *

# Custom Tables
from .custom_models import *

# Health Economics Data Tables
from .health_economics import *

# Health System Data Tables
from .health_systems import *

# Vocabulary Tables
from .metadata import *

# Module utils
from .registry import OmopCdmModelBase, OmopCdmModelRegistry

# Results Tables
from .results import *

# Standardized Derived Elements
from .standardized_derived_elements import *

# Vocabulary Tables
from .vocabulary import *

OMOPCDM_VERSION: Final[str] = "5.4"

# pylint: disable=no-member
OMOPCDM_REGISTRY: Final[Dict[str, OmopCdmModelBase]] = (
    OmopCdmModelRegistry().registered
)

# pylint: disable=no-member
OMOPCDM_MODELS: Final[List[OmopCdmModelBase]] = (
    OmopCdmModelRegistry().registered.values()
)

# pylint: disable=no-member
OMOPCDM_MODEL_NAMES: Final[List[str]] = [
    k for k, _ in OmopCdmModelRegistry().registered.items()
]

__all__ = OMOPCDM_MODEL_NAMES
