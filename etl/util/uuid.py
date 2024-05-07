"""Module for UUID helpers"""

import uuid


def generate_uuid() -> uuid.UUID:
    """Generate a UUID (uuid4)"""
    return uuid.uuid4()


def generate_uuid_as_str() -> str:
    """Generate a UUID (uuid4) as string"""
    return str(generate_uuid())
