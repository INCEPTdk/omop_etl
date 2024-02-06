"""get memory usage"""
import os

import psutil


def get_memory_use():
    """
    Utility function to get the current memory usage of
    the etl process.
    """
    process = psutil.Process(os.getpid())
    return process.memory_info().rss  # in bytes
