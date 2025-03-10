# conftest.py
import logging

def pytest_configure(config):
    # Set the logging level for the asyncio.proactor_events package to WARNING
    logging.getLogger('asyncio.proactor_events').setLevel(logging.WARNING)