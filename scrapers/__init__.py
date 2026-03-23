"""
State inmate database scrapers.

Each scraper discovers inmates from state DOC databases
and loads them into the Inmate table for outreach processing.
"""

from .base_scraper import BaseScraper, ScraperResult
from .oklahoma_parser import run as run_oklahoma
from .washington_scraper import run as run_washington
from .newyork_scraper import run as run_newyork
from .california_scraper import run as run_california

__all__ = [
    "BaseScraper",
    "ScraperResult",
    "run_oklahoma",
    "run_washington",
    "run_newyork",
    "run_california",
]

