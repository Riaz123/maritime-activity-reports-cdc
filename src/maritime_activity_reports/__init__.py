"""
Maritime Activity Reports with CDC/CDF Medallion Architecture

A comprehensive data engineering solution for processing maritime vessel activity data
using Change Data Capture (CDC) and Change Data Feed (CDF) in a medallion architecture.

Features:
- Bronze layer: Raw CDC data ingestion with audit trails
- Silver layer: Cleaned and validated data with CDF streaming
- Gold layer: Business-ready analytics with materialized views
- Real-time change propagation across all layers
- Maritime domain expertise embedded in data transformations
"""

__version__ = "1.0.0"
__author__ = "Data Engineering Team"
__email__ = "data-team@your-company.com"

from .models.config import MaritimeConfig
from .utils.logging import setup_logging

__all__ = [
    "MaritimeConfig",
    "setup_logging",
]
