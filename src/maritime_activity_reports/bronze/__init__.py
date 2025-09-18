"""Bronze layer - CDC data ingestion and raw data storage."""

from .cdc_ingestion import BronzeCDCLayer
from .table_setup import BronzeTableSetup

__all__ = [
    "BronzeCDCLayer",
    "BronzeTableSetup",
]
