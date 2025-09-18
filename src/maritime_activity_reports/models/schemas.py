"""Data schemas and models for maritime activity reports."""

from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel, Field
from enum import Enum


class CDCOperation(str, Enum):
    """CDC operation types."""
    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"
    UPDATE_PREIMAGE = "update_preimage"
    UPDATE_POSTIMAGE = "update_postimage"


class NavigationalStatus(str, Enum):
    """AIS navigational status codes."""
    UNDER_WAY = "UNDER_WAY"
    ANCHORED = "ANCHORED"
    NOT_UNDER_COMMAND = "NOT_UNDER_COMMAND"
    RESTRICTED_MANOEUVRABILITY = "RESTRICTED_MANOEUVRABILITY"
    CONSTRAINED_BY_DRAUGHT = "CONSTRAINED_BY_DRAUGHT"
    MOORED = "MOORED"
    AGROUND = "AGROUND"
    FISHING = "FISHING"
    SAILING = "SAILING"
    UNDEFINED = "UNDEFINED"


class VesselMovement(BaseModel):
    """Schema for vessel movement data."""
    
    imo: str = Field(..., description="International Maritime Organization number")
    movementdatetime: datetime = Field(..., description="Timestamp of the movement")
    latitude: float = Field(..., ge=-90, le=90, description="Latitude coordinate")
    longitude: float = Field(..., ge=-180, le=180, description="Longitude coordinate")
    speed_over_ground: Optional[float] = Field(None, ge=0, description="Speed in knots")
    course_over_ground: Optional[float] = Field(None, ge=0, lt=360, description="Course in degrees")
    heading: Optional[float] = Field(None, ge=0, lt=360, description="Heading in degrees")
    navigational_status: Optional[NavigationalStatus] = Field(None, description="AIS navigational status")
    
    # Data quality fields
    is_valid_position: bool = Field(default=True, description="Position validity flag")
    is_reasonable_speed: bool = Field(default=True, description="Speed reasonableness flag")
    data_quality_score: float = Field(default=1.0, ge=0, le=1, description="Overall data quality score")
    
    # Zone information
    zone_country: Optional[str] = Field(None, description="Country zone")
    zone_eez: Optional[str] = Field(None, description="Exclusive Economic Zone")
    zone_continent: Optional[str] = Field(None, description="Continent zone")
    zone_hrz_v2: Optional[str] = Field(None, description="High Risk Zone v2")
    zone_sanction: Optional[str] = Field(None, description="Sanction zone")
    zone_seca: Optional[str] = Field(None, description="SECA zone")
    zone_port: Optional[str] = Field(None, description="Port zone")
    
    # CDC metadata
    cdc_operation: Optional[CDCOperation] = Field(None, description="CDC operation type")
    cdc_timestamp: Optional[datetime] = Field(None, description="CDC operation timestamp")
    cdc_sequence_number: Optional[int] = Field(None, description="CDC sequence number")
    
    # Processing metadata
    ingestion_timestamp: Optional[datetime] = Field(None, description="Data ingestion timestamp")
    processing_timestamp: Optional[datetime] = Field(None, description="Processing timestamp")


class VesselMetadata(BaseModel):
    """Schema for vessel metadata."""
    
    imo: str = Field(..., description="International Maritime Organization number")
    vessel_name: str = Field(..., description="Current vessel name")
    vessel_type: Optional[str] = Field(None, description="Vessel type classification")
    gross_tonnage: Optional[float] = Field(None, ge=0, description="Gross tonnage")
    deadweight_tonnage: Optional[float] = Field(None, ge=0, description="Deadweight tonnage")
    length_overall: Optional[float] = Field(None, ge=0, description="Length overall in meters")
    beam: Optional[float] = Field(None, ge=0, description="Beam in meters")
    flag_country: Optional[str] = Field(None, description="Flag state")
    
    # Company information
    owner_code: Optional[str] = Field(None, description="Registered owner code")
    operator_code: Optional[str] = Field(None, description="Operator company code")
    technical_manager_code: Optional[str] = Field(None, description="Technical manager code")
    ship_manager_code: Optional[str] = Field(None, description="Ship manager code")
    group_beneficial_owner_code: Optional[str] = Field(None, description="Group beneficial owner code")
    
    # Temporal validity
    valid_from_datetime: datetime = Field(..., description="Valid from timestamp")
    valid_to_datetime: Optional[datetime] = Field(None, description="Valid to timestamp")
    is_current_record: bool = Field(default=True, description="Current record flag")
    
    # CDC metadata
    cdc_operation: Optional[CDCOperation] = Field(None, description="CDC operation type")
    cdc_timestamp: Optional[datetime] = Field(None, description="CDC operation timestamp")
    cdc_sequence_number: Optional[int] = Field(None, description="CDC sequence number")
    
    # Processing metadata
    ingestion_timestamp: Optional[datetime] = Field(None, description="Data ingestion timestamp")
    processing_timestamp: Optional[datetime] = Field(None, description="Processing timestamp")


class ActivityReport(BaseModel):
    """Schema for vessel activity reports."""
    
    vesselimo: str = Field(..., description="Vessel IMO number")
    vesselname: Optional[str] = Field(None, description="Vessel name")
    vesseltypename: Optional[str] = Field(None, description="Vessel type name")
    vesselownername: Optional[str] = Field(None, description="Vessel owner name")
    
    # Activity timing
    entrytime: datetime = Field(..., description="Zone entry timestamp")
    exittime: Optional[datetime] = Field(None, description="Zone exit timestamp")
    timespent: Optional[float] = Field(None, ge=0, description="Time spent in zone (hours)")
    
    # Geographic information
    geoareaname: str = Field(..., description="Geographic area name")
    geoareatype: str = Field(..., description="Geographic area type")
    geoareacategory: str = Field(..., description="Geographic area category")
    geoarealevel: str = Field(..., description="Geographic area level")
    
    # Port-specific fields
    geoareaid: Optional[str] = Field(None, description="Geographic area ID")
    geoarealeveleezsearegion: Optional[str] = Field(None, description="EEZ/Sea region")
    geoarealevelwarzone: Optional[str] = Field(None, description="War zone")
    geoarealevelcountry: Optional[str] = Field(None, description="Country")
    geoarealevelcontinent: Optional[str] = Field(None, description="Continent")
    geoarealevelsubcontinent: Optional[str] = Field(None, description="Subcontinent")
    
    # Report metadata
    report_type: str = Field(..., description="Type of activity report")
    business_date: datetime = Field(..., description="Business date")
    data_lineage: str = Field(default="bronze->silver->gold", description="Data lineage")
    
    # Processing metadata
    gold_processing_timestamp: Optional[datetime] = Field(None, description="Gold layer processing timestamp")
    
    # CDF metadata
    change_type: Optional[str] = Field(None, description="Change type from CDF")
    commit_version: Optional[int] = Field(None, description="Delta commit version")
    commit_timestamp: Optional[datetime] = Field(None, description="Delta commit timestamp")


class JourneyReport(BaseModel):
    """Schema for vessel journey reports."""
    
    vesselimo: str = Field(..., description="Vessel IMO number")
    vesselname: Optional[str] = Field(None, description="Vessel name")
    vesseltypename: Optional[str] = Field(None, description="Vessel type name")
    vesselownername: Optional[str] = Field(None, description="Vessel owner name")
    
    # Journey details
    departuretime: datetime = Field(..., description="Departure timestamp")
    geoareanamedeparture: str = Field(..., description="Departure port/area name")
    arrivaltime: Optional[datetime] = Field(None, description="Arrival timestamp")
    geoareanamearrival: Optional[str] = Field(None, description="Arrival port/area name")
    
    # Journey metrics
    journey_duration_hours: Optional[float] = Field(None, ge=0, description="Journey duration in hours")
    estimated_distance_nm: Optional[float] = Field(None, ge=0, description="Estimated distance in nautical miles")
    average_speed_knots: Optional[float] = Field(None, ge=0, description="Average speed in knots")
    
    # Classification
    route_type: Optional[str] = Field(None, description="Route type (LINEAR, CIRCULAR, RETURN)")
    journey_classification: Optional[str] = Field(None, description="Journey length classification")
    
    # Metadata
    date: datetime = Field(..., description="Journey date")
    isexit: Optional[bool] = Field(None, description="Exit flag")
    business_date: datetime = Field(..., description="Business date")
    
    # Processing metadata
    gold_processing_timestamp: Optional[datetime] = Field(None, description="Gold layer processing timestamp")


class MaterializedViewConfig(BaseModel):
    """Configuration for materialized views."""
    
    view_name: str = Field(..., description="Materialized view name")
    cluster_by: List[str] = Field(default_factory=list, description="Clustering columns")
    partition_by: List[str] = Field(default_factory=list, description="Partitioning columns")
    max_staleness_hours: int = Field(default=24, description="Maximum staleness in hours")
    refresh_interval_minutes: Optional[int] = Field(None, description="Refresh interval in minutes")
    enable_refresh: bool = Field(default=True, description="Enable automatic refresh")
    allow_non_incremental: bool = Field(default=True, description="Allow non-incremental definition")
