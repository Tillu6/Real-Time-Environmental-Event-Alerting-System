# USGS Earthquake API Data Ingestion Module

import requests
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
import time
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from ..config.settings import settings
from ..utils.helpers import RateLimiter, DataValidator
from ..utils.validators import validate_coordinate, validate_magnitude

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.monitoring.LOG_LEVEL),
    format=settings.monitoring.LOG_FORMAT
)
logger = logging.getLogger(__name__)

@dataclass
class EarthquakeEvent:
    """Data model for earthquake events"""
    event_id: str
    magnitude: float
    location: str
    latitude: float
    longitude: float
    depth: float
    timestamp: datetime
    place: str
    event_type: str
    significance: int
    alert_level: Optional[str] = None
    tsunami_warning: bool = False
    felt_reports: Optional[int] = None
    cdi: Optional[float] = None  # Community Decimal Intensity
    mmi: Optional[float] = None  # Modified Mercalli Intensity
    net: Optional[str] = None  # Network
    nst: Optional[int] = None  # Number of seismic stations
    gap: Optional[float] = None  # Azimuthal gap
    dmin: Optional[float] = None  # Distance to nearest station
    rms: Optional[float] = None  # Root mean square
    updated: Optional[datetime] = None
    detail_url: Optional[str] = None
    ingestion_timestamp: datetime = None
    
    def __post_init__(self):
        if self.ingestion_timestamp is None:
            self.ingestion_timestamp = datetime.utcnow()

class USGSEarthquakeIngestion:
    """
    USGS Earthquake API Data Ingestion Client
    Handles real-time earthquake data retrieval with rate limiting and error handling
    """
    
    def __init__(self):
        self.base_url = settings.api.USGS_BASE_URL
        self.rate_limiter = RateLimiter(
            max_requests=settings.api.USGS_RATE_LIMIT,
            time_window=60  # per minute
        )
        self.validator = DataValidator()
        
        # Configure requests session with retry strategy
        self.session = requests.Session()
        retry_strategy = Retry(
            total=3,
            status_forcelist=[429, 500, 502, 503, 504],
            backoff_factor=1,
            allowed_methods=["GET"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        # Set timeout and headers
        self.session.headers.update({
            'User-Agent': 'Environmental-Alert-System/1.0 (Research Project)',
            'Accept': 'application/json'
        })
        
        logger.info("USGS Earthquake ingestion client initialized")
    
    def get_recent_earthquakes(
        self, 
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        min_magnitude: float = 2.5,
        limit: int = 1000,
        bbox: Optional[Dict[str, float]] = None
    ) -> List[EarthquakeEvent]:
        """
        Retrieve recent earthquake events from USGS API
        
        Args:
            start_time: Start time for earthquake search (default: last hour)
            end_time: End time for earthquake search (default: now)
            min_magnitude: Minimum magnitude threshold
            limit: Maximum number of events to return
            bbox: Bounding box {'min_lat', 'max_lat', 'min_lon', 'max_lon'}
        
        Returns:
            List of EarthquakeEvent objects
        """
        
        # Set default time range (last hour if not specified)
        if start_time is None:
            start_time = datetime.utcnow() - timedelta(hours=1)
        if end_time is None:
            end_time = datetime.utcnow()
        
        # Build query parameters
        params = {
            'format': 'geojson',
            'starttime': start_time.strftime('%Y-%m-%dT%H:%M:%S'),
            'endtime': end_time.strftime('%Y-%m-%dT%H:%M:%S'),
            'minmagnitude': min_magnitude,
            'limit': limit,
            'orderby': 'time-asc'
        }
        
        # Add bounding box if specified
        if bbox:
            params.update({
                'minlatitude': bbox['min_lat'],
                'maxlatitude': bbox['max_lat'],
                'minlongitude': bbox['min_lon'],
                'maxlongitude': bbox['max_lon']
            })
        
        try:
            # Rate limiting
            self.rate_limiter.wait_if_needed()
            
            logger.info(f"Fetching earthquakes: {start_time} to {end_time}, min_mag={min_magnitude}")
            
            # Make API request
            response = self.session.get(
                self.base_url,
                params=params,
                timeout=30
            )
            response.raise_for_status()
            
            # Parse response
            data = response.json()
            earthquakes = self._parse_earthquake_data(data)
            
            logger.info(f"Successfully retrieved {len(earthquakes)} earthquake events")
            return earthquakes
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse API response: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error in earthquake ingestion: {e}")
            raise
    
    def get_significant_earthquakes(self, days_back: int = 7) -> List[EarthquakeEvent]:
        """Get significant earthquakes from the past N days"""
        
        start_time = datetime.utcnow() - timedelta(days=days_back)
        
        params = {
            'format': 'geojson',
            'starttime': start_time.strftime('%Y-%m-%dT%H:%M:%S'),
            'minsig': 600,  # Significance threshold
            'orderby': 'time-desc',
            'limit': 100
        }
        
        try:
            self.rate_limiter.wait_if_needed()
            
            logger.info(f"Fetching significant earthquakes from last {days_back} days")
            
            response = self.session.get(
                self.base_url,
                params=params,
                timeout=30
            )
            response.raise_for_status()
            
            data = response.json()
            earthquakes = self._parse_earthquake_data(data)
            
            logger.info(f"Retrieved {len(earthquakes)} significant earthquake events")
            return earthquakes
            
        except Exception as e:
            logger.error(f"Error fetching significant earthquakes: {e}")
            raise
    
    def _parse_earthquake_data(self, api_data: Dict[str, Any]) -> List[EarthquakeEvent]:
        """Parse USGS API response into EarthquakeEvent objects"""
        
        earthquakes = []
        
        try:
            features = api_data.get('features', [])
            
            for feature in features:
                try:
                    # Extract geometry
                    geometry = feature.get('geometry', {})
                    coordinates = geometry.get('coordinates', [])
                    
                    if len(coordinates) < 3:
                        logger.warning(f"Invalid coordinates for event: {feature.get('id')}")
                        continue
                    
                    longitude, latitude, depth = coordinates[:3]
                    
                    # Validate coordinates
                    if not validate_coordinate(latitude, longitude):
                        logger.warning(f"Invalid coordinates: lat={latitude}, lon={longitude}")
                        continue
                    
                    # Extract properties
                    props = feature.get('properties', {})
                    
                    # Validate magnitude
                    magnitude = props.get('mag')
                    if not validate_magnitude(magnitude):
                        logger.warning(f"Invalid magnitude: {magnitude}")
                        continue
                    
                    # Parse timestamp
                    time_ms = props.get('time')
                    timestamp = datetime.utcfromtimestamp(time_ms / 1000) if time_ms else datetime.utcnow()
                    
                    # Parse updated timestamp
                    updated_ms = props.get('updated')
                    updated = datetime.utcfromtimestamp(updated_ms / 1000) if updated_ms else None
                    
                    # Create EarthquakeEvent object
                    earthquake = EarthquakeEvent(
                        event_id=feature.get('id', ''),
                        magnitude=magnitude,
                        location=f"{latitude:.4f},{longitude:.4f}",
                        latitude=latitude,
                        longitude=longitude,
                        depth=depth if depth is not None else 0.0,
                        timestamp=timestamp,
                        place=props.get('place', 'Unknown'),
                        event_type=props.get('type', 'earthquake'),
                        significance=props.get('sig', 0),
                        alert_level=props.get('alert'),
                        tsunami_warning=bool(props.get('tsunami', 0)),
                        felt_reports=props.get('felt'),
                        cdi=props.get('cdi'),
                        mmi=props.get('mmi'),
                        net=props.get('net'),
                        nst=props.get('nst'),
                        gap=props.get('gap'),
                        dmin=props.get('dmin'),
                        rms=props.get('rms'),
                        updated=updated,
                        detail_url=props.get('detail')
                    )
                    
                    # Validate event data
                    if self.validator.validate_earthquake_event(earthquake):
                        earthquakes.append(earthquake)
                    else:
                        logger.warning(f"Event validation failed: {earthquake.event_id}")
                        
                except Exception as e:
                    logger.warning(f"Error parsing earthquake event: {e}")
                    continue
            
            return earthquakes
            
        except Exception as e:
            logger.error(f"Error parsing earthquake data: {e}")
            raise
    
    def get_live_feed(self, feed_type: str = "significant_month") -> List[EarthquakeEvent]:
        """
        Get data from USGS live feeds
        
        Args:
            feed_type: Type of feed (significant_month, 4.5_month, 2.5_month, etc.)
        """
        
        feed_urls = {
            "significant_month": "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/significant_month.geojson",
            "4.5_month": "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/4.5_month.geojson",
            "2.5_month": "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_month.geojson",
            "all_hour": "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson",
            "all_day": "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"
        }
        
        feed_url = feed_urls.get(feed_type)
        if not feed_url:
            raise ValueError(f"Unknown feed type: {feed_type}")
        
        try:
            self.rate_limiter.wait_if_needed()
            
            logger.info(f"Fetching live feed: {feed_type}")
            
            response = self.session.get(feed_url, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            earthquakes = self._parse_earthquake_data(data)
            
            logger.info(f"Retrieved {len(earthquakes)} events from {feed_type} feed")
            return earthquakes
            
        except Exception as e:
            logger.error(f"Error fetching live feed {feed_type}: {e}")
            raise
    
    def to_dataframe(self, earthquakes: List[EarthquakeEvent]) -> pd.DataFrame:
        """Convert earthquake events to pandas DataFrame"""
        
        if not earthquakes:
            return pd.DataFrame()
        
        # Convert to list of dictionaries
        data = [asdict(eq) for eq in earthquakes]
        
        # Create DataFrame
        df = pd.DataFrame(data)
        
        # Convert timestamp columns to proper datetime
        timestamp_cols = ['timestamp', 'updated', 'ingestion_timestamp']
        for col in timestamp_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], utc=True)
        
        return df
    
    def save_to_bronze_layer(self, earthquakes: List[EarthquakeEvent], path: str) -> None:
        """Save earthquake data to bronze layer (raw data)"""
        
        try:
            df = self.to_dataframe(earthquakes)
            
            if df.empty:
                logger.warning("No earthquake data to save")
                return
            
            # Add metadata
            df['data_source'] = 'usgs_api'
            df['ingestion_batch_id'] = f"earthquake_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
            df['schema_version'] = '1.0'
            
            # Save as parquet (optimized for Fabric Lakehouse)
            df.to_parquet(
                path,
                engine='pyarrow',
                compression='snappy',
                index=False
            )
            
            logger.info(f"Saved {len(earthquakes)} earthquake events to bronze layer: {path}")
            
        except Exception as e:
            logger.error(f"Error saving earthquake data to bronze layer: {e}")
            raise

# Usage example and factory function
def create_earthquake_ingestion() -> USGSEarthquakeIngestion:
    """Factory function to create earthquake ingestion client"""
    return USGSEarthquakeIngestion()

# Export main classes
__all__ = ['USGSEarthquakeIngestion', 'EarthquakeEvent', 'create_earthquake_ingestion']
