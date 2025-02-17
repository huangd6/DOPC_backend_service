import aiohttp
from aiohttp import ClientSession
from pydantic import BaseModel, field_validator, Field, ValidationError
from aiohttp import web
import json
import asyncio
import sys
import tomli
from server import create_app, start_server
import time
import logging
from datetime import datetime
import os

from typing import Dict, Any, Tuple, Annotated, List
from math import radians, sin, cos, sqrt, atan2

# Load config
with open('config.toml', 'rb') as f:
    config = tomli.load(f)
    
DOPC_PORT = config['general']['dopc_port']
DOPC_END_POINT = config['general']['dopc_end_point']
SERVICE_PORT_START = config['dopc_balancer']['service_port_start']
NUM_SERVICES = config['dopc_balancer']['num_services']
N_MAX_REQUEST = config['dopc_service']['n_max_request'] # max number of concurrent user requests 

MOCK_HOST_ASSIGNMENT_FLAG = config['dopc_service']['mock_home_assignment_flag']
if config['dopc_service']['mock_home_assignment_flag']:
    BASE_API_URL = config['dopc_service']['mock_base_api_url']
else:
    BASE_API_URL = config['dopc_service']['base_api_url']


class DeliveryDetails(BaseModel):
    fee: int = Field(..., gt=0, description="Delivery fee in cents")
    distance: int = Field(..., gt=0, description="Distance in meters")

    @field_validator('fee')
    @classmethod  # field_validator requires this
    def validate_fee(cls, v):
        if v < 0:
            raise ValueError('Delivery fee cannot be negative')
        if v > 1500000:  # 15000 EUR
            raise ValueError('Delivery fee exceeds maximum allowed value')
        return v

    @field_validator('distance')
    @classmethod
    def validate_distance(cls, v):
        if v < 0:
            raise ValueError('Distance cannot be negative')
        if v > 2000000:  # 2000km
            raise ValueError('Distance exceeds maximum allowed value')
        return v
# All the money related information (prices, fees, etc) are in the lowest denomination of the local currency. In euro countries they are in cents, in Sweden they are in öre, and in Japan they are in yen.
class DeliveryPriceResponse(BaseModel):
    total_price: int = Field(..., gt=0, description="Total price in cents")
    small_order_surcharge: int = Field(..., ge=0, description="Surcharge for small orders in cents")
    cart_value: int = Field(..., gt=0, description="Cart value in cents")
    delivery: DeliveryDetails
    
    @field_validator('total_price')
    @classmethod
    def validate_total_price(cls, v, info):  # info instead of values
        values = info.data  # access other fields through info.data
        if 'cart_value' in values and 'delivery' in values:
            expected = values['cart_value'] + values['delivery'].fee
            if 'small_order_surcharge' in values:
                expected += values['small_order_surcharge']
            if v != expected:
                raise ValueError('Total price does not match component sum')
        return v

class DeliveryOrderRequest(BaseModel):
    venue_slug: str = Field(..., description="Venue identifier")
    cart_value: int = Field(..., gt=0, description="Cart value in cents")
    user_lat: float = Field(..., description="User latitude")
    user_lon: float = Field(..., description="User longitude")

    @field_validator('venue_slug')
    @classmethod
    def validate_venue_slug(cls, v: str) -> str:
        if not v or not isinstance(v, str):
            raise ValueError("Venue slug must be a non-empty string")
        return v

    @field_validator('user_lat')
    @classmethod
    def validate_latitude(cls, v: float) -> float:
        if not -90 <= v <= 90:
            raise ValueError("Latitude must be between -90 and 90 degrees")
        return v

    @field_validator('user_lon')
    @classmethod
    def validate_longitude(cls, v: float) -> float:
        if not -180 <= v <= 180:
            raise ValueError("Longitude must be between -180 and 180 degrees")
        return v

class APIConnectionPool:
    def __init__(self, pool_size: int = 5, health_check_interval: int = 30):
        self.pool_size = pool_size
        self.health_check_interval = health_check_interval  # Check every 30 seconds
        self.health_check_task = None
        
        # Use lists for sessions
        self.static_sessions = []
        self.dynamic_sessions = []
        
        # Add indices for round-robin
        self.static_index = 0
        self.dynamic_index = 0
        
        self.BASE_API_URL = BASE_API_URL # "https://consumer-api.development.dev.woltapi.com/home-assignment-api/v1"

    async def create_session(self) -> ClientSession:
        """Create a session with basic timeout"""
        timeout = aiohttp.ClientTimeout(total=30)  # 30 second total timeout
        return ClientSession(timeout=timeout)

    async def start(self):
        """Initialize connection pools"""
        # Print server configuration information
        print("\n=== Server Configuration ===")
        print(f"Base API URL: {self.BASE_API_URL}")
        print(f"Pool Size: {self.pool_size}")
        print(f"Health Check Interval: {self.health_check_interval} seconds")
        print(f"Max Concurrent Requests: {N_MAX_REQUEST}")
        print(f"Mock Mode: {MOCK_HOST_ASSIGNMENT_FLAG}")
        print(f"Service Port: {DOPC_PORT}")
        print("===========================\n")
        print("Starting API connection pools...")
        for _ in range(self.pool_size):
            self.static_sessions.append(await self.create_session())
            self.dynamic_sessions.append(await self.create_session())
        print(f"{self.pool_size} static connections and {self.pool_size} dynamic connections have been established.")
        # Start monitoring as background task
        self.health_check_task = asyncio.create_task(self.monitor_sessions())

    async def stop(self):
        """Cleanup all connections"""
        # Cancel health monitoring
        if self.health_check_task:
            self.health_check_task.cancel()
            try:
                await self.health_check_task
            except asyncio.CancelledError:
                pass

        # Close all sessions
        for session_pool in [self.static_sessions, self.dynamic_sessions]:
            for session in session_pool:
                if not session.closed:
                    await session.close()

    async def check_session_health(self, session: ClientSession, session_type: str) -> bool:
        """Check if a session is healthy by making a test request"""
        try:
            url = f"{self.BASE_API_URL}/venues/home-assignment-venue-helsinki/{session_type}"
            async with session.get(url) as response:
                return response.status != 500  # Any non-500 response is considered healthy
        except Exception as e:
            print(f"Health check failed for {session_type} session: {str(e)}")
            return False

    async def replace_session(self, session: ClientSession, sessions: List[ClientSession], index: int):
        """Replace an unhealthy session with a new one"""
        try:
            if not session.closed:
                await session.close()  # Clean up old session
        except:
            pass
        
        new_session = await self.create_session()
        sessions[index] = new_session
        print(f"Replaced session at index {index}")

    async def monitor_sessions(self):
        """Continuously monitor session health and replace unhealthy ones"""
        while True:
            try:
                # Check static sessions
                for i, session in enumerate(self.static_sessions):
                    if not await self.check_session_health(session, "static"):
                        print(f"Replacing unhealthy static session {i}")
                        await self.replace_session(session, self.static_sessions, i)

                # Check dynamic sessions
                for i, session in enumerate(self.dynamic_sessions):
                    if not await self.check_session_health(session, "dynamic"):
                        print(f"Replacing unhealthy dynamic session {i}")
                        await self.replace_session(session, self.dynamic_sessions, i)

                await asyncio.sleep(self.health_check_interval)  # Wait 30 seconds
            except asyncio.CancelledError:  # Add this to handle cancellation
                print("Health monitoring stopped")
                break
            except Exception as e:
                print(f"Error in session monitoring: {str(e)}")
                await asyncio.sleep(5)  # Short delay before retry

    def get_static_session(self) -> ClientSession:
        """Get next static session using round-robin"""
        session = self.static_sessions[self.static_index]
        self.static_index = (self.static_index + 1) % self.pool_size
        return session

    def get_dynamic_session(self) -> ClientSession:
        """Get next dynamic session using round-robin"""
        session = self.dynamic_sessions[self.dynamic_index]
        self.dynamic_index = (self.dynamic_index + 1) % self.pool_size
        return session

class DeliveryOrderPriceCalculator:
    #BASE_API_URL = "https://consumer-api.development.dev.woltapi.com/home-assignment-api/v1"
    BASE_API_URL = BASE_API_URL
    
    def __init__(self, static_session: ClientSession, dynamic_session: ClientSession):
        self.static_session = static_session
        self.dynamic_session = dynamic_session
        logger.info("Initialized DOPC calculator")

    async def make_request(self, session: ClientSession, url: str) -> Tuple[bool, Dict[str, Any] | str]:
        """Make request with timeout handling"""
        try:
            async with session.get(url) as response:
                if response.status == 200:
                    return True, await response.json()
                else:
                    return False, f"Request failed with status: {response.status}"
        except asyncio.TimeoutError:
            return False, "Request timed out"
        except Exception as e:
            return False, f"Request error: {str(e)}"

    async def get_venue_static_data(self, venue_slug: str) -> Tuple[bool, Dict[str, Any] | str]:
        """Fetch venue static data with caching and rate limiting"""
        url = f"{self.BASE_API_URL}/venues/{venue_slug}/static"
        return await self.make_request(self.static_session, url)
        
    async def get_venue_dynamic_data(self, venue_slug: str) -> Tuple[bool, Dict[str, Any] | str]:
        """Fetch venue dynamic data with rate limiting"""
        url = f"{self.BASE_API_URL}/venues/{venue_slug}/dynamic"
        return await self.make_request(self.dynamic_session, url)

    def calculate_distance(self, lat1: float, lon1: float, lat2: float, lon2: float) -> int:
        """Calculate distance between two points in meters using Haversine formula"""
        R = 6371000  # Earth's radius in meters

        lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
        dlat = lat2 - lat1
        dlon = lon2 - lon1

        a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
        c = 2 * atan2(sqrt(a), sqrt(1-a))
        distance = R * c

        return round(distance)

    def calculate_delivery_fee(self, distance: int, delivery_specs: Dict[str, Any]) -> Tuple[bool, int | str]:
        """
        Calculate delivery fee based on distance and delivery specifications
        Returns: (success, fee_or_error_message)
        """
        try:
            pricing = delivery_specs['delivery_pricing']
            base_price = pricing['base_price']
            
            for range_spec in pricing['distance_ranges']:
                min_dist = range_spec['min']
                max_dist = range_spec['max']
                
                if max_dist == 0:
                    if distance >= min_dist:
                        return False, f"Delivery distance {distance}m exceeds maximum allowed distance {min_dist}m"
                    continue
                
                if min_dist <= distance <= max_dist:
                    return True, base_price + range_spec['a'] + (range_spec['b'] * distance // 10)
            
            return False, f"No suitable delivery fee range found for distance {distance}m"
        except KeyError as e:
            return False, f"Invalid delivery specifications: {str(e)}"

    def calculate_small_order_surcharge(self, cart_value: int, min_value: int) -> int:
        """Calculate surcharge for small orders (in cents)"""
        if cart_value < min_value:
            return min_value - cart_value
        return 0

    def validate_coordinates(self, lat: float, lon: float) -> Tuple[bool, str]:
        """Validate latitude and longitude values"""
        try:
            if not isinstance(lat, (int, float)) or not isinstance(lon, (int, float)):
                return False, "Coordinates must be numeric"
            if not -90 <= lat <= 90:
                return False, f"Invalid latitude: {lat}. Must be between -90 and 90"
            if not -180 <= lon <= 180:
                return False, f"Invalid longitude: {lon}. Must be between -180 and 180"
            return True, ""
        except Exception as e:
            return False, f"Coordinate validation error: {str(e)}"

    def extract_venue_coordinates(self, venue_static: Dict) -> Tuple[bool, Tuple[float, float] | str]:
        """Extract and validate venue coordinates from static data"""
        try:
            # Navigate through the nested structure
            venue_raw = venue_static.get('venue_raw')
            if not venue_raw:
                return False, "Missing venue_raw in static data"

            location = venue_raw.get('location')
            if not location:
                return False, "Missing location in venue_raw"

            coordinates = location.get('coordinates')
            if not coordinates or len(coordinates) != 2:
                return False, "Invalid or missing coordinates"

            # In the API response, coordinates are [longitude, latitude]
            lon, lat = coordinates[0], coordinates[1]
            
            # Validate coordinates
            valid, error = self.validate_coordinates(lat, lon)
            if not valid:
                return False, error

            return True, (lat, lon)
            
        except Exception as e:
            return False, f"Failed to extract venue coordinates: {str(e)}"

    async def calculate_price(self, venue_slug: str, cart_value: int, 
                            user_lat: float, user_lon: float) -> Tuple[bool, Dict[str, Any] | str]:
        """
        Main method to process delivery order price calculation
        Returns: (success, result_or_error_message)
        """
        logger.info(f"Processing request for venue: {venue_slug}")

        # Fetch venue data
        success, venue_static = await self.get_venue_static_data(venue_slug)
        if not success:
            logger.error(f"Failed to get static data: {venue_static}")
            return False, venue_static

        # Extract and validate venue coordinates
        success, result = self.extract_venue_coordinates(venue_static)
        if not success:
            return False, result
        venue_lat, venue_lon = result

        success, venue_dynamic = await self.get_venue_dynamic_data(venue_slug)
        if not success:
            return False, venue_dynamic

        try:
            # Calculate distance
            distance = self.calculate_distance(user_lat, user_lon, venue_lat, venue_lon)

            # Get delivery specifications
            delivery_specs = venue_dynamic['venue_raw']['delivery_specs']

            # Calculate delivery fee
            success, delivery_fee = self.calculate_delivery_fee(distance, delivery_specs)
            if not success:
                return False, delivery_fee

            # Calculate small order surcharge
            small_order_surcharge = self.calculate_small_order_surcharge(
                cart_value, 
                delivery_specs['order_minimum_no_surcharge']
            )

            # Calculate total price in cents (no rounding needed as all values are integers)
            total_price = cart_value + delivery_fee + small_order_surcharge

            return True, {
                "total_price": total_price,
                "small_order_surcharge": small_order_surcharge,
                "cart_value": cart_value,
                "delivery": {
                    "fee": delivery_fee,
                    "distance": distance
                }
            }
        except Exception as e:
            return False, f"Calculation error: {str(e)}"

# Create global connection pool
api_pool = APIConnectionPool(pool_size=5)

# Create a global semaphore to limit concurrent requests
request_semaphore = asyncio.Semaphore(N_MAX_REQUEST)  # Max concurrent requests

# Define handlers
async def calculate_delivery_price(request):
    try:
        # Try to acquire semaphore
        async with request_semaphore:
            # Log incoming request
            current_time = time.strftime("%H:%M:%S")
            print(f"[{current_time}] Received request")
            
            # Get query parameters
            params = request.query
            logger.info(f"Received request with params: {params}")
            
            # Check for required parameters
            required_params = ['venue_slug', 'cart_value', 'user_lat', 'user_lon']
            missing_params = [param for param in required_params if param not in params]
            if missing_params:
                error_msg = f"Missing required parameters: {', '.join(missing_params)}"
                print(f"[{current_time}] Error: {error_msg}")
                return web.json_response(
                    {"success": False, "error": error_msg},
                    status=400
                )
            
            # Validate request parameters
            try:
                request_data = DeliveryOrderRequest(
                    venue_slug=params.get('venue_slug'),
                    cart_value=int(params.get('cart_value')),
                    user_lat=float(params.get('user_lat')),
                    user_lon=float(params.get('user_lon'))
                )
            except ValidationError as e:
                logger.warning(f"Validation error: {str(e)}")
                return web.json_response({
                    "success": False,
                    "error": f"Validation error: {str(e)}"
                }, status=400)
            

            # Get sessions from pool
            static_session = api_pool.get_static_session()
            dynamic_session = api_pool.get_dynamic_session()
            
            # Create calculator with sessions
            calculator = DeliveryOrderPriceCalculator(static_session, dynamic_session)
            
            success, result = await calculator.calculate_price(
                venue_slug=request_data.venue_slug,
                cart_value=request_data.cart_value,
                user_lat=request_data.user_lat,
                user_lon=request_data.user_lon
            )
            
            if not success:
                return web.json_response(
                    {"success": False, "error": result},
                    status=400
                )

            response = DeliveryPriceResponse(**result)
            return web.json_response(response.model_dump())

    except asyncio.TimeoutError:
        return web.json_response(
            {"success": False, "error": "Server too busy"},
            status=503
        )
    except ValueError as e:
        return web.json_response(
            {"success": False, "error": f"Validation error: {str(e)}"},
            status=400
        )

# Add health check endpoint for load_balancer
async def health_check(request):
    return web.json_response({"status": "healthy"})

# Create application
app = create_app(calculate_delivery_price)

# Add health endpoint
app.router.add_get('/health', health_check)

# Add lifecycle hooks
app.on_startup.append(lambda app: api_pool.start())
app.on_cleanup.append(lambda app: api_pool.stop())

def setup_logger(port: int):
    """Setup logger with unique file for each service instance"""
    # Create logs directory if it doesn't exist
    os.makedirs('logs', exist_ok=True)
    
    # Create logger
    logger = logging.getLogger(f'dopc_service_{port}')
    logger.setLevel(logging.INFO)
    
    # Create unique log file for this service instance
    file_handler = logging.FileHandler(f'logs/dopc_service_{port}.log')
    console_handler = logging.StreamHandler()
    
    # Create formatters and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    # Add the handlers to the logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

# Get port from command line args
port = int(sys.argv[1]) if len(sys.argv) > 1 else DOPC_PORT

# Setup logger for this service instance
logger = setup_logger(port)
logger.info(f"Starting DOPC service on port {port}")

if __name__ == '__main__':
    port = int(sys.argv[1]) if len(sys.argv) > 1 else DOPC_PORT
    start_server(app, port)