from aiohttp import web, ClientSession, ClientTimeout
import asyncio
import subprocess
import sys
from typing import Dict, List
import tomli
from server import handle_unsupported_method, create_app, start_server
import time  
import urllib.parse

class LoadBalancer:
    def __init__(self, num_services: int, base_port: int, host: str):
        self.num_services = num_services
        self.base_port = base_port
        self.host = host
        self.services: Dict[int, subprocess.Popen] = {}
        self.sessions: Dict[int, ClientSession] = {}
        self.healthy_ports: List[int] = []
        self.current_index = 0
        
    async def start(self):
        """Initialize services and establish connections"""
        print(f"Starting {self.num_services} DOPC services...")
        for i in range(self.num_services):
            port = self.base_port + i
            # Start service process
            process = subprocess.Popen(
                ["python", "dopc_service.py", str(port)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True  # Get text output
            )
            self.services[port] = process
            
            # Create persistent session
            timeout = ClientTimeout(total=5)
            session = ClientSession(timeout=timeout)
            self.sessions[port] = session
            
            print(f"Started DOPC service on port {port}")
            self.healthy_ports.append(port)
            
            # Wait for service to start
            await asyncio.sleep(2)  # Give each service time to initialize

        # Start health check loop
        asyncio.create_task(self.health_check_loop())

    async def stop(self):
        """Cleanup services and connections"""
        # Close all sessions
        for session in self.sessions.values():
            await session.close()
        
        # Stop all processes
        for process in self.services.values():
            process.terminate()

    async def health_check_loop(self):
        """Continuously monitor service health"""
        while True:
            for port in list(self.services.keys()):
                healthy = await self.check_service_health(port)
                if healthy and port not in self.healthy_ports:
                    self.healthy_ports.append(port)
                elif not healthy and port in self.healthy_ports:
                    self.healthy_ports.remove(port)
            await asyncio.sleep(5)  # Check every 5 seconds

    async def check_service_health(self, port: int) -> bool:
        """Check if a service is responding"""
        try:
            session = self.sessions[port]
            url = f"http://{self.host}:{port}/health"
            async with session.get(url) as response:
                return response.status == 200
        except:
            # Format time as HH:MM:SS
            current_time = time.strftime("%H:%M:%S")
            print(f"[{current_time}] {self.host}:{port} does not respond.", file=sys.stderr)
            return False

    async def select_next_service(self) -> int:
        """Select next healthy service"""
        if not self.healthy_ports:
            raise Exception("No healthy services available")
            
        port = self.healthy_ports[self.current_index]
        self.current_index = (self.current_index + 1) % len(self.healthy_ports)
        return port

    async def forward_request(self, request):
        """Forward request to selected service using persistent session"""
        try:
            port = await self.select_next_service()
            current_time = time.strftime("%H:%M:%S")
            print(f"[{current_time}] Forwarding to port {port}")
            
            params = dict(request.query)
            session = self.sessions[port]
            url = f"http://{self.host}:{port}{DOPC_END_POINT}"
            
            async with session.get(url, params=params) as response:
                print(f"[{current_time}] Response from {port}: {response.status}")
                return web.json_response(
                    await response.json(),
                    status=response.status
                )
        except Exception as e:
            return web.json_response(
                {"success": False, "error": f"Load balancer error: {str(e)}"},
                status=500
            )

# Load config
with open('config.toml', 'rb') as f:
    config = tomli.load(f)

DOPC_PORT = config['general']['dopc_port']
DOPC_END_POINT = config['general']['dopc_end_point']
HOST = config['general']['host']
SERVICE_PORT_START = config['dopc_balancer']['service_port_start']
NUM_SERVICES = config['dopc_balancer']['num_services']

# Create load balancer
load_balancer = LoadBalancer(NUM_SERVICES, SERVICE_PORT_START, HOST)

# Route handlers
async def handle_get_request(request):
    return await load_balancer.forward_request(request)

# Create application with routes
app = create_app(handle_get_request)  # This adds the route handler

# Add lifecycle hooks
app.on_startup.append(lambda app: load_balancer.start())
app.on_cleanup.append(lambda app: load_balancer.stop())

if __name__ == '__main__':
    start_server(app, DOPC_PORT)
     