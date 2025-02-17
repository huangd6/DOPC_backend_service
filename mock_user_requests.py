import aiohttp
import asyncio
import random
import time
import tomli
from typing import List, Dict
import sys
import argparse
from collections import defaultdict

# Load config
with open('config.toml', 'rb') as f:
    config = tomli.load(f)

DOPC_PORT = config['general']['dopc_port']
DOPC_END_POINT = config['general']['dopc_end_point']

class UserRequestSimulator:
    def __init__(self, num_users: int = 100, request_delay: float = 0.1, requests_per_user: int = 2):
        self.num_users = num_users
        self.request_delay = request_delay
        self.requests_per_user = requests_per_user
        self.base_url = f"http://localhost:{DOPC_PORT}{DOPC_END_POINT}"
        
        # Sample locations in Helsinki
        self.test_locations = [
            (60.17045, 24.93147),  # Helsinki Central Station
            (60.16866, 24.92538),  # Kamppi
            (60.18526, 24.95083),  # Kallio
            (60.15824, 24.94459),  # Kaivopuisto
            (60.18785, 24.98226)   # Kulosaari
        ]
        
        # Sample cart values (in cents)
        self.cart_values = [
            1000,   # 10 EUR
            1500,   # 15 EUR
            2000,   # 20 EUR
            2500,   # 25 EUR
            3000    # 30 EUR
        ]
        self.concurrent_limit = asyncio.Semaphore(100)  # Match server's limit
        self.stats = defaultdict(int)
        self.response_times = []

    def generate_request_params(self) -> Dict:
        """Generate random request parameters"""
        lat, lon = random.choice(self.test_locations)
        # Add some random variation to location
        lat += random.uniform(-0.002, 0.002)
        lon += random.uniform(-0.002, 0.002)
        
        return {
            'venue_slug': 'home-assignment-venue-helsinki',
            'cart_value': random.choice(self.cart_values),
            'user_lat': lat,
            'user_lon': lon
        }

    async def make_request(self, session: aiohttp.ClientSession, user_id: int) -> None:
        """Make a single request"""
        async with self.concurrent_limit:
            params = self.generate_request_params()
            try:
                start_time = time.time()
                async with session.get(self.base_url, params=params) as response:
                    elapsed = time.time() - start_time
                    self.response_times.append(elapsed)
                    status = response.status
                    
                    if status == 200:
                        result = await response.json()
                        self.stats['success'] += 1
                        print(f"User {user_id:3d} | Status: {status} | Time: {elapsed:.3f}s | "
                              f"Price: {result.get('total_price', 'N/A')} cents")
                    else:
                        error_text = await response.text()
                        if "distance" in error_text:
                            self.stats['distance_exceeded'] += 1
                            print(f"User {user_id:3d} | Distance Exceeded | Time: {elapsed:.3f}s")
                        elif "429" in error_text:
                            self.stats['rate_limited'] += 1
                            print(f"User {user_id:3d} | Rate Limited | Time: {elapsed:.3f}s")
                        else:
                            self.stats['other_errors'] += 1
                            print(f"User {user_id:3d} | Error | Time: {elapsed:.3f}s | {error_text[:100]}")
            except Exception as e:
                self.stats['connection_errors'] += 1
                print(f"User {user_id:3d} | Connection Error: {str(e)}")

    async def simulate_user(self, user_id: int) -> None:
        """Simulate a user making a fixed number of requests"""
        async with aiohttp.ClientSession() as session:
            for req_num in range(self.requests_per_user):
                try:
                    await self.make_request(session, user_id)
                    await asyncio.sleep(self.request_delay + 3)  # Add extra delay between requests
                except Exception as e:
                    print(f"User {user_id:3d} | Error: {str(e)}")
                    await asyncio.sleep(1)

    def print_summary(self):
        """Print test summary"""
        total_requests = sum(self.stats.values())
        print("\n=== Test Summary ===")
        print(f"Total Users: {self.num_users}")
        print(f"Requests per User: {self.requests_per_user}")
        print(f"Total Requests: {total_requests}")
        
        print("\nResponse Breakdown:")
        print(f"✅ Successful: {self.stats['success']} ({self.stats['success']/total_requests*100:.1f}%)")
        print(f"📍 Distance Exceeded: {self.stats['distance_exceeded']} ({self.stats['distance_exceeded']/total_requests*100:.1f}%)")
        print(f"⏳ Rate Limited: {self.stats['rate_limited']} ({self.stats['rate_limited']/total_requests*100:.1f}%)")
        print(f"❌ Other Errors: {self.stats['other_errors']} ({self.stats['other_errors']/total_requests*100:.1f}%)")
        print(f"🔌 Connection Errors: {self.stats['connection_errors']} ({self.stats['connection_errors']/total_requests*100:.1f}%)")
        
        if self.response_times:
            print("\nTiming Statistics:")
            print(f"Average Response Time: {sum(self.response_times)/len(self.response_times):.3f}s")
            print(f"Min Response Time: {min(self.response_times):.3f}s")
            print(f"Max Response Time: {max(self.response_times):.3f}s")

    async def run_simulation(self) -> None:
        """Run the full simulation with multiple users"""
        print(f"Starting simulation with {self.num_users} users...")
        start_time = time.time()
        
        tasks = []
        for user_id in range(self.num_users):
            task = asyncio.create_task(self.simulate_user(user_id))
            tasks.append(task)
        
        try:
            await asyncio.gather(*tasks)
        except KeyboardInterrupt:
            print("\nStopping simulation...")
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            duration = time.time() - start_time
            print(f"\nSimulation completed in {duration:.2f} seconds")
            self.print_summary()

def main():
    parser = argparse.ArgumentParser(description='Simulate user requests to DOPC service')
    parser.add_argument('--users', type=int, default=50, help='Number of users')
    parser.add_argument('--delay', type=float, default=0.5, help='Delay between requests (seconds)')
    parser.add_argument('--requests', type=int, default=2, help='Requests per user')
    args = parser.parse_args()

    simulator = UserRequestSimulator(
        num_users=args.users, 
        request_delay=args.delay,
        requests_per_user=args.requests
    )
    try:
        asyncio.run(simulator.run_simulation())
    except KeyboardInterrupt:
        print("\nSimulation stopped by user")

if __name__ == "__main__":
    main() 