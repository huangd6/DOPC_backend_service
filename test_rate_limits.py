import aiohttp
import asyncio
import time
from datetime import datetime
from collections import defaultdict
import json

class RateLimitTester:
    def __init__(self, base_url: str = "https://consumer-api.development.dev.woltapi.com/home-assignment-api/v1"):
        self.base_url = base_url
        self.venue_slug = "home-assignment-venue-helsinki"
        self.stats = defaultdict(int)
        self.start_time = None
        self.response_times = []

    async def make_request(self, session: aiohttp.ClientSession, endpoint: str) -> bool:
        """Make a request and return True if successful"""
        url = f"{self.base_url}/venues/{self.venue_slug}/{endpoint}"
        start = time.time()
        try:
            async with session.get(url) as response:
                elapsed = time.time() - start
                self.response_times.append(elapsed)
                current_time = time.time() - self.start_time
                
                if response.status == 200:
                    print(f"[{current_time:.2f}s] Success: {url}")
                    self.stats['success'] += 1
                    return True
                elif response.status == 429:
                    print(f"[{current_time:.2f}s] Rate limit hit: {url}")
                    self.stats['rate_limited'] += 1
                    return False
                else:
                    error_text = await response.text()
                    if "exceeds maximum allowed distance" in error_text:
                        self.stats['distance_exceeded'] += 1
                        print(f"[{current_time:.2f}s] Distance exceeded: {url}")
                    else:
                        self.stats['other_errors'] += 1
                        print(f"[{current_time:.2f}s] Error {response.status}: {url}")
                    return False
        except Exception as e:
            self.stats['connection_errors'] += 1
            print(f"Connection error: {str(e)}")
            return False

    def print_summary(self, duration: float, num_requests: int):
        """Print detailed test summary"""
        print("\n=== Test Summary ===")
        print(f"Duration: {duration:.2f} seconds")
        print(f"Total Requests: {num_requests}")
        print("\nResponse Breakdown:")
        print(f"✅ Successful: {self.stats['success']} ({self.stats['success']/num_requests*100:.1f}%)")
        print(f"⏳ Rate Limited: {self.stats['rate_limited']} ({self.stats['rate_limited']/num_requests*100:.1f}%)")
        print(f"📍 Distance Exceeded: {self.stats['distance_exceeded']} ({self.stats['distance_exceeded']/num_requests*100:.1f}%)")
        print(f"❌ Other Errors: {self.stats['other_errors']} ({self.stats['other_errors']/num_requests*100:.1f}%)")
        print(f"🔌 Connection Errors: {self.stats['connection_errors']} ({self.stats['connection_errors']/num_requests*100:.1f}%)")
        
        if self.response_times:
            print("\nTiming Statistics:")
            print(f"Average Response Time: {sum(self.response_times)/len(self.response_times):.3f}s")
            print(f"Min Response Time: {min(self.response_times):.3f}s")
            print(f"Max Response Time: {max(self.response_times):.3f}s")
            print(f"Requests/Second: {num_requests/duration:.1f}")

    async def test_rate_limits(self, num_requests: int = 100, delay: float = 0.1):
        """Test rate limits by making multiple requests"""
        print(f"\nStarting rate limit test at {datetime.now()}")
        print(f"Making {num_requests} requests with {delay}s delay between requests")
        
        self.start_time = time.time()
        async with aiohttp.ClientSession() as session:
            tasks = []
            for i in range(num_requests):
                endpoint = "static" if i % 2 == 0 else "dynamic"
                tasks.append(self.make_request(session, endpoint))
                await asyncio.sleep(delay)

            await asyncio.gather(*tasks)

        duration = time.time() - self.start_time
        self.print_summary(duration, num_requests)

async def main():
    tester = RateLimitTester()
    await tester.test_rate_limits(num_requests=100, delay=0.1)
    
    # Wait before second test
    await asyncio.sleep(60)
    
    tester = RateLimitTester()
    await tester.test_rate_limits(num_requests=200, delay=0.1)

if __name__ == "__main__":
    asyncio.run(main()) 