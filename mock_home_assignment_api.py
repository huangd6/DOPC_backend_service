from aiohttp import web
import asyncio
import time

class MockWoltAPI:
    def __init__(self):
        self.static_data = {
            "venue_raw": {
                "location": {
                    "coordinates": [24.93087, 60.17094]  # Helsinki coordinates
                }
            }
        }
        self.dynamic_data = {
            "venue_raw": {
                "delivery_specs": {
                    "delivery_pricing": {
                        "base_price": 390,
                        "distance_ranges": [
                            {"min": 0, "max": 1000, "a": 0, "b": 0},
                            {"min": 1000, "max": 2000, "a": 100, "b": 0},
                            {"min": 2000, "max": 3000, "a": 200, "b": 0}
                        ]
                    },
                    "order_minimum_no_surcharge": 1000
                }
            }
        }
        # Track metrics
        self.request_count = 0
        self.start_time = time.time()

    async def get_static_data(self, request):
        """Handle static data requests"""
        # await asyncio.sleep(31)  # Force timeout
        self.request_count += 1
        venue_slug = request.match_info['venue_slug']
        return web.json_response(self.static_data)

    async def get_dynamic_data(self, request):
        """Handle dynamic data requests"""
        # await asyncio.sleep(5)  # 5 second delay
        self.request_count += 1
        venue_slug = request.match_info['venue_slug']
        return web.json_response(self.dynamic_data)

    def print_stats(self):
        """Print API statistics"""
        duration = time.time() - self.start_time
        rps = self.request_count / duration if duration > 0 else 0
        print(f"\n=== API Statistics ===")
        print(f"Total Requests: {self.request_count}")
        print(f"Running Time: {duration:.1f} seconds")
        print(f"Requests/Second: {rps:.1f}")

async def init_app():
    app = web.Application()
    api = MockWoltAPI()
    
    # Add routes
    app.router.add_get('/home-assignment-api/v1/venues/{venue_slug}/static', api.get_static_data)
    app.router.add_get('/home-assignment-api/v1/venues/{venue_slug}/dynamic', api.get_dynamic_data)
    
    # Add shutdown handler to print stats
    async def on_shutdown(app):
        api.print_stats()
    app.on_shutdown.append(on_shutdown)
    
    return app

if __name__ == '__main__':
    app = init_app()
    web.run_app(app, host='localhost', port=10000) 