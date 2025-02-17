from aiohttp import web
import tomli
import sys

# Load config
with open('config.toml', 'rb') as f:
    config = tomli.load(f)

DOPC_PORT = config['general']['dopc_port']
DOPC_END_POINT = config['general']['dopc_end_point']
USE_BALANCER = config['general']['use_balancer_flag']  # Use existing flag

async def handle_unsupported_method(request):
    return web.json_response(
        {
            "success": False,
            "error": f"Method {request.method} not supported. Only GET requests are allowed."
        },
        status=405
    )

def create_app(handler):
    """Create application with routes"""
    app = web.Application()
    
    # Add routes
    app.router.add_get(DOPC_END_POINT, handler)
    app.router.add_put(DOPC_END_POINT, handle_unsupported_method)
    app.router.add_post(DOPC_END_POINT, handle_unsupported_method)
    app.router.add_delete(DOPC_END_POINT, handle_unsupported_method)
    app.router.add_patch(DOPC_END_POINT, handle_unsupported_method)
    
    return app

def start_server(app, port):
    """Run application with specified port"""
    web.run_app(app, host='localhost', port=port)

if __name__ == '__main__':
    if USE_BALANCER:
        from load_balancer import app
        start_server(app, DOPC_PORT)
    else:
        from dopc_service import app
        port = int(sys.argv[1]) if len(sys.argv) > 1 else DOPC_PORT
        start_server(app, port) 