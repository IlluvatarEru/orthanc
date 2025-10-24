#!/usr/bin/env python3
"""
Launch script for the Orthanc webapp frontend.
"""
import argparse
import logging
from frontend.src.webapp import app

def main():
    """Launch the webapp."""
    parser = argparse.ArgumentParser(description='Launch Orthanc webapp frontend')
    parser.add_argument('--host', default='0.0.0.0', help='Host to bind to (default: 0.0.0.0)')
    parser.add_argument('--port', type=int, default=5000, help='Port to bind to (default: 5000)')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')
    parser.add_argument('--api-url', default='http://localhost:8000', help='API server URL (default: http://localhost:8000)')
    
    args = parser.parse_args()
    
    # Configure logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    # Update API client URL if provided
    if args.api_url != 'http://localhost:8000':
        from frontend.src.webapp_api_client import api_client
        api_client.base_url = args.api_url.rstrip('/')
        logger.info(f"API client configured to use: {api_client.base_url}")
    
    logger.info(f"Starting Orthanc webapp on {args.host}:{args.port}")
    logger.info(f"API server expected at: {args.api_url}")
    logger.info("Webapp will show errors if API server is not running")
    
    # Run the Flask app
    app.run(debug=args.debug, host=args.host, port=args.port)

if __name__ == '__main__':
    main()
