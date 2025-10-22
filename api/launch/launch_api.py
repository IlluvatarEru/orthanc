"""
Launcher script for the Orthanc Real Estate Analytics API.
"""
import uvicorn
import argparse
import logging
import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from api.src.app import app

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    """Launch the FastAPI application."""
    parser = argparse.ArgumentParser(description="Launch Orthanc Real Estate Analytics API")
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind to (default: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=8000, help="Port to bind to (default: 8000)")
    parser.add_argument("--reload", action="store_true", help="Enable auto-reload for development")
    parser.add_argument("--log-level", default="info", choices=["debug", "info", "warning", "error"], 
                       help="Log level (default: info)")
    
    args = parser.parse_args()
    
    logger.info(f"Starting Orthanc Real Estate Analytics API on {args.host}:{args.port}")
    logger.info(f"API documentation available at: http://{args.host}:{args.port}/docs")
    logger.info(f"ReDoc documentation available at: http://{args.host}:{args.port}/redoc")
    
    uvicorn.run(
        "api.src.app:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
        log_level=args.log_level
    )

if __name__ == "__main__":
    main()
