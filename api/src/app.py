"""
FastAPI application for Orthanc real estate analytics API.
"""
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import logging

from api.src.flat_analysis import router as flat_analysis_router
from api.src.jk_analysis_sales import router as jk_sales_router
from api.src.jk_analysis_rentals import router as jk_rentals_router
from api.src.residential_complex_management import router as residential_complex_management
from api.src.flat_management import router as flat_management_router
from api.src.database_stats import router as database_stats_router

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(
    title="Orthanc Real Estate Analytics API",
    description="API for analyzing real estate data including flats, sales, and rentals",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure this properly for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(flat_analysis_router, prefix="/api/flats", tags=["flat-analysis"])
app.include_router(jk_sales_router, prefix="/api/jks/sales", tags=["jk-sales-analysis"])
app.include_router(jk_rentals_router, prefix="/api/jks/rentals", tags=["jk-rentals-analysis"])
app.include_router(residential_complex_management, prefix="/api/complexes", tags=["complex-management"])
app.include_router(flat_management_router, prefix="/api/flats", tags=["flat-management"])
app.include_router(database_stats_router, prefix="/api/database", tags=["database-stats"])

@app.get("/")
async def root():
    """Root endpoint with API information."""
    return {
        "message": "Orthanc Real Estate Analytics API",
        "version": "1.0.0",
        "docs": "/docs",
        "redoc": "/redoc"
    }

@app.get("/api/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "message": "API is running"
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
