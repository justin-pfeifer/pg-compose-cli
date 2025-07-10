"""
Health and status endpoints.
"""

from fastapi import APIRouter
from pg_compose_core.api.models import HealthResponse
from datetime import datetime

router = APIRouter()

@router.get("/health", response_model=HealthResponse)
def health():
    """Get API health status"""
    return HealthResponse(
        status="healthy",
        timestamp=datetime.now().isoformat(),
        version="0.2.0",
        uptime="running"
    )