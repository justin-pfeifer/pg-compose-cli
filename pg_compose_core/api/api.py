"""
Main FastAPI application for pg-compose-core.
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .health import router as health_router
from .compare import router as compare_router
from .sort import router as sort_router
from .deploy import router as deploy_router
from .merge import router as merge_router
from .errors import not_found_handler, internal_error_handler

app = FastAPI(
    title="pg-compose-core API",
    description="Core library for comparing PostgreSQL schemas from SQL files or live connections",
    version="0.2.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register routers
app.include_router(health_router, tags=["system"])
app.include_router(compare_router, tags=["schema"])
app.include_router(sort_router, tags=["schema"])
app.include_router(deploy_router, tags=["operations"])
app.include_router(merge_router, tags=["schema"])

# Add error handlers
app.add_exception_handler(404, not_found_handler)
app.add_exception_handler(500, internal_error_handler)

# To run: uvicorn pg_compose_core.api:app --reload --host 0.0.0.0 --port 8000 