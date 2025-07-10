"""
Pydantic models for API requests and responses.
"""

from pydantic import BaseModel, Field

class SortRequest(BaseModel):
    """Request model for SQL sorting."""
    sql_content: str = Field(..., description="SQL content to sort")
    use_object_names: bool = Field(True, description="Use object names for dependency resolution")
    grant_handling: str = Field("after", description="When to handle grants: before, after, or ignore")

class HealthResponse(BaseModel):
    """Health check response."""
    status: str = Field(..., example="healthy")
    version: str = Field(..., example="0.2.0")

class ErrorResponse(BaseModel):
    """Error response model."""
    detail: str = Field(..., example="An error occurred") 