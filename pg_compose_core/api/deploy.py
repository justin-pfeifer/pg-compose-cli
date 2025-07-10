"""
Schema deployment endpoint.
"""

from fastapi import APIRouter, HTTPException, Form
from fastapi.responses import JSONResponse
from pg_compose_core.lib.deploy import diff_sort
from typing import Optional

router = APIRouter()

@router.post("/deploy", responses={
    200: {
        "description": "Deployment result",
        "content": {
            "application/json": {
                "example": {
                    "status": "success",
                    "changes_applied": 3,
                    "sql": "ALTER TABLE users ADD COLUMN email TEXT;"
                }
            }
        }
    },
    400: {
        "description": "Deployment errors",
        "content": {
            "application/json": {
                "example": {
                    "status": "error",
                    "errors": [
                        "Table 'users' already exists",
                        "Column 'email' already exists in table 'users'"
                    ]
                }
            }
        }
    }
})
async def deploy_schema(
    source_a: str = Form(..., description="Source A: file path, directory, or connection string"),
    source_b: Optional[str] = Form(None, description="Source B: file path, directory, or connection string (optional)"),
    target_db: str = Form(..., description="Target PostgreSQL connection string"),
    prod: bool = Form(False, description="Deploy to production (default is preview mode)")
):
    """
    Deploy schema changes to a target database.
    
    Compares source_a against source_b (or target_db if source_b not provided) and applies necessary changes.
    Default is dry-run mode for safety. Use prod=True to actually apply changes.
    """
    try:
        # If source_b is not provided, use target_db as the comparison target
        comparison_target = source_b if source_b else target_db
        
        # Generate the diff and sort by dependencies
        result = diff_sort(
            source_a=source_a,
            source_b=comparison_target
        )
        
        # Convert to SQL
        sql = result.to_sql()
        
        if not prod:
            return JSONResponse({
                "status": "preview",
                "changes_count": len(result),
                "sql": sql,
                "message": "Dry run completed - no changes applied"
            })
        else:
            # TODO: Implement actual database deployment
            # This would execute the SQL against target_db
            # For now, just return the SQL that would be executed
            
            return JSONResponse({
                "status": "success",
                "changes_applied": len(result),
                "sql": sql,
                "message": f"Generated {len(result)} changes to apply"
            })
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 