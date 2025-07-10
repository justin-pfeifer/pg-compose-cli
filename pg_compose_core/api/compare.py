"""
Schema comparison endpoints.
"""

from fastapi import APIRouter, HTTPException, Form
from fastapi.responses import JSONResponse, PlainTextResponse
from pg_compose_core.lib.compare import compare_sources
from pg_compose_core.lib.ast_objects import ASTList

router = APIRouter()

@router.post("/compare", responses={
    200: {
        "description": "Schema comparison result",
        "content": {
            "text/plain": {
                "example": """ALTER TABLE users ADD COLUMN email TEXT;
CREATE INDEX idx_users_email ON users(email);"""
            },
            "application/json": {
                "example": [
                    {
                        "type": "ALTER_TABLE",
                        "command": "ALTER TABLE users ADD COLUMN email TEXT;",
                        "table": "users",
                        "operation": "ADD_COLUMN"
                    },
                    {
                        "type": "CREATE_INDEX", 
                        "command": "CREATE INDEX idx_users_email ON users(email);",
                        "index": "idx_users_email",
                        "table": "users"
                    }
                ]
            }
        }
    }
})
async def compare(
    source_a: str = Form(..., description="First source: file upload, git URL, or PostgreSQL connection string"),
    source_b: str = Form(..., description="Second source: file upload, git URL, or PostgreSQL connection string"),
    output_format: str = Form("sql", description="Output format: sql or json")
):
    """
    Compare two schema sources and return differences.
    
    Sources can be:
    - File uploads (.sql files)
    - Git URLs (git@github.com:user/repo.git, https://github.com/user/repo.git)
    - PostgreSQL connection strings (postgres://user:pass@host:port/db)
    - Raw SQL strings
    """
    try:
        # Validate output format
        if output_format not in ["sql", "json"]:
            raise HTTPException(status_code=400, detail="output_format must be 'sql' or 'json'")
        
        # Perform the comparison
        result = compare_sources(
            source_a,
            source_b,
            schemas=None,
            grants=True,
            use_ast_objects=True,
            verbose=False
        )
        
        # Return in requested format
        if output_format == "sql":
            if isinstance(result, ASTList):
                return PlainTextResponse(result.to_sql())
            else:
                # fallback: join dict-based commands
                return PlainTextResponse("\n".join(cmd["command"] for cmd in result))
        else:  # json
            if isinstance(result, ASTList):
                return JSONResponse(result.to_dict_list())
            else:
                return JSONResponse(result)
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 