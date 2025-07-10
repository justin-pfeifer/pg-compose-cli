"""
SQL sorting endpoint.
"""

from fastapi import APIRouter, HTTPException, Body
from fastapi.responses import PlainTextResponse
from typing import Optional, List, Dict, Any
from pg_compose_core.lib.extract import extract_build_queries
from pg_compose_core.lib.sorter import sort_queries
from pg_compose_core.lib.ast_objects import ASTList

router = APIRouter()

@router.post("/sort", responses={
    200: {
        "description": "Sorted SQL statements",
        "content": {
            "text/plain": {
                "example": "CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT);\nCREATE INDEX idx_users_name ON users(name);\nGRANT SELECT ON users TO readonly;"
            },
            "application/json": {
                "example": [
                    {
                        "type": "CREATE_TABLE",
                        "command": "CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT);",
                        "table": "users"
                    },
                    {
                        "type": "CREATE_INDEX",
                        "command": "CREATE INDEX idx_users_name ON users(name);",
                        "index": "idx_users_name",
                        "table": "users"
                    },
                    {
                        "type": "GRANT",
                        "command": "GRANT SELECT ON users TO readonly;",
                        "table": "users",
                        "privilege": "SELECT"
                    }
                ]
            }
        }
    },
    400: {
        "description": "Invalid input",
        "content": {
            "application/json": {
                "example": {
                    "detail": "Must provide either sql or ast parameter"
                }
            }
        }
    }
})
async def sort_sql(
    sql: Optional[str] = Body(
        None, 
        description="SQL content: raw SQL, file path, or git URL",
        example="CREATE INDEX idx_users_name ON users(name); CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT);"
    ),
    ast: Optional[List[Dict[str, Any]]] = Body(
        None, 
        description="List of ASTObject dictionaries",
        example=[
            {
                "type": "CREATE_TABLE",
                "command": "CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT);",
                "table": "users"
            },
            {
                "type": "CREATE_INDEX",
                "command": "CREATE INDEX idx_users_name ON users(name);",
                "index": "idx_users_name",
                "table": "users"
            }
        ]
    )
):
    """
    Sort SQL statements by dependencies.
    
    Takes unsorted SQL and reorders it so dependencies are created in the right order.
    Example: Tables before indexes, functions before triggers, grants after objects.
    """
    try:
        if sql:
            # Extract objects from SQL
            objects = extract_build_queries(sql, use_ast_objects=True)
        elif ast:
            # Convert dictionary list to ASTList
            objects = ASTList.from_dict_list(ast)
        else:
            raise HTTPException(status_code=400, detail="Must provide either sql or ast parameter")
        
        # Sort the objects with default settings
        sorted_objects = sort_queries(objects, use_object_names=True, grant_handling="after")
        
        # Return sorted SQL
        return PlainTextResponse(sorted_objects.to_sql())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 