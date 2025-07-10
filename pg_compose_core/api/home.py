"""
Home route for rendering README.
"""

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path
import markdown

router = APIRouter()
templates = Jinja2Templates(directory=Path(__file__).parent / "templates")

@router.get("/", response_class=HTMLResponse)
async def home(request: Request):
    """Render README at root endpoint"""
    readme_path = Path(__file__).parent.parent.parent / "README.md"
    try:
        with open(readme_path, "r", encoding="utf-8") as f:
            readme_content = f.read()
        # Convert markdown to HTML
        html_content = markdown.markdown(readme_content)
    except FileNotFoundError:
        html_content = "<p>README.md not found.</p>"
    
    return templates.TemplateResponse("home.html", {
        "request": request,
        "readme_content": html_content
    }) 