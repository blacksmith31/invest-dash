
from pathlib import Path
from typing import Any
from fastapi.responses import HTMLResponse
from pydantic_settings import BaseSettings

APP_DIR = Path(__file__).resolve().parent.parent

class Settings(BaseSettings):
    APP_DIR: Path = APP_DIR
    STATIC_DIR: Path = APP_DIR / "frontend/static"
    TEMPLATE_DIR: Path = APP_DIR / "frontend/templates"
    # DATA_DIR???

    FASTAPI_PROPERTIES: dict = {
        "title": "Dual Momentum Dashboard",
        "description": "Test Description",
        "version": "0.0.2",
        "default_response_class": HTMLResponse,
    }

    DISABLE_DOCS: bool = False

    @property
    def fastapi_kwargs(self) -> dict[str, Any]:
        fastapi_kwargs = self.FASTAPI_PROPERTIES
        if self.DISABLE_DOCS:
            fastapi_kwargs.update(
                {
                "openapi_url": None,
                "openapi_prefix": None,
                "docs_url": None,
                "redoc_url": None,
                }
            )
        return fastapi_kwargs

settings = Settings()
# print(f"settings from settings file: static dir: {settings.STATIC_DIR}")

def test():
    print(f"path file: {Path(__file__)}")
    print(f"path file resolve: {Path(__file__).resolve()}")
