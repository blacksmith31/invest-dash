
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import logging

from config.settings import settings
from config.lifespan import lifespan
from routers.api import router as api_router
from routers.views import router as view_router


def get_app() -> FastAPI:

    app = FastAPI(lifespan=lifespan, **settings.fastapi_kwargs)
    app.mount("/static", StaticFiles(directory=settings.STATIC_DIR), name="static")
    app.include_router(api_router)
    app.include_router(view_router)

    app.add_middleware(
        CORSMiddleware,
        allow_origins=['*'],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"]
    )
    
    return app

app = get_app()

    

