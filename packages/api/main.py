import os
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from loguru import logger
from packages.api.routes import router
from packages.api.routers import export


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Handle application startup and shutdown events"""
    # Startup
    logger.info("Analytics Pipeline API starting up...")
    yield
    # Shutdown
    logger.info("Analytics Pipeline API shutting down gracefully...")


def create_app() -> FastAPI:
    app = FastAPI(
        title="Analytics Pipeline API",
        description="API for triggering and managing analytics pipeline runs (on-demand/benchmarking)",
        version="1.0.0",
        lifespan=lifespan
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(router, prefix="/api/v1", tags=["pipelines"])
    app.include_router(export.router, prefix="/api/v1", tags=["export"])

    @app.get("/health")
    async def health_check():
        return {"status": "healthy", "mode": os.getenv("ANALYTICS_EXECUTION_MODE", "unknown")}

    return app

app = create_app()