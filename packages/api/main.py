import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from packages.api.routes import router

def create_app() -> FastAPI:
    app = FastAPI(
        title="Analytics Pipeline API",
        description="API for triggering and managing analytics pipeline runs (on-demand/benchmarking)",
        version="1.0.0"
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(router, prefix="/api/v1", tags=["pipelines"])

    @app.get("/health")
    async def health_check():
        return {"status": "healthy", "mode": os.getenv("ANALYTICS_EXECUTION_MODE", "unknown")}

    return app

app = create_app()