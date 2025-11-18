from contextlib import asynccontextmanager

from fastapi import FastAPI

from .routers import upload, status


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield


app = FastAPI(
    title="Data to S3 Upload Service",
    description="API para ler arquivos da pasta data e fazer upload para S3",
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(upload.router, prefix="/api/v1", tags=["upload"])
app.include_router(status.router, prefix="/api/v1", tags=["status"])

@app.get("/")
async def root():
    return {
        "message": "Data to S3 Upload Service",
        "version": "1.0.0",
        "endpoints": {
            "upload_all": "/api/v1/upload/all",
            "upload_year": "/api/v1/upload/year/{year}",
            "status": "/api/v1/status",
        },
    }


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy"}
