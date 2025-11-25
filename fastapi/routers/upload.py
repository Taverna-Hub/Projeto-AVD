"""
Upload router for handling file uploads to S3.
"""
import asyncio
import time
from typing import List, Dict
from fastapi import APIRouter, HTTPException, BackgroundTasks, Depends
from pydantic import BaseModel

from ..config import settings
from ..services.file_reader import FileReader
from ..services.s3_service import S3Service
from ..services.mlflow_service import mlflow_service

router = APIRouter()


class UploadResponse(BaseModel):
    """Response model for upload operations."""
    total_files: int
    successful: int
    failed: int
    results: List[Dict[str, str]]


class UploadStatus(BaseModel):
    """Status model for upload operations."""
    status: str
    message: str
    total_files: int
    processed: int
    successful: int
    failed: int


# Global state for tracking upload progress
upload_status: Dict[str, UploadStatus] = {}


def get_file_reader() -> FileReader:
    """Dependency to get FileReader instance."""
    return FileReader(str(settings.get_data_directory()))


def get_s3_service() -> S3Service:
    """Dependency to get S3Service instance."""
    return S3Service(
        bucket_name=settings.S3_BUCKET_NAME,
        aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
        aws_region=settings.AWS_REGION,
        s3_prefix=settings.S3_PREFIX,
    )


async def upload_files_task(
    task_id: str,
    files: List[Dict[str, str]],
    s3_service: S3Service,
):
    """
    Background task to upload files to S3.
    
    Args:
        task_id: Unique task identifier
        files: List of file dictionaries to upload
        s3_service: S3Service instance
    """
    start_time = time.time()
    total_files = len(files)
    successful = 0
    failed = 0
    total_size = 0
    
    upload_status[task_id] = UploadStatus(
        status="processing",
        message="Upload in progress",
        total_files=total_files,
        processed=0,
        successful=0,
        failed=0,
    )
    
    for idx, file_info in enumerate(files):
        try:
            result = s3_service.upload_file_with_structure(
                local_file_path=file_info["path"],
                relative_path=file_info["relative_path"],
            )
            
            if result["success"]:
                successful += 1
                # Adicionar tamanho do arquivo se disponível
                if "size" in file_info:
                    total_size += file_info["size"]
            else:
                failed += 1
            
            # Update status
            upload_status[task_id].processed = idx + 1
            upload_status[task_id].successful = successful
            upload_status[task_id].failed = failed
            
        except Exception as e:
            failed += 1
            upload_status[task_id].failed = failed
            upload_status[task_id].processed = idx + 1
    
    # Mark as completed
    upload_status[task_id].status = "completed"
    upload_status[task_id].message = f"Upload completed: {successful} successful, {failed} failed"
    
    # Log no MLflow
    duration = time.time() - start_time
    mlflow_service.log_upload_operation(
        operation_type="upload_background",
        files_count=total_files,
        success_count=successful,
        failed_count=failed,
        total_size_mb=total_size / (1024 * 1024),
        duration_seconds=duration,
        additional_params={"task_id": task_id}
    )


@router.post("/upload/all", response_model=Dict[str, str])
async def upload_all_files(
    background_tasks: BackgroundTasks,
    file_reader: FileReader = Depends(get_file_reader),
    s3_service: S3Service = Depends(get_s3_service),
):
    """
    Upload all files from the data directory to S3.
    
    This endpoint starts a background task to upload all files.
    Returns a task ID that can be used to check the upload status.
    """
    import uuid
    
    # Check if bucket exists
    if not s3_service.check_bucket_exists():
        raise HTTPException(
            status_code=400,
            detail=f"S3 bucket '{settings.S3_BUCKET_NAME}' does not exist or is not accessible",
        )
    
    # Get all files
    files = file_reader.get_all_files()
    
    if not files:
        raise HTTPException(
            status_code=404,
            detail="No files found in data directory",
        )
    
    # Generate task ID
    task_id = str(uuid.uuid4())
    
    # Start background task
    background_tasks.add_task(
        upload_files_task,
        task_id=task_id,
        files=files,
        s3_service=s3_service,
    )
    
    return {
        "task_id": task_id,
        "message": f"Upload started for {len(files)} files",
        "status_endpoint": f"/api/v1/status/{task_id}",
    }


@router.post("/upload/year/{year}", response_model=Dict[str, str])
async def upload_files_by_year(
    year: str,
    background_tasks: BackgroundTasks,
    file_reader: FileReader = Depends(get_file_reader),
    s3_service: S3Service = Depends(get_s3_service),
):
    """
    Upload all files from a specific year to S3.
    
    Args:
        year: Year to upload files from (e.g., "2024")
    
    Returns:
        Task ID and status information
    """
    import uuid
    
    # Check if bucket exists
    if not s3_service.check_bucket_exists():
        raise HTTPException(
            status_code=400,
            detail=f"S3 bucket '{settings.S3_BUCKET_NAME}' does not exist or is not accessible",
        )
    
    # Get files for the year
    files = file_reader.get_files_by_year(year)
    
    if not files:
        raise HTTPException(
            status_code=404,
            detail=f"No files found for year {year}",
        )
    
    # Generate task ID
    task_id = str(uuid.uuid4())
    
    # Start background task
    background_tasks.add_task(
        upload_files_task,
        task_id=task_id,
        files=files,
        s3_service=s3_service,
    )
    
    return {
        "task_id": task_id,
        "message": f"Upload started for {len(files)} files from year {year}",
        "status_endpoint": f"/api/v1/status/{task_id}",
    }


@router.post("/upload/sync/all", response_model=UploadResponse)
async def upload_all_files_sync(
    file_reader: FileReader = Depends(get_file_reader),
    s3_service: S3Service = Depends(get_s3_service),
):
    """
    Upload all files synchronously (waits for completion).
    
    Use this endpoint if you want to wait for the upload to complete.
    For large numbers of files, prefer the async endpoint.
    """
    start_time = time.time()
    
    # Check if bucket exists
    if not s3_service.check_bucket_exists():
        raise HTTPException(
            status_code=400,
            detail=f"S3 bucket '{settings.S3_BUCKET_NAME}' does not exist or is not accessible",
        )
    
    # Get all files
    files = file_reader.get_all_files()
    
    if not files:
        raise HTTPException(
            status_code=404,
            detail="No files found in data directory",
        )
    
    # Upload files
    results = []
    successful = 0
    failed = 0
    total_size = 0
    
    for file_info in files:
        result = s3_service.upload_file_with_structure(
            local_file_path=file_info["path"],
            relative_path=file_info["relative_path"],
        )
        results.append({
            "file": file_info["filename"],
            "relative_path": file_info["relative_path"],
            **result,
        })
        
        if result["success"]:
            successful += 1
            if "size" in file_info:
                total_size += file_info["size"]
        else:
            failed += 1
    
    # Log no MLflow
    duration = time.time() - start_time
    mlflow_service.log_upload_operation(
        operation_type="upload_sync_all",
        files_count=len(files),
        success_count=successful,
        failed_count=failed,
        total_size_mb=total_size / (1024 * 1024),
        duration_seconds=duration
    )
    
    return UploadResponse(
        total_files=len(files),
        successful=successful,
        failed=failed,
        results=results,
    )

