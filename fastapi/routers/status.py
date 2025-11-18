"""
Status router for checking upload status and file information.
"""
from typing import List, Dict
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel

from ..config import settings
from ..services.file_reader import FileReader
from ..routers.upload import upload_status

router = APIRouter()


class FileInfo(BaseModel):
    """File information model."""
    filename: str
    relative_path: str
    year: str
    size: int


class DirectoryInfo(BaseModel):
    """Directory information model."""
    total_files: int
    total_size: int
    years: List[str]
    files_by_year: Dict[str, int]


def get_file_reader() -> FileReader:
    """Dependency to get FileReader instance."""
    return FileReader(str(settings.get_data_directory()))


@router.get("/status/{task_id}")
async def get_upload_status(task_id: str):
    """
    Get the status of an upload task.
    
    Args:
        task_id: Task ID returned from upload endpoint
    
    Returns:
        Upload status information
    """
    if task_id not in upload_status:
        raise HTTPException(
            status_code=404,
            detail=f"Task ID '{task_id}' not found",
        )
    
    return upload_status[task_id]


@router.get("/files", response_model=List[FileInfo])
async def list_all_files(
    file_reader: FileReader = Depends(get_file_reader),
):
    """
    List all files in the data directory.
    
    Returns:
        List of file information
    """
    files = file_reader.get_all_files()
    return [FileInfo(**f) for f in files]


@router.get("/files/year/{year}", response_model=List[FileInfo])
async def list_files_by_year(
    year: str,
    file_reader: FileReader = Depends(get_file_reader),
):
    """
    List all files for a specific year.
    
    Args:
        year: Year to filter files (e.g., "2024")
    
    Returns:
        List of file information for the specified year
    """
    files = file_reader.get_files_by_year(year)
    
    if not files:
        raise HTTPException(
            status_code=404,
            detail=f"No files found for year {year}",
        )
    
    return [FileInfo(**f) for f in files]


@router.get("/info", response_model=DirectoryInfo)
async def get_directory_info(
    file_reader: FileReader = Depends(get_file_reader),
):
    """
    Get information about the data directory.
    
    Returns:
        Directory information including total files, size, and years
    """
    files = file_reader.get_all_files()
    years = file_reader.get_years()
    
    total_size = sum(f["size"] for f in files)
    
    files_by_year = {}
    for file_info in files:
        year = file_info.get("year", "unknown")
        files_by_year[year] = files_by_year.get(year, 0) + 1
    
    return DirectoryInfo(
        total_files=len(files),
        total_size=total_size,
        years=years,
        files_by_year=files_by_year,
    )

