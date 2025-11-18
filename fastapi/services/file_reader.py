"""
Service for reading files from the data directory.
"""
import os
from pathlib import Path
from typing import List, Dict, Optional


class FileReader:
    """Service to read files from the data directory structure."""
    
    def __init__(self, data_directory: str):
        """
        Initialize FileReader.
        
        Args:
            data_directory: Path to the data directory
        """
        self.data_directory = Path(data_directory)
        if not self.data_directory.exists():
            raise ValueError(f"Data directory does not exist: {data_directory}")
    
    def get_all_files(self) -> List[Dict[str, str]]:
        """
        Get all files recursively from the data directory.
        
        Returns:
            List of dictionaries with file information:
            {
                "path": str,  # Full file path
                "relative_path": str,  # Path relative to data directory
                "year": str,  # Year extracted from directory structure
                "filename": str  # Just the filename
            }
        """
        files = []
        
        for root, dirs, filenames in os.walk(self.data_directory):
            root_path = Path(root)
            
            # Extract year from directory structure (e.g., data/2024/...)
            relative_root = root_path.relative_to(self.data_directory)
            year = relative_root.parts[0] if relative_root.parts else None
            
            for filename in filenames:
                file_path = root_path / filename
                
                # Skip hidden files and directories
                if filename.startswith("."):
                    continue
                
                relative_path = file_path.relative_to(self.data_directory)
                
                files.append({
                    "path": str(file_path),
                    "relative_path": str(relative_path),
                    "year": year,
                    "filename": filename,
                    "size": file_path.stat().st_size,
                })
        
        return files
    
    def get_files_by_year(self, year: str) -> List[Dict[str, str]]:
        """
        Get all files for a specific year.
        
        Args:
            year: Year to filter files (e.g., "2024")
            
        Returns:
            List of dictionaries with file information
        """
        all_files = self.get_all_files()
        return [f for f in all_files if f.get("year") == year]
    
    def get_years(self) -> List[str]:
        """
        Get list of available years in the data directory.
        
        Returns:
            List of year strings
        """
        years = []
        if not self.data_directory.exists():
            return years
        
        for item in self.data_directory.iterdir():
            if item.is_dir() and item.name.isdigit() and len(item.name) == 4:
                years.append(item.name)
        
        return sorted(years)

