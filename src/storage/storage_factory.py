"""
Storage factory for creating appropriate storage instances based on configuration.
"""

from storage.storage_interface import StorageInterface
from storage.sqlite_storage import SQLiteStorage
from storage.postgresql_storage import PostgreSQLStorage
from config import (
    STORAGE_TYPE,
    DEFAULT_DB_PATH,
    DEFAULT_PG_HOST,
    DEFAULT_PG_PORT,
    DEFAULT_PG_DB,
    DEFAULT_PG_USER,
    DEFAULT_PG_PASSWORD,
)


def create_storage() -> StorageInterface:
    """
    Create and return the appropriate storage instance based on configuration.
    
    Returns:
        StorageInterface: The configured storage instance
        
    Raises:
        ValueError: If the storage type is not supported
    """
    if STORAGE_TYPE.lower() == "sqlite":
        return SQLiteStorage(db_path=DEFAULT_DB_PATH)
    elif STORAGE_TYPE.lower() == "postgresql":
        return PostgreSQLStorage(
            host=DEFAULT_PG_HOST,
            port=DEFAULT_PG_PORT,
            database=DEFAULT_PG_DB,
            user=DEFAULT_PG_USER,
            password=DEFAULT_PG_PASSWORD,
        )
    else:
        raise ValueError(f"Unsupported storage type: {STORAGE_TYPE}")