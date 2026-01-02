import os
import sqlalchemy
from sqlalchemy import create_engine

def get_db_engine():
    """Create and return a SQLAlchemy engine for the Postgres database."""
    user = os.getenv("POSTGRES_USER", "admin")
    password = os.getenv("POSTGRES_PASSWORD", "admin")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "piro_db")
    
    url = f"postgresql://{user}:{password}@{host}:{port}/{db}"
    return create_engine(url)
