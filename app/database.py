import os
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

# Load environment variables from .env file [cite: 207]
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

# Create the SQLAlchemy engine [cite: 210]
engine = create_engine(DATABASE_URL)

# Create a SessionLocal class for database individual sessions 
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base class for our models to inherit from [cite: 194]
Base = declarative_base()

# Dependency to get a DB session for FastAPI routes [cite: 154]
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()