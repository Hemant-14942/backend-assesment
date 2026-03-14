from fastapi import FastAPI
from .database import engine, Base
from .routes import etl_routes, asset_routes

# This command creates the tables in PostgreSQL if they don't exist
Base.metadata.create_all(bind=engine)

app = FastAPI(title="Crypto ETL Service")

# Include the routes we created
app.include_router(etl_routes.router)
app.include_router(asset_routes.router)

@app.get("/")
def read_root():
    return {"message": "Crypto ETL API is running"}