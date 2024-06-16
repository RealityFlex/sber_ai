from fastapi import FastAPI, HTTPException
import logging
from utils.distributed_bills import distribute_bills
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from utils.upload_data import _delete_data
from repository.db import Base

app = FastAPI()

DATABASE_URL = "postgresql://lct_guest:postgres@localhost/lct_postgres_db"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base.metadata.create_all(engine)


# @app.get("/service_code")
# def get_service_code():
#     with SessionLocal() as session:
#         service_code = session.query(ServiceCode).all()
#         if service_code is None:
#             raise HTTPException(status_code=404, detail="Service code not found")
#         return service_code
    
# @app.get("/contracts")
# def get_contracts():
#     with SessionLocal() as session:
#         service_code = session.query(Contract).all()
#         if service_code is None:
#             raise HTTPException(status_code=404, detail="Service code not found")
#         return service_code
    
@app.get("/distributed_bills")
def get_distributed_bills(id_distr_returnable: int, user_name: str, bills_link: str):
    return distribute_bills(SessionLocal, user_name, bills_link)

@app.get("/delete")
def delete_data():
    return _delete_data(SessionLocal, "qwe")


