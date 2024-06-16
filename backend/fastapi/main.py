from fastapi import FastAPI, HTTPException
import logging
from utils.distributed_bills import distribute_bills
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from utils.upload_data import _delete_data
from repository.db import Base
from utils.tasks import celery_use_filter, get_res
app = FastAPI()

# DATABASE_URL = "postgresql://lct_guest:postgres@62.109.8.64:9559/lct_postgres_db"

# engine = create_engine(DATABASE_URL)
# SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
# Base.metadata.create_all(engine)


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

progress = {}

def get_progress(task_id):
    if task_id in progress:
        return progress[task_id]
    else:
        return "unknow"
    
@app.get("/distributed_bills")
def get_distributed_bills(id_distr_returnable: str, user_name: str, bills_link: str):
    task = celery_use_filter.delay(id_distr_returnable, user_name, bills_link)
    return {
        "task_id": task.id
    }

# @app.get("/delete")
# def delete_data():
#     return _delete_data(SessionLocal, "qwe")

@app.get("/result")
def result(task_id, curent:int, max:int):
    progress[task_id] = round(curent/max*100,2)

@app.get('/task_status/{task_id}')
async def task_status(task_id: str):
    task = await get_res(task_id)
    if task.state == 'PENDING':
        response = {
            "status": "PENDING",
            "result": get_progress(task_id=task_id)
        }
    elif task.state != 'FAILURE':
        response = {
                "status": task.state,
                "result": task.result
            }
    else:
        # Что-то пошло не так, задача не выполнена
        response = {
            "status": "FAILURE",
            "result": str(task.info),  # task.info содержит исключение, если задача не выполнена
        }
    return response

