from fastapi import FastAPI, HTTPException
import logging
import asyncio
from utils.distributed_bills import distribute_bills
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from utils.upload_data import _delete_data
from repository.db import Base
from utils.tasks import celery_use_filter
import redis
import json
import uuid
from threading import Thread

app = FastAPI()

# DATABASE_URL = "postgresql://lct_guest:postgres@62.109.8.64:9559/lct_postgres_db"
r = redis.Redis(host='62.109.8.64', port=6377, db=0)

from pydantic import BaseModel

class Item(BaseModel):
    distributed_bills: str
    export_distributed_bills_csv: str
    donut_graph: dict
    dots_graph: dict

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
    a = r.get(task_id)
    if a == None:
        return {"status": "PENDING", "result": 0}
    return json.loads(r.get(task_id))

@app.get("/distributed_bills")
async def get_distributed_bills(id_distr_returnable: str, user_name: str, bills_link: str):
    id = str(uuid.uuid4())
    t = Thread(target=celery_use_filter, args=(id_distr_returnable, user_name, bills_link, id))
    t.start()
    return {"task_id":id}


async def run_in_thread(id_distr_returnable, user_name, bills_link):
    await celery_use_filter(id_distr_returnable, user_name, bills_link)

    
# @app.get("/distributed_bills")
# def get_distributed_bills(id_distr_returnable: str, user_name: str, bills_link: str):
#     asyncio.create_task(celery_use_filter(id_distr_returnable, user_name, bills_link))
#     return {
#         "task_id": "task.id"
#     }

# @app.get("/distributed_predict_bills")
# def get_distributed_predict_bills(id_distr_returnable: str, user_name: str, bills_link: str):
#     task = celery_use_another_filter(id_distr_returnable, user_name, bills_link)
#     return {
#         "task_id": task.id
#     }

# @app.get("/distributed_predicted_bills")
# def get_distributed_bills(id_distr_returnable: str, user_name: str, bills_link: str):
#     task = celery_use_filter.delay(id_distr_returnable, user_name, bills_link)
#     return {
#         "task_id": task.id
#     }

# @app.get("/delete")
# def delete_data():
#     return _delete_data(SessionLocal, "qwe")

@app.get("/prog")
def prog(task_id, curent:int, max:int):
    progress[task_id] = round(curent/max*100,2)

@app.post("/result")
def prog(task_id, data: Item):
    progress[task_id] = data

@app.get('/task_status/{task_id}')
async def task_status(task_id: str):
    # task = await get_res(task_id)
    return get_progress(task_id=task_id)
    # if task_id in progress:
    #     response = {
    #         "status": "PENDING",
    #         "result": get_progress(task_id=task_id)
    #     }
    # else:
    #     response = {
    #         "status": "CREATING",
    #         "result": ""
    #     }
    # return response

