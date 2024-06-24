import time
from celery import Celery
from celery.result import AsyncResult
from utils.distributed_bills import distribute_bills, distribute_predicted_bills
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from utils.upload_data import _delete_data
from repository.db import Base
import json
from sqlalchemy.orm import Session
import redis
# tb.init()
DATABASE_URL = "postgresql://lct_guest:postgres@62.109.8.64:5433/lct_postgres_db"
r = redis.Redis(host='62.109.8.64', port=6377, db=0)
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base.metadata.create_all(engine)
# celery = Celery('tasks', broker='redis://62.109.8.64:6377', backend='redis://62.109.8.64:6377')
# celery.conf.broker_connection_retry_on_startup = True


# async def get_res(id):
#     return AsyncResult(id, app=celery)

# @celery.task(bind=True)
def celery_use_filter(id_distr_returnable, user_name, bills_link, id):
    try:
        _delete_data(SessionLocal, user_name)
        Base.metadata.create_all(engine)
        res = distribute_bills(SessionLocal, user_name, bills_link, id)
        print(res)
        return {"id":id_distr_returnable, "user_name":user_name, "result":res}
    except Exception as e:
        print("ERROR", e)
        r.set(task_id, json.dumps({"status":"FAILURE", "result": e}))
        _delete_data(SessionLocal, user_name)

# @celery.task(bind=True)
# def celery_use_another_filter(self, id_distr_returnable,user_name, bills_link):
#     try:
#         _delete_data(SessionLocal, user_name)
#         Base.metadata.create_all(engine)
#         res = distribute_predicted_bills(SessionLocal, user_name, bills_link, self.request.id)
#         return {"id":id_distr_returnable, "user_name":user_name, "result":res}
#     except Exception as e:
#         print("ERROR", e)
#         _delete_data(SessionLocal, user_name)