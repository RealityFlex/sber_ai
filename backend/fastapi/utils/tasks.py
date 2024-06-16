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

# tb.init()
DATABASE_URL = "postgresql://lct_guest:postgres@postgres:5432/lct_postgres_db"

engine = create_engine(DATABASE_URL)
Base.metadata.create_all(engine)
celery = Celery('tasks', broker='redis://62.109.8.64:6377', backend='redis://62.109.8.64:6377')
celery.conf.broker_connection_retry_on_startup = True


async def get_res(id):
    return AsyncResult(id, app=celery)

@celery.task(bind=True)
def celery_use_filter(self, id_distr_returnable, user_name, bills_link):
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    _delete_data(SessionLocal, user_name)
    Base.metadata.create_all(engine)
    res = distribute_bills(SessionLocal, user_name, bills_link, self.request.id)
    return {"id":id_distr_returnable, "user_name":user_name, "result":res}

@celery.task(bind=True)
def celery_use_another_filter(self, id_distr_returnable,user_name, bills_link):
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    _delete_data(SessionLocal, user_name)
    Base.metadata.create_all(engine)
    res = distribute_predicted_bills(SessionLocal, user_name, bills_link, self.request.id)
    return {"id":id_distr_returnable, "user_name":user_name, "result":res}
