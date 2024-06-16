from sqlalchemy import Column, Integer, String  
from repository.db import Base

class ServiceCodes(Base):
    '''Таблица с кодами услуг
    
    Поля класса:
        service_id - ID услуги,\n
        service_class - Класс услуги,\n
        user - Пользователь\n
    '''
    __tablename__ = "service_codes"
    service_id = Column(Integer, primary_key=True, index=True) # ID услуги
    service_class = Column(String) # Класс услуги
    user = Column(String, index=True) # Пользователь