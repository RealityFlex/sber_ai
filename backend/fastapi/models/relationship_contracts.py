from sqlalchemy import Column, Integer, String, DateTime
from repository.db import Base

class ContractRelationship(Base):
    '''Таблица с взаимоотношениями договоров со зданиями

    Поля класса:
        contract_relationship_id - ID отношения договора,\n
        contract_id - ID договора,\n	
        building_id - ID здания,\n	
        action_from - Отношение действ. с,\n	
        action_to - Отношение действ. до,\n
        user - Пользователь\n
    '''
    __tablename__ = "contract_relationship"
    contract_relationship_id = Column(Integer, primary_key=True, index=True, autoincrement=True) # ID отношения договора
    contract_id = Column(String, nullable=False, index=True) # ID договора
    building_id = Column(String, nullable=False) # ID здания
    action_from = Column(DateTime, nullable=True) # Отношение действ. с
    action_to = Column(DateTime, nullable=True) # Отношение действ. до
    user = Column(String, index=True) # Пользователь

