from sqlalchemy import Column, Integer, String, Float
from repository.db import Base

class FixedAssets(Base):
    '''Таблица с основными средствами
    
    Поля класса:
        fixed_assets_relationship_id - ID таблицы,\n
        fixed_asset_id - ID основного средства,\n
        fixed_asset_class - Класс основного средства,\n
        fixed_asset_used - Признак "Используется в основной деятельности",\n
        fixed_asset_usage - Признак "Способ использования",\n
        fixed_asset_square - Площадь,\n
        square_unit - ЕИ площади,\n
        building_id - ID здания,\n
        action_from - Дата начала действия связи с зданием,\n
        action_to - Дата окончания действия связи с зданием,\n
        input_date - Дата ввода в эксплуатацию,\n
        output_date - Дата выбытия,\n
        user - Пользователь\n
    '''
    #
    __tablename__ = "fixed_assets"
    fixed_assets_relationship_id = Column(Integer, primary_key=True, autoincrement=True) # ID таблицы
    fixed_asset_id = Column(String, primary_key=True, index=True) # ID основного средства
    fixed_asset_class = Column(String) # Класс основного средства
    fixed_asset_used = Column(String) # Признак "Используется в основной деятельности"
    fixed_asset_usage = Column(String) # Признак "Способ использования"
    fixed_asset_square = Column(Float) # Площадь
    square_unit = Column(String) # ЕИ площади
    building_id = Column(String) # ID здания
    action_from = Column(String, nullable=True) # Дата начала действия связи с зданием
    action_to = Column(String, nullable=True) # Дата окончания действия связи с зданием
    input_date = Column(String, nullable=True) # Дата ввода в эксплуатацию
    output_date = Column(String, nullable=True) # Дата выбытия
    user = Column(String, index=True) # Пользователь
