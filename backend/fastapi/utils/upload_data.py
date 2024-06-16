import pandas as pd
from models.service_codes import ServiceCodes
from models.fixed_assets import FixedAssets
from models.relationship_contracts import ContractRelationship
import utils.mini as mini

# Функция для чтения данных из Excel и записи в PostgreSQL
def _load_service_classes(SessionLocal, user_name):
    # Чтение данных из Excel
    df = pd.read_excel(mini.presigned_get_object('user-tabels',f'{user_name}/hardcoded/service_codes.xlsx'))
    df = df.drop_duplicates(subset=['ID услуги'], keep='first')
        
    # Запись данных в таблицу
    with SessionLocal() as session:
        for _, row in df.iterrows():
            service_code = ServiceCodes(
                service_id = row['ID услуги'], 
                service_class = row['Класс услуги'],
                user = user_name
            )
            session.add(service_code)
        session.commit()

def _load_contract_building_relationship(SessionLocal, user_name):
    df = pd.read_excel(mini.presigned_get_object('user-tabels',f'{user_name}/hardcoded/contracts_relationship.XLSX'))
    df = df.drop_duplicates(subset=['ID договора', 'ID здания'], keep='first')
    df = df.fillna(method="ffill")
    
    with SessionLocal() as session:
        for _, row in df.iterrows():
            contract_building_relationship = ContractRelationship(
                contract_relationship_id = _,
                contract_id = row['ID договора'], 
                building_id = row['ID здания'], 
                action_from = row['Отношение действ. с'], 
                action_to = row['Отношение действ. до'],
                user = user_name
                )
            session.add(contract_building_relationship)
        session.commit()

def _load_fixed_assets(SessionLocal, user_name):
    df = pd.read_excel(mini.presigned_get_object('user-tabels',f'{user_name}/hardcoded/main_assets.xlsx'))

    df = df.drop_duplicates(subset=['ID основного средства'], keep='first')
    df['Признак "Используется в основной деятельности"'] = df['Признак "Используется в основной деятельности"'].fillna(False)
    df['Признак "Используется в основной деятельности"'] = df['Признак "Используется в основной деятельности"'].replace("X", True)

    df['Признак "Способ использования"'] = df['Признак "Способ использования"'].fillna(False)
    df['Признак "Способ использования"'] = df['Признак "Способ использования"'].replace("X", True)

    df['Дата начала действия связи с зданием'] = df['Дата начала действия связи с зданием'].fillna("01.01.1900")
    df['Дата окончания действия связи с зданием'] = df['Дата окончания действия связи с зданием'].fillna("12.12.3000")
    df['Дата ввода в эксплуатацию'] = df['Дата ввода в эксплуатацию'].fillna("01.01.1900")
    df['Дата выбытия'] = df['Дата выбытия'].fillna("12.12.3000")
    d = {'62001M01' : 62001801, '62001M04':62001804}
    df = df.replace({'Класс основного средства': d})

    with SessionLocal() as session:
        for _, row in df.iterrows():
            fixed_asset = FixedAssets(
                fixed_assets_relationship_id  = _,
                fixed_asset_id = row['ID основного средства'], 
                fixed_asset_class = row['Класс основного средства'],
                fixed_asset_used = row['Признак "Используется в основной деятельности"'],
                fixed_asset_usage = row['Признак "Способ использования"'],
                building_id = row['ID здания'],
                fixed_asset_square = row['Площадь'],
                square_unit = row['ЕИ площади'],
                action_from = row['Дата начала действия связи с зданием'],
                action_to = row['Дата окончания действия связи с зданием'],
                input_date = row['Дата ввода в эксплуатацию'],
                output_date = row['Дата выбытия'],
                user = user_name
                )
            session.add(fixed_asset)
        session.commit()
    

def _delete_data(SessionLocal, user_name):
    with SessionLocal() as session:
        session.query(ServiceCodes).filter(ServiceCodes.user == user_name).delete()
        session.query(ContractRelationship).filter(ContractRelationship.user == user_name).delete()
        session.query(FixedAssets).filter(FixedAssets.user == user_name).delete()
        session.commit()
