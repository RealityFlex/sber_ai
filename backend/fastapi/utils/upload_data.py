# import pandas as pd
# from models.service_code import ServiceCode
# from models.contract import Contract
# from models.fixed_assets import FixedAssets
# from models.contract_building_relationship import ContractBuildingRelationship
# from models.square import Square

# # Функция для чтения данных из Excel и записи в PostgreSQL
# def _load_service_classes(SessionLocal):
#     # Чтение данных из Excel
#     df = pd.read_excel("../../data/hardcoded/Коды услуг.xlsx")
#     df = df.drop_duplicates(subset=['ID услуги'], keep='first')
        
#     # Запись данных в таблицу
#     with SessionLocal() as session:
#         for _, row in df.iterrows():
#             service_code = ServiceCode(
#                 id=row['ID услуги'], 
#                 service_class=row['Класс услуги']
#                 )
#             session.add(service_code)
#         session.commit()

# def _load_contracts(SessionLocal):
#     df = pd.read_excel("../../data/hardcoded/Договоры.xlsx")   
#     df = df.drop_duplicates(subset=['ID договора'], keep='first')
   
#     with SessionLocal() as session:
#         for _, row in df.iterrows():
#             contract = Contract(
#                 id_contract=row['ID договора'], 
#                 date_start=row['Дата начала действия договора'], 
#                 date_end=row['Дата окончания действия договора']
#                 )
#             session.add(contract)
#         session.commit()

# def _load_contract_building_relationship(SessionLocal):
#     df = pd.read_excel("../../data/hardcoded/Связь договор - здания.xlsx")
#     df = df.drop_duplicates(subset=['ID договора', 'ID здания'], keep='first')
    
#     with SessionLocal() as session:
#         for _, row in df.iterrows():
#             contract_building_relationship = ContractBuildingRelationship(
#                 id = _,
#                 id_contract=row['ID договора'], 
#                 id_building=row['ID здания'], 
#                 relationship_start=row['Отношение действ. с'], 
#                 relationship_end=row['Отношение действ. до']
#                 )
#             session.add(contract_building_relationship)
#         session.commit()

# def _load_fixed_assets(SessionLocal):
#     df = pd.read_excel("../../data/hardcoded/Основные средства.xlsx")
#     df = df.drop_duplicates(subset=['ID основного средства'], keep='first')
#     df['Признак "Используется в основной деятельности"'] = df['Признак "Используется в основной деятельности"'].fillna(False)
#     df['Признак "Используется в основной деятельности"'] = df['Признак "Используется в основной деятельности"'].replace("X", True)

#     df['Признак "Способ использования"'] = df['Признак "Способ использования"'].fillna(False)
#     df['Признак "Способ использования"'] = df['Признак "Способ использования"'].replace("X", True)

#     df['Дата начала действия связи с зданием'] = df['Дата начала действия связи с зданием'].fillna("01.01.1900")
#     df['Дата окончания действия связи с зданием'] = df['Дата окончания действия связи с зданием'].fillna("12.12.3000")
#     df['Дата ввода в эксплуатацию'] = df['Дата ввода в эксплуатацию'].fillna("01.01.1900")
#     df['Дата выбытия'] = df['Дата выбытия'].fillna("12.12.3000")
#     d = {'62001M01' : 62001801, '62001M04':62001804}
#     df = df.replace({'Класс основного средства': d})

#     with SessionLocal() as session:
#         for _, row in df.iterrows():
#             fixed_asset = FixedAssets(
#                 id = _,
#                 id_fixed_asset=row['ID основного средства'], 
#                 class_fixed_asset=row['Класс основного средства'],
#                 sign_using_in_main_activity=row['Признак "Используется в основной деятельности"'],
#                 sign_way_of_using=row['Признак "Способ использования"'],
#                 id_building=row['ID здания'],
#                 square_of_main_assets=row['Площадь'],
#                 date_of_begin_contract=row['Дата начала действия связи с зданием'],
#                 date_of_end_contract=row['Дата окончания действия связи с зданием'],
#                 date_of_commissioning=row['Дата ввода в эксплуатацию'],
#                 date_of_disposal=row['Дата выбытия']
#                 )
#             session.add(fixed_asset)
#         session.commit()

# def _load_square(SessionLocal):
#     df = pd.read_excel("../../data/hardcoded/Площади зданий.xlsx")
#     df["Начало владения"] = df["Начало владения"].fillna("01.01.1900")
#     df["Конец владения"] = df["Конец владения"].fillna("31.12.9999")
#     df["Измерение действ. с"] = df["Измерение действ. с"].fillna("01.01.1900")
#     df["Измер. действит. по"] = df["Измер. действит. по"].fillna("31.12.9999")
#     with SessionLocal() as session:
#         for _, row in df.iterrows():
#             square = Square(
#                 id = _,
#                 id_building=row['Здание'], 
#                 square=row['Площадь'],
#                 own_start = row['Начало владения'],
#                 own_end = row["Конец владения"],
#                 measurement_start = row["Измерение действ. с"],
#                 measurement_end = row["Измер. действит. по"],
#                 unit_of_measurement = row["Единица измерения"]         			
#                 )
#             session.add(square)
#         session.commit()
    