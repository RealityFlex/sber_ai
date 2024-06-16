import pandas as pd
from catboost import CatBoostClassifier
import logging
from sqlalchemy.orm import sessionmaker
from models.relationship_contracts import ContractRelationship
from models.fixed_assets import FixedAssets
from models.service_codes import ServiceCodes
import progressbar
from joblib import load
import  numpy as np
from catboost import CatBoostRegressor
import json
import utils.mini as mini
from utils.upload_data import _load_service_classes, _load_contract_building_relationship, _load_fixed_assets, _delete_data
import warnings
import requests
with warnings.catch_warnings():
    warnings.simplefilter(action='ignore', category=EncodingWarning)


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")



model = CatBoostClassifier()      # parameters not required.
model.load_model('ml-models/main_bills_predict.cbm')
cool_encoder = load('ml-models/main_bill_encoder.joblib')
cluster_encoder = load('ml-models/cluster_encoder.joblib')

service_class_cluster_model = CatBoostClassifier()
service_class_cluster_model.load_model('ml-models/service_class_cluster_model.cbm')

# Функция распределяет счета
def distribute_bills(SessionLocal: sessionmaker, user_name: str, bills_link: str, task_id):
    with SessionLocal() as session:
        _load_service_classes(SessionLocal, user_name),
        _load_contract_building_relationship(SessionLocal, user_name),
        _load_fixed_assets(SessionLocal, user_name)

        bills = pd.read_excel(mini.presigned_get_object('user-tabels',f'{user_name}/bills/low.xlsx')).head(5000)
        distributed_columns = _get_distributed_columns()

        _distribute_bills_by_building(session, user_name, bills, "distributed_bills.xlsx", task_id)
        new_bills = predict_future_bills(session, bills)
        _distribute_bills_by_building(session, user_name, new_bills, "distributed_bills_predict.xlsx", task_id)
        # по каждому договору получать все здания
        # по каждому зданию получать все основные средства
        _delete_data(SessionLocal, user_name)
        
        
# Предсказывает будущие счета
def predict_future_bills(session, dataframe):
    dataframe["Дата отражения счета в учетной системе"] = dataframe["Дата отражения счета в учетной системе"].fillna(method="ffill")
    dataframe["Дата отражения счета в учетной системе"] = pd.DatetimeIndex(dataframe["Дата отражения счета в учетной системе"]).month
    unique_months = dataframe["Дата отражения счета в учетной системе"].unique()
    print(unique_months)
    _month = dataframe.groupby("Дата отражения счета в учетной системе").value_counts().max()
    bills = dataframe.query("`Дата отражения счета в учетной системе` == @_month")
    output_df = []
    current_year = bills.iloc[0]["Год"] + 1
    for i in range(1, 13):
        for _, bill in bills.iterrows():
            new_row = {}
            new_row.setdefault("Компания", bill["Компания"])
            new_row.setdefault("Год", bill["Год"] + 1)
            new_row.setdefault("Номер счета", bill["Номер счета"])
            new_row.setdefault("Позиция счета", bill["Позиция счета"])
            new_row.setdefault("ID услуги", bill["ID услуги"])
            new_row.setdefault("ID договора", bill["ID договора"])
            new_row.setdefault("Дата отражения счета в учетной системе", i)
            new_row.setdefault("Сумма площадей", sum([x.fixed_asset_square for x in session.query(FixedAssets).filter(FixedAssets.building_id in [y.building_id for y in session.query(ContractRelationship).filter(ContractRelationship.contract_id == bill["ID договора"]).all()]).all()]))
            new_row.setdefault("Кластер", service_class_cluster_model.predict(np.ravel(pd.DataFrame({"ID услуги": bill["ID услуги"], "Класс услуги": cluster_encoder.fit_transform(np.ravel(session.query(ServiceCodes).filter(ServiceCodes.service_id == bill["ID услуги"]).first().service_class).reshape(-1))})).reshape(-1)))
            output_df.append(new_row)
    print(output_df)
    model = CatBoostRegressor()
    cool_dataframe = pd.DataFrame(output_df)
    model.load_model('ml-models/predict_future_current_best_WTF.cbm')
    y_pred = model.predict(cool_dataframe)
    if y_pred.min() < 0:
        y_pred = y_pred + y_pred.min() * (-1)
    pred_dataframe = pd.DataFrame({"Стоимость без НДС": y_pred})
    cool_dataframe = pd.concat([cool_dataframe, pred_dataframe], axis=1)
    cool_dataframe["Дата отражения счета в учетной системе"] = cool_dataframe["Дата отражения счета в учетной системе"].apply(lambda x: pd.to_datetime(f'{current_year}-{x}-01 12:00:00'))    
    return cool_dataframe

# Определяет счет главной книги
def predict_main_bill(data_for_predict: dict) -> int:
    cringe_val = np.ravel(data_for_predict["Класс услуги"]).reshape(-1)
    x = pd.DataFrame({
        "Компания": [data_for_predict["Компания"]], 
        "Номер счета": [data_for_predict["Номер счета"]],              
        "ID договора": [data_for_predict["ID договора"]],             
        "Услуга": [data_for_predict["Услуга"]],                   
        "Класс услуги": cringe_val,           
        "Здание": [data_for_predict["Здание"]],                 
        "Класс ОС": [data_for_predict["Класс ОС"]],                
        "ID основного средства": [data_for_predict["ID основного средства"]]})
    output = model.predict(x)
    output = cool_encoder.inverse_transform(np.ravel(output))
    return int(output)


# Функция распределяет счета 
def _distribute_bills_by_building(session, user_name: str, bills: pd.DataFrame, file_name, task_id) -> str:
    distributed_bills = []
    max_value_l = bills.shape[0]
    with progressbar.ProgressBar(max_value=max_value_l) as bar:
        for idx_row, row in bills.iterrows():
            bar.update(idx_row)
            requests.get(f"http://62.109.8.64:8288/result?task_id={task_id}&curent={idx_row}&max={max_value_l}")
            _contract_id = row["ID договора"]
            #list_contracts = contracts_relationship.query("`ID договора` == @_contract_id")
            list_contracts = session.query(ContractRelationship).filter(ContractRelationship.contract_id == _contract_id).all()
            if len(list_contracts) > 0:
                distributed_position = 1
                for contract_relation in list_contracts:
                    current_distributed_bills = []
                    #print(contract_relation[0])
                    _current_building_id = contract_relation.building_id
                    _current_service_id = row["ID услуги"]
                    list_main_assets = session.query(FixedAssets).filter(FixedAssets.building_id == _current_building_id).all()
                    if len(list_main_assets) > 0:
                        for main_asset in list_main_assets:
                            new_distributed_bill = {}
                            new_distributed_bill.setdefault("Компания", row["Компания"])
                            new_distributed_bill.setdefault("Год счета", row["Год"])
                            new_distributed_bill.setdefault("Номер счета", row["Номер счета"])
                            new_distributed_bill.setdefault("Позиция счета", row["Позиция счета"])
                            new_distributed_bill.setdefault("Номер позиции распределения", distributed_position)
                            default_date = pd.to_datetime(row["Дата отражения счета в учетной системе"])
                            if row["Дата отражения счета в учетной системе"] is None:
                                default_date = "01.01.1900"
                            new_distributed_bill.setdefault("Дата отражения в учетной системе", default_date)
                            new_distributed_bill.setdefault("ID договора", contract_relation.contract_id)
                            new_distributed_bill.setdefault("Услуга", row["ID услуги"])
                            new_distributed_bill.setdefault("Здание", contract_relation.building_id)
                            new_distributed_bill.setdefault("Класс ОС", main_asset.fixed_asset_class)
                            new_distributed_bill.setdefault("Класс услуги", session.query(ServiceCodes).filter(ServiceCodes.service_id == _current_service_id).all()[0].service_class)
                            new_distributed_bill.setdefault("ID основного средства", main_asset.fixed_asset_id)
                            new_distributed_bill.setdefault('Признак "Использование в основной деятельности"', main_asset.fixed_asset_used)
                            new_distributed_bill.setdefault('Признак "Способ использования"', main_asset.fixed_asset_usage)
                            new_distributed_bill.setdefault("Площадь", session.query(FixedAssets).filter(FixedAssets.building_id == _current_building_id).first().fixed_asset_square)
                            current_main_asset = session.query(FixedAssets).filter(FixedAssets.building_id == _current_building_id).all()
                            list_of_squares = []
                            for current_main_asset in current_main_asset:
                                list_of_squares.append(current_main_asset.fixed_asset_square)
                            sum_list = sum(list_of_squares)

                            if sum_list <= 0 or (not main_asset.fixed_asset_used and not main_asset.fixed_asset_usage):
                                new_distributed_bill.setdefault("Сумма распределения", 0)
                            else:
                                new_distributed_bill.setdefault("Сумма распределения", row['Стоимость без НДС'] * (new_distributed_bill["Площадь"] / (sum(list_of_squares))))
                            new_distributed_bill.setdefault("Счет главной книги", predict_main_bill(new_distributed_bill))
                            distributed_bills.append(new_distributed_bill)
                            distributed_position += 1
                    #distributed_bills.append(current_distributed_bills)
        cool_dataframe = pd.DataFrame(columns=_get_distributed_columns(), data=[cool_values.values() for cool_values in distributed_bills])
        cool_dataframe.to_excel("distributed_bills.xlsx", index=False)
        with open('distributed_bills.xlsx', 'rb') as f:
            mini.load_data_bytes('user-tabels',f'{user_name}/bills/{file_name}', f.read())
        # Преобразование списка словарей в датафрейм
        # df = pd.DataFrame(list_of_dicts)

        # Сохранение датафрейма в xlsx файл
        #df.to_excel("output.xlsx", index=False)
        return "distributed_bills"

def _get_distributed_columns() -> dict:
    return {
        "Компания": "",
        "Год счета": "",
        "Номер счета": "",	
        "Позиция счета": "",
        "Номер позиции распределения": "", 	
        "Дата отражения в учетной системе": "",	
        "ID договора": "",	
        "Услуга": "",	
        "Класс услуги": "",	
        "Здание": "",	
        "Класс ОС": "",	
        "ID основного средства": "",	
        'Признак "Использование в основной деятельности"': "",	
        'Признак "Способ использования"': "",
        "Площадь": "",	
        "Сумма распределения": "",	
        "Счет главной книги": "",
    }

            




