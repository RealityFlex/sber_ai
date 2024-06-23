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
import redis

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

r = redis.Redis(host='62.109.8.64', port=6377, db=0)

model = CatBoostClassifier()      # parameters not required.
model.load_model('ml-models/main_bills_predict.cbm')
cool_encoder = load('ml-models/main_bill_encoder.joblib')
cluster_encoder = load('ml-models/cluster_encoder.joblib')

service_class_cluster_model = CatBoostClassifier()
service_class_cluster_model.load_model('ml-models/service_class_cluster_model.cbm')

# Функция распределяет счета
def distribute_bills(SessionLocal: sessionmaker, user_name: str, bills_link: str, task_id):
    try:
        with SessionLocal() as session:
            print("ASD")
            r.set(task_id, json.dumps({"status":"PENDING", "result":0}))
            print("LINK", mini.presigned_get_object('user-tabels',f'{user_name}/filter/1.xlsx'))
            _load_service_classes(SessionLocal, user_name),
            _load_contract_building_relationship(SessionLocal, user_name),
            _load_fixed_assets(SessionLocal, user_name)
            bills = pd.read_excel(mini.presigned_get_object('user-tabels',f'{user_name}/filter/1.xlsx'))
            print("1", bills.head())
            distributed_columns = _get_distributed_columns()
            print("2", distributed_columns)
            df_for_graphs =_distribute_bills_by_building(session, user_name, bills.head(1000), "distributed_bills.xlsx", task_id)
            print("3", df_for_graphs)
            # new_bills = predict_future_bills(session, bills)
            # _distribute_bills_by_building(session, user_name, new_bills, "distributed_bills_predict.xlsx", task_id)
            # по каждому договору получать все здания
            # по каждому зданию получать все основные средства
            donut_graph = get_data_for_donut_graphs(df_for_graphs)
            print("4", donut_graph)
            dots_graph = get_data_for_dot_graphs(df_for_graphs)
            print("5", dots_graph)
            _delete_data(SessionLocal, user_name)
            res =  {
                "distributed_bills": mini.presigned_get_object('user-tabels',f'{user_name}/result/distributed_bills.xlsx'),
                "export_distributed_bills_csv": mini.presigned_get_object('user-tabels', f'{user_name}/result/distributed_bills.csv'),
                "donut_graph": donut_graph,
                "dots_graph": dots_graph
                }
            print(6, res)
            r.set(task_id, json.dumps({"status":"SUCCESS", "result": res}))
            # requests.post(f"http://62.109.8.64:8288/result?task_id={task_id}", data=res)
            return res
    except Exception as e:
        print("ERROR -- ", e)
        r.set(task_id, json.dumps({"status":"FAILURE", "result": e}))
        _delete_data(SessionLocal, user_name)

def distribute_predicted_bills(SessionLocal: sessionmaker, user_name: str, bills_link: str, task_id):
    try:
        with SessionLocal() as session:
            _load_service_classes(SessionLocal, user_name),
            _load_contract_building_relationship(SessionLocal, user_name),
            _load_fixed_assets(SessionLocal, user_name)

            bills = pd.read_excel(mini.presigned_get_object('user-tabels',f'{user_name}/bills/low_data.xlsx'))
            distributed_columns = _get_distributed_columns()

            new_bills = predict_future_bills(session, bills.head(1000))
            df_for_graphs = _distribute_bills_by_building(session, user_name, new_bills, "distributed_bills_predict.xlsx", task_id)
            donut_graph = get_data_for_donut_graphs(df_for_graphs)
            dots_graph = get_data_for_dot_graphs(df_for_graphs)
            _delete_data(SessionLocal, user_name)
        
            return {
                "distributed_bills": mini.presigned_get_object('user-tabels',f'{user_name}/result/distributed_bills_predict.xlsx'),
                "export_distributed_bills_csv": mini.presigned_get_object('user-tabels', f'{user_name}/result/distributed_bills_predict.csv'),
                "donut_graph": donut_graph,
                "dots_graph": dots_graph
            }
    except Exception as e:
        print("ERROR", e)
        r.set(task_id, json.dumps({"status":"FAILURE", "result": e}))
        _delete_data(SessionLocal, user_name)

def get_data_for_donut_graphs(distributed_bills: pd.DataFrame):
        df_for_donut = distributed_bills
        df_for_donut['Дата отражения в учетной системе'] = pd.to_datetime(df_for_donut['Дата отражения в учетной системе'], dayfirst=True)
        df_for_donut['Месяц-Год'] = df_for_donut['Дата отражения в учетной системе'].dt.strftime('%m-%Y')

        # Группировка данных по месяцу и году с суммированием
        grouped_df = df_for_donut.groupby('Месяц-Год')['Сумма распределения'].sum().reset_index()

        return {"series": grouped_df["Сумма распределения"].to_list(), "labels": grouped_df["Месяц-Год"].to_list()}

def get_data_for_dot_graphs(distributed_bills: pd.DataFrame):
    df_for_dots = distributed_bills
    df_grouped = df_for_dots.groupby('Услуга').agg({'Сумма распределения': 'sum', 'Позиция счета': 'count'}).reset_index()
    df_grouped = df_grouped.sort_values(by=['Позиция счета', 'Сумма распределения'], ascending=[False, False])
    sizes = df_grouped['Сумма распределения'] / df_for_dots['Сумма распределения'].mean()
    output_dots = []
    distribute_sum = df_grouped['Сумма распределения'].to_list()
    index = df_grouped.index.to_list()
    size = sizes.to_list()
    for i in range(0, len(distribute_sum)):
        new_temp_dict = {"x": index[i], "y": distribute_sum[i], "r": size[i]}
        output_dots.append(new_temp_dict)

    return output_dots


    


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

# Экспорт csv
def _export_csv(export_dataframe: pd.DataFrame, file_name: str, user_name: str):
    export_dataframe = export_dataframe[["Компания",
                                         "Год счета",
                                         "Номер счета",
                                         "Позиция счета",
                                         "Номер позиции распределения",
                                         "Дата отражения в учетной системе",
                                         "ID договора",
                                         "Услуга",
                                         "Класс услуги",
                                         "Здание",
                                         "Класс ОС",
                                         "ID основного средства",
                                         'Признак "Использование в основной деятельности"',
                                         'Признак "Способ использования"',
                                         "Площадь",
                                         "Сумма распределения",
                                         "Счет главной книги"
                                         ]] 
    export_dataframe = export_dataframe.rename(columns={'Класс услуги': 'ID услуги'})
    export_dataframe = export_dataframe.rename(columns={'Признак "Использование в основной деятельности"': 'Признак использования в основной деятель.'})
    export_dataframe = export_dataframe.rename(columns={'Признак "Способ использования"': 'Признак передачи в аренду'})
    export_dataframe.to_csv(file_name, index=False)
    with open(file_name, 'rb') as f:
        mini.load_data_bytes('user-tabels',f'{user_name}/result/{file_name}', f.read())

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
            r.set(task_id, json.dumps({"status":"PENDING", "result": round(idx_row/max_value_l*100,2)}))
            # r.set(task_id, str(round(idx_row/max_value_l*100,2)))
            # requests.get(f"http://62.109.8.64:8288/prog?task_id={task_id}&curent={idx_row}&max={max_value_l}")
            _contract_id = row["ID договора"]
            list_contracts = session.query(ContractRelationship).filter(ContractRelationship.contract_id == _contract_id).all()
            
            if list_contracts:
                distributed_position = 1
                for contract_relation in list_contracts:
                    list_main_assets = session.query(FixedAssets).filter(FixedAssets.building_id == contract_relation.building_id).all()
                    
                    if list_main_assets:
                        for main_asset in list_main_assets:
                            new_distributed_bill = {
                                "Компания": row["Компания"],
                                "Год счета": row["Год"],
                                "Номер счета": row["Номер счета"],
                                "Позиция счета": row["Позиция счета"],
                                "Номер позиции распределения": distributed_position,
                                "Дата отражения в учетной системе": pd.to_datetime(row["Дата отражения счета в учетной системе"]) if pd.notna(row["Дата отражения счета в учетной системе"]) else pd.to_datetime("1900-01-01"),
                                "ID договора": contract_relation.contract_id,
                                "Услуга": row["ID услуги"],
                                "Здание": contract_relation.building_id,
                                "Класс ОС": main_asset.fixed_asset_class,
                                "Класс услуги": session.query(ServiceCodes).filter(ServiceCodes.service_id == row["ID услуги"]).first().service_class,
                                "ID основного средства": main_asset.fixed_asset_id,
                                'Признак "Использование в основной деятельности"': main_asset.fixed_asset_used,
                                'Признак "Способ использования"': main_asset.fixed_asset_usage,
                                "Площадь": main_asset.fixed_asset_square
                            }
                            
                            list_of_squares = [fa.fixed_asset_square for fa in session.query(FixedAssets).filter(FixedAssets.building_id == contract_relation.building_id).all()]
                            sum_list = sum(list_of_squares)
                            
                            if sum_list <= 0 or (not main_asset.fixed_asset_used and not main_asset.fixed_asset_usage):
                                new_distributed_bill["Сумма распределения"] = 0
                            else:
                                new_distributed_bill["Сумма распределения"] = row['Стоимость без НДС'] * (new_distributed_bill["Площадь"] / sum_list)
                            
                            new_distributed_bill["Счет главной книги"] = predict_main_bill(new_distributed_bill)
                            distributed_bills.append(new_distributed_bill)
                            distributed_position += 1
    
    cool_dataframe = pd.DataFrame(distributed_bills)
    cool_dataframe.to_excel("distributed_bills.xlsx", index=False)
    _export_csv(cool_dataframe, file_name, user_name)
    
    with open('distributed_bills.xlsx', 'rb') as f:
        mini.load_data_bytes('user-tabels', f'{user_name}/result/{file_name}', f.read())
    
    return cool_dataframe

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