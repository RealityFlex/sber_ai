import pandas as pd
from catboost import CatBoostClassifier
import logging
import progressbar
from joblib import load
import  numpy as np
import json
import utils.mini as mini
import warnings
with warnings.catch_warnings():
    warnings.simplefilter(action='ignore', category=EncodingWarning)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

model = CatBoostClassifier()      # parameters not required.
model.load_model('../../main_bills_predict.cbm')
cool_encoder = load('../../main_bill_encoder.joblib')

# Функция распределяет счета
def distribute_bills(user_name: str, bills_link: str):
    #bills = pd.read_excel(client.presigned_get_object('user-tabels',f'{user_name}/hardcoded/{file_name}'), ignore_index=True)
    bills = pd.read_excel(mini.presigned_get_object('user-tabels',f'{user_name}/bills/low_data.xlsx')).head(1000)
    distributed_columns = _get_distributed_columns()
    # связь договоров и зданий
    #bills = mini.presigned_get_object('user-tabels',f'{user_name}/hardcoded/contracts_relationship.xlsx')
    contracts_relationship = pd.read_excel(mini.presigned_get_object('user-tabels',f'{user_name}/hardcoded_sdfll73hsakdlfjjkdsfhakhbhja/contracts_relationship.XLSX'))
    
    # основные средства
    main_assets = pd.read_excel(mini.presigned_get_object('user-tabels',f'{user_name}/hardcoded_sdfll73hsakdlfjjkdsfhakhbhja/main_assets.xlsx'))
    
    # коды услуг
    service_codes = pd.read_excel(mini.presigned_get_object('user-tabels',f'{user_name}/hardcoded_sdfll73hsakdlfjjkdsfhakhbhja/service_codes.xlsx'))

    distributed_bills = []
    # по каждому договору получать все здания
    # по каждому зданию получать все основные средства
    with progressbar.ProgressBar(max_value=bills.shape[0]) as bar:
        for idx_row, row in bills.iterrows():
            bar.update(idx_row)      
            _contract_id = row["ID договора"]
            list_contracts = contracts_relationship.query("`ID договора` == @_contract_id")
            if len(list_contracts) > 0:
                distributed_position = 1
                for idx_contract, contract_relation in list_contracts.iterrows():
                    current_distributed_bills = []
                    #print(contract_relation[0])
                    _current_building_id = contract_relation["ID здания"]
                    _current_service_id = row["ID услуги"]
                    list_main_assets = main_assets.query("`ID здания` == @_current_building_id")
                    if len(list_main_assets) > 0:
                        for idx_main_assets, main_asset in main_assets.iterrows():
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
                            new_distributed_bill.setdefault("ID договора", contract_relation["ID договора"])
                            new_distributed_bill.setdefault("Услуга", row["ID услуги"])
                            new_distributed_bill.setdefault("Здание", contract_relation["ID здания"])
                            new_distributed_bill.setdefault("Класс ОС", main_asset["Класс основного средства"])
                            new_distributed_bill.setdefault("Класс услуги", service_codes.query("`ID услуги` == @_current_service_id")["Класс услуги"].iloc[0])
                            new_distributed_bill.setdefault("ID основного средства", main_asset["ID основного средства"])
                            new_distributed_bill.setdefault('Признак "Использование в основной деятельности"', main_asset['Признак "Используется в основной деятельности"'])
                            new_distributed_bill.setdefault('Признак "Способ использования"', main_asset['Признак "Способ использования"'])
                            new_distributed_bill.setdefault("Площадь", main_assets.query("`ID здания` == @_current_building_id").head(1)["Площадь"])
                            current_main_asset = main_assets.query("`ID здания`== @_current_building_id")
                            list_of_squares = []
                            for idx_current_main_asset, current_main_asset in current_main_asset.iterrows():
                                list_of_squares.append(current_main_asset["Площадь"])
                            sum_list = sum(list_of_squares)

                            if sum_list <= 0 or (not main_asset['Признак "Используется в основной деятельности"'] and not main_asset['Признак "Способ использования"']):
                                new_distributed_bill.setdefault("Сумма распределения", 0)
                            else:
                                new_distributed_bill.setdefault("Сумма распределения", row['Стоимость без НДС'] * (new_distributed_bill["Площадь"] / (sum(list_of_squares))))
                            new_distributed_bill.setdefault("Счет главной книги", predict_main_bill(new_distributed_bill))
                            distributed_bills.append(new_distributed_bill)
                            distributed_position += 1
                    #distributed_bills.append(current_distributed_bills)
        cool_dataframe = pd.DataFrame(columns=distributed_columns, data=[cool_values.values() for cool_values in distributed_bills])
        return distributed_bills
        

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

            




