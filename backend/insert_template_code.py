from elasticsearch import Elasticsearch
import pandas as pd
import  os
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

def insert_data(path: str, data_ready: bool = False) -> None:
    es = Elasticsearch("http://localhost:9200")
    # Чтение данных из Excel файла
    df = pd.read_excel(path)
    if data_ready:
        # Замена "X" на True и пустых значений на False
        df['Признак "Используется в основной деятельности"'] = df['Признак "Используется в основной деятельности"'].apply(lambda x: True if x == 'X' else False)
        df['Признак "Способ использования"'] = df['Признак "Способ использования"'].apply(lambda x: True if x == 'X' else False)

        # Заполнение отсутствующих дат выбытия значением "1/1/1900"
        df['Дата выбытия'] = df['Дата выбытия'].fillna("1/1/1900")
    
    # Получение имени файла без расширения для использования в качестве названия индекса
    file_name = os.path.splitext(os.path.basename(path))[0].lower().replace(" ", "_")
    
    # Проверка существования индекса и его создание, если он не существует
    if not es.indices.exists(index=file_name):
        es.indices.create(index=file_name)
    
    # Преобразование данных DataFrame в формат, пригодный для вставки в Elasticsearch
    for i, row in df.iterrows():
        # Преобразование строки в словарь
        doc = row.to_dict()
        # Вставка данных в Elasticsearch
        es.index(index=file_name, body=doc)

# insert_data("data/hardcoded/Площади зданий.XLSX")
insert_data("data/hardcoded/Связь договор - здания.xlsx")
# insert_data("../data/hardcoded/Коды услуг.xlsx")
# insert_data("../data/hardcoded/Основные средства.xlsx")