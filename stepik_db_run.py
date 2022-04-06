from stepik_db import DataDefinitionLanguage, DataManipulationLanguage
from data_to_insert import *

# ----------Создать все таблицы--------------------
# create = DataDefinitionLanguage(dns)
# create.create_table_genre()
# create.create_table_author()
# create.create_table_book()
# create.create_table_city()
# create.create_table_client()
# create.create_table_buy()
# create.create_table_buy_book()
# create.create_table_step()
# create.create_table_buy_step()

# ----------Удалить все таблицы-------------------------
# drop_table = DataDefinitionLanguage(dns)
# tables = ('author', 'buy', 'book', 'buy_book', 'buy_step', 'city', 'client', 'genre', 'step')
# for table in tables:
#     drop_table.drop_table(table)

# -----------Вставить данные в таблицу-------------------
# Данные для инсерта в data_to_insert.py
insert_data = DataManipulationLanguage(dns)
insert_data.insert_data_author(name_authors)
insert_data.insert_data_genre(name_genre)
insert_data.insert_data_book(book_data)
insert_data.insert_data_city(city_data)
insert_data.insert_data_client(client_data)
insert_data.insert_data_buy(buy_data)
insert_data.insert_data_buy_book(buy_book)
insert_data.insert_data_step(step_data)
insert_data.insert_data_buy_step(buy_step_data)


