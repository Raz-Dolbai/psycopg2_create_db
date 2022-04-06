import psycopg2
from psycopg2.extras import execute_values


class DataDefinitionLanguage:
    """Класс для работы с DDL запросами, ожидпет строку psucopg2.connect()"""

    def __init__(self, dns):
        self.dns = dns

    def create_table_genre(self):
        """Создает таблицу genre"""
        try:
            connect = psycopg2.connect(self.dns)
            cursor = connect.cursor()
            cursor.execute("""CREATE TABLE IF NOT EXISTS genre 
                             (
                            genre_id int PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
                            name_genre varchar (32)
                            );""")
            connect.commit()
        except Exception as exc:
            print(f'Что то пошло не так , ошибка {exc}')
        finally:
            cursor.close()
            connect.close()

    def create_table_author(self):
        """Создает таблицу author"""
        try:
            connect = psycopg2.connect(self.dns)
            cursor = connect.cursor()
            cursor.execute("""CREATE TABLE IF NOT EXISTS author 
                           (
                           author_id int PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
                           name_author varchar (64)
                           );""")
            connect.commit()
        except Exception as exc:
            print(f'Что то пошло не так , ошибка {exc}')
        finally:
            cursor.close()
            connect.close()

    def create_table_city(self):
        """Создает таблицу city"""
        try:
            connect = psycopg2.connect(self.dns)
            cursor = connect.cursor()
            cursor.execute("""CREATE TABLE IF NOT EXISTS city 
                           (
                           city_id int PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
                           name_city varchar (30),
                           days_delivery INT
                           );""")
            connect.commit()
        except Exception as exc:
            print(f'Что то пошло не так , ошибка {exc}')
        finally:
            cursor.close()
            connect.close()

    def create_table_book(self):
        """Создает таблицу book"""
        try:
            connect = psycopg2.connect(self.dns)
            cursor = connect.cursor()
            cursor.execute("""CREATE TABLE IF NOT EXISTS book 
                           (
                           book_id int PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
                           title varchar (30),
                           author_id INT NOT NULL,
                           genre_id INT NOT NULL,
                           price DECIMAL(8,2),
                           amount INT,
                           CONSTRAINT FK_book_author_id FOREIGN KEY (author_id) REFERENCES author(author_id) ON DELETE CASCADE,
                           CONSTRAINT FK_book_genre_id FOREIGN KEY (genre_id) REFERENCES genre(genre_id) ON DELETE CASCADE
                           );""")
            connect.commit()
        except Exception as exc:
            print(f'Что то пошло не так , ошибка {exc}')
        finally:
            cursor.close()
            connect.close()

    def create_table_client(self):
        """Создает таблицу client"""
        try:
            connect = psycopg2.connect(self.dns)
            cursor = connect.cursor()
            cursor.execute("""CREATE TABLE IF NOT EXISTS client 
                           (
                           client_id int PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
                           name_client varchar(50),
                           city_id INT,
                           email varchar(30),
                           CONSTRAINT FK_client_city_id FOREIGN KEY (city_id) REFERENCES city(city_id) ON DELETE CASCADE
                           );""")
            connect.commit()
        except Exception as exc:
            print(f'Что то пошло не так , ошибка {exc}')
        finally:
            cursor.close()
            connect.close()

    def create_table_buy(self):
        """Создает таблицу buy"""
        try:
            connect = psycopg2.connect(self.dns)
            cursor = connect.cursor()
            cursor.execute("""CREATE TABLE IF NOT EXISTS buy 
                           (
                           buy_id int PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
                           buy_description varchar(100),
                           client_id INT,
                           CONSTRAINT FK_buy_client_id FOREIGN KEY (client_id) REFERENCES client(client_id) ON DELETE CASCADE
                           );""")
            connect.commit()
        except Exception as exc:
            print(f'Что то пошло не так , ошибка {exc}')
        finally:
            cursor.close()
            connect.close()

    def create_table_buy_book(self):
        """Создает таблицу buy_book"""
        try:
            connect = psycopg2.connect(self.dns)
            cursor = connect.cursor()
            cursor.execute("""CREATE TABLE IF NOT EXISTS buy_book 
                           (
                           buy_book_id int PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
                           buy_id INT,
                           book_id INT,
                           amount INT,
                           CONSTRAINT FK_buy_book_buy_id FOREIGN KEY (buy_id) REFERENCES buy(buy_id) ON DELETE CASCADE,
                           CONSTRAINT FK_buy_book_id FOREIGN KEY (book_id) REFERENCES book(book_id) ON DELETE CASCADE
                           );""")
            connect.commit()
        except Exception as exc:
            print(f'Что то пошло не так , ошибка {exc}')
        finally:
            cursor.close()
            connect.close()

    def create_table_step(self):
        """Создает таблицу step"""
        try:
            connect = psycopg2.connect(self.dns)
            cursor = connect.cursor()
            cursor.execute("""CREATE TABLE IF NOT EXISTS step 
                           (
                           step_id int PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
                           name_step varchar(30)
                           );""")
            connect.commit()
        except Exception as exc:
            print(f'Что то пошло не так , ошибка {exc}')
        finally:
            cursor.close()
            connect.close()

    def create_table_buy_step(self):
        """Создает таблицу buy_step"""
        try:
            connect = psycopg2.connect(self.dns)
            cursor = connect.cursor()
            cursor.execute("""CREATE TABLE IF NOT EXISTS buy_step 
                           (
                           buy_step_id int PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
                           buy_id INT,
                           step_id INT,
                           date_step_beg DATE DEFAULT null,
                           date_step_end DATE DEFAULT null,
                           CONSTRAINT FK_buy_step_buy_id FOREIGN KEY (buy_id) REFERENCES buy(buy_id),
                           CONSTRAINT FK_buy_step_step_id FOREIGN KEY (step_id) REFERENCES step(step_id)
                           );""")
            connect.commit()
        except Exception as exc:
            print(f'Что то пошло не так , ошибка {exc}')
        finally:
            cursor.close()
            connect.close()

    def drop_table(self, table):
        """Удаляет таблицб в качестве аргумента принимает название таблицы"""
        try:
            connect = psycopg2.connect(self.dns)
            cursor = connect.cursor()
            cursor.execute('DROP TABLE {} CASCADE'.format(table))
            connect.commit()
        except Exception as exc:
            print(f'Произошла ошибка {exc}')
        finally:
            cursor.close()
            connect.close()


class DataManipulationLanguage:
    """Класс с DML запросами, ожидает строку для psucopg2.connect()"""

    def __init__(self, dns):
        self.dns = dns

    def insert_data_author(self, name_authors):
        """Вставляет данные в таблицу author"""
        try:
            connect = psycopg2.connect(self.dns)
            cursor = connect.cursor()
            execute_values(cursor, 'INSERT INTO author(name_author) VALUES %s', name_authors)
            connect.commit()
        except Exception as exc:
            print(f'Произошла ошибка {exc}')
        finally:
            cursor.close()
            connect.close()

    def insert_data_genre(self, name_genre):
        """Вставляет данные в таблицу genre"""
        try:
            connect = psycopg2.connect(self.dns)
            cursor = connect.cursor()
            execute_values(cursor, 'INSERT INTO genre(name_genre) VALUES %s', name_genre)
            connect.commit()
        except Exception as exc:
            print(f'Произошла ошибка {exc}')
        finally:
            cursor.close()
            connect.close()

    def insert_data_book(self, book_data):
        """Вставляет данные в таблицу book"""
        try:
            connect = psycopg2.connect(self.dns)
            cursor = connect.cursor()
            execute_values(cursor, 'INSERT INTO book(title, author_id, genre_id, price, amount) VALUES %s',
                           book_data)
            connect.commit()
        except Exception as exc:
            print(f'Произошла ошибка {exc}')
        finally:
            cursor.close()
            connect.close()

    def insert_data_city(self, city_data):
        """Вставляет данные в таблицу city"""
        try:
            connect = psycopg2.connect(self.dns)
            cursor = connect.cursor()
            execute_values(cursor, 'INSERT INTO city(name_city, days_delivery) VALUES %s',
                           city_data)
            connect.commit()
        except Exception as exc:
            print(f'Произошла ошибка {exc}')
        finally:
            cursor.close()
            connect.close()

    def insert_data_client(self, client_data):
        """Вставляет данные в таблицу client"""
        try:
            connect = psycopg2.connect(self.dns)
            cursor = connect.cursor()
            execute_values(cursor, 'INSERT INTO client(name_client, city_id, email) VALUES %s',
                           client_data)
            connect.commit()
        except Exception as exc:
            print(f'Произошла ошибка {exc}')
        finally:
            cursor.close()
            connect.close()

    def insert_data_buy(self, buy_data):
        """Вставляет данные в таблицу buy_data"""
        try:
            connect = psycopg2.connect(self.dns)
            cursor = connect.cursor()
            execute_values(cursor, 'INSERT INTO buy(buy_description, client_id) VALUES %s',
                           buy_data)
            connect.commit()
        except Exception as exc:
            print(f'Произошла ошибка {exc}')
        finally:
            cursor.close()
            connect.close()

    def insert_data_buy_book(self, buy_book):
        """Вставляет данные в таблицу buy_book"""
        try:
            connect = psycopg2.connect(self.dns)
            cursor = connect.cursor()
            execute_values(cursor, 'INSERT INTO buy_book(buy_id, book_id, amount) VALUES %s',
                           buy_book)
            connect.commit()
        except Exception as exc:
            print(f'Произошла ошибка {exc}')
        finally:
            cursor.close()
            connect.close()

    def insert_data_step(self, step_data):
        """Вставляет данные в таблицу step"""
        try:
            connect = psycopg2.connect(self.dns)
            cursor = connect.cursor()
            execute_values(cursor, 'INSERT INTO step(name_step) VALUES %s',
                           step_data)
            connect.commit()
        except Exception as exc:
            print(f'Произошла ошибка {exc}')
        finally:
            cursor.close()
            connect.close()

    def insert_data_buy_step(self, buy_step_data):
        """Вставляет данные в таблицу buy_step"""
        try:
            connect = psycopg2.connect(self.dns)
            cursor = connect.cursor()
            execute_values(cursor, 'INSERT INTO buy_step(buy_id, step_id, date_step_beg, date_step_end) VALUES %s',
                           buy_step_data)
            connect.commit()
        except Exception as exc:
            print(f'Произошла ошибка {exc}')
        finally:
            cursor.close()
            connect.close()
