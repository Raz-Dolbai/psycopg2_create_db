import psycopg2
from data_to_insert import dns

try:
    conn = psycopg2.connect(dns)
    cursor = conn.cursor()
    cursor.execute("""select DISTINCT name_client from author
                    join book USING (author_id)
                    join buy_book using (book_id)
                    join buy using (buy_id)
                    join client using (client_id)
                    where name_author = 'Достоевский Ф.М.'
                    order by name_client;""")
    x = cursor.fetchall()
    print(x)
except Exception as exc:
    print(f'Упс... Произошла ошибка {exc}')
finally:
    cursor.close()
    conn.close()