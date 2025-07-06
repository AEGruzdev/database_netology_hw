import psycopg2
from pprint import pprint
from config import password
from config import login


passw = password
log = login


def create_tables():
    '''
    Функция создающая две таблицы, из которых состоит наша база данных
    В первой содержится имя, фамилия, email
    Во второй id каждого клиента и номера телефонов
    '''
    conn = psycopg2.connect(database = 'CLIENTSBASE', user = log, password = passw)
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS clients(
                    id SERIAL PRIMARY KEY,
                    first_name VARCHAR(50) NOT NULL,
                    last_name VARCHAR(50) NOT NULL,
                    email VARCHAR(100) UNIQUE NOT NULL);
            """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS clients_numbers(
                    id SERIAL PRIMARY KEY,
                    phone_number VARCHAR(20),
                    client_id INTEGER NOT NULL REFERENCES clients(id) ON DELETE CASCADE);
            """)
        conn.commit()
    conn.close()


def add_client(first_name: str, last_name: str, email: str, phone_numbers: list = None):
    '''
    Функция, которая добавляет нового клиента, номера телефонов передаются списком,
    если клиент не хочет оставлять номер, то не передается ничего
    '''
    conn = psycopg2.connect(database='CLIENTSBASE', user=log, password=passw)
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO clients (first_name, last_name, email) 
            VALUES (%s, %s, %s) 
            RETURNING id;
            """, (first_name, last_name, email))
        conn.commit()

        client_id = cur.fetchone()[0]
        
        if phone_numbers == []:
            return
        else:
            for number in phone_numbers:
                cur.execute("""
                    INSERT INTO clients_numbers(phone_number, client_id) 
                    VALUES (%s, %s);
                    """, (number, client_id))
                conn.commit()
    conn.close()
    

def add_phone_number(phone_number: str, client_id: int):
    '''
    Функция, которая добавляет номер телефона к существующему клиенту
    '''
    conn = psycopg2.connect(database='CLIENTSBASE', user=log, password=passw)
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO clients_numbers(phone_number, client_id) 
            VALUES (%s, %s);
            """, (phone_number, client_id))
        conn.commit()
    conn.close()


def update_client_info(client_id, first_name: str = None, last_name: str = None, email: str = None, phone_numbers: list = None):
    '''
    Функция изменяющая данные клиента
    Принимает в себя id-клиента, и как одно, так и несколько изменений, которые надо внести
    '''
    if first_name is None and last_name is None and email is None and phone_numbers is None:
        return ('Вы ничего не изменили')
    else:
        conn = psycopg2.connect(database='CLIENTSBASE', user=log, password=passw)
        with conn.cursor() as cur:
            if first_name is not None:
                cur.execute("""
                UPDATE clients SET first_name = %s WHERE id = %s;
                """, (first_name, client_id))
            if last_name is not None:
                cur.execute("""
                UPDATE clients SET last_name = %s WHERE id = %s;
                """, (last_name, client_id))
            if email is not None:
                cur.execute("""
                UPDATE clients SET email = %s WHERE id = %s;
                """, (email, client_id))
            if phone_numbers is not None:
                cur.execute("""DELETE FROM clients_numbers WHERE client_id = %s""", (client_id,))
                if phone_numbers:
                    for number in phone_numbers:
                        cur.execute("""
                            INSERT INTO clients_numbers (phone_number, client_id)
                            VALUES (%s, %s)
                            """, (number, client_id))
            conn.commit()
        conn.close()


def delete_phone_number(client_id, phone_number):
    '''
    Функция удаляет номер клиента, на вход принимает ДВА аргумента - id и номер, который нужно удалить
    Так сделано специально, т.к. номер телефона может не является уникальным 
    (например один номер может быть указан и для мужа и для жены, чтобы не удалить номер у всех, дополнительно запрашивается еще и id клиента)
    '''
    conn = psycopg2.connect(database='CLIENTSBASE', user=log, password=passw)
    with conn.cursor() as cur:
        cur.execute("""
        DELETE FROM clients_numbers WHERE client_id = %s AND phone_number = %s
        """, (client_id, phone_number))
        conn.commit()
    conn.close()


def delete_client(client_id):
    '''
    Функция удаляет клиента из базы, по id
    '''
    conn = psycopg2.connect(database='CLIENTSBASE', user=log, password=passw)
    with conn.cursor() as cur:
        cur.execute("""DELETE FROM clients WHERE id = %s
        """, (client_id, ))
        conn.commit()
    conn.close()


def show_client(first_name: str = None, last_name: str = None, email: str = None, phone_number: str = None):
    '''
    Функция, которая находит клиента по любой вводной информации 
    (Запрос может содержать в себе как один, так несколько аргументов, например одновременно Имя и Фамилия)
    Если такого клиента нет - вернет пустой список
    '''
    conn = psycopg2.connect(database='CLIENTSBASE', user=log, password=passw)
    with conn.cursor() as cur:
        query = "SELECT c.id, c.first_name, c.last_name, c.email, cn.phone_number FROM clients c LEFT JOIN clients_numbers cn ON c.id = cn.client_id"
        query_plus = []
        params = []
        if first_name:
            query_plus.append("c.first_name ILIKE %s")
            params.append(f"%{first_name}%")    
        if last_name is not None:
            query_plus.append("c.last_name ILIKE %s")
            params.append(f"%{last_name}%")    
        if email is not None:
            query_plus.append("c.email = %s")
            params.append(email)
        if phone_number is not None:
            query_plus.append("cn.phone_number = %s")
            params.append(phone_number)
        if query_plus:
            query += ' WHERE ' + ' AND '.join(query_plus)
        cur.execute(query, params)
        result = cur.fetchall()
        clients = {}
        for row in result:
            client_id = row[0]
            if client_id not in clients:
                clients[client_id] = {
                    'id': row[0],
                    'first_name': row[1],
                    'last_name': row[2],
                    'email': row[3],
                    'phones': []
                }
            if row[4]: 
                clients[client_id]['phones'].append(row[4])
        return list(clients.values())  


def show_all_base():
    '''
    Функция, которая выводит всю базу клиентов, в удобном виде
    '''
    conn = psycopg2.connect(database='CLIENTSBASE', user=log, password=passw)
    with conn.cursor() as cur:
        query = "SELECT c.id, c.first_name, c.last_name, c.email, cn.phone_number FROM clients c LEFT JOIN clients_numbers cn ON c.id = cn.client_id"
        cur.execute(query)
        result = cur.fetchall()
        clients = {}
        for row in result:
            client_id = row[0]
            if client_id not in clients:
                clients[client_id] = {
                    'id': row[0],
                    'first_name': row[1],
                    'last_name': row[2],
                    'email': row[3],
                    'phones': []
                }
            if row[4]: 
                clients[client_id]['phones'].append(row[4])
        return list(clients.values())  
    



#create_tables()
#add_client('Павел', 'Иванов', 'pasha@mail.ru', ['+6728328383', '83932321313'] )
#add_client('Лада', 'Петрова', 'ls@mail.ru', ['83832131311', '+7382018321'])
#add_client('Михаил', 'Аликин', 'mii@mail.ru', ['83232131341', '+7982818321'])
#add_client('Артем', 'Михайлов', 'am@mail.ru', ['83832111341', '+7384818321'])
#add_client('Артур', 'Алмазов', 'aa@mail.ru', ['83832121341', '+7312818321'])
#add_client('Наташа', 'Рогова', 'jfkljdfklsdsdjfklj@dsadas', ['8383211341323', '+7381231481831'])
#pprint(show_client(last_name='Иванов'))
#delete_client(6)
#add_phone_number('3232323232', 11)
#update_client_info(11, last_name='Рогова', email='asd@dsadsad')
#pprint(show_client(last_name='Аdsd'))
#pprint(show_all_base())
