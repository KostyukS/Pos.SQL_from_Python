import psycopg2
from pprint import pprint

Name_database = 'Client_management'
User_name = 'postgres'
Password = 'admin'


# Функция удаляющая все таблицы:
def del_all_table():
    conn = psycopg2.connect(database=Name_database, user=User_name, password=Password)
    with conn.cursor() as cur:
        cur.execute("""DROP TABLE telephone_number;
                    DROP  TABLE first_name_last_name_email;""")
        print('Таблицы удалены')
        conn.commit()
    conn.close()


# Функция, создающая структуру БД (таблицы):
def create_table():
    conn = psycopg2.connect(database=Name_database, user=User_name, password=Password)
    with conn.cursor() as cur:
        cur.execute(
            """CREATE TABLE IF NOT EXISTS  first_name_last_name_email(id SERIAL PRIMARY KEY,
               first_name  VARCHAR(40) NOT NULL, last_name  VARCHAR(40) NOT NULL,
               email VARCHAR NOT NULL UNIQUE );""")
        cur.execute(
            """CREATE TABLE IF NOT EXISTS telephone_number(client_id INTEGER REFERENCES first_name_last_name_email (id)
             ON DELETE CASCADE , 
               "number" varchar(11) unique ,
                CONSTRAINT cl_fir PRIMARY KEY (client_id, number));""")
        print('Таблицы созданы')
        conn.commit()
    conn.close()


# Функция, позволяющая добавить нового клиента:
def add_client(f_name, l_name, mail):
    conn = psycopg2.connect(database=Name_database, user=User_name, password=Password)
    with conn.cursor() as cur:
        cur.execute("""INSERT INTO first_name_last_name_email(first_name, last_name, email)
                    VALUES (%s, %s, %s ) RETURNING first_name, last_name, email;""", (f_name, l_name, mail))
        print(f'Клиент {cur.fetchone()} добавлен')
        conn.commit()
    conn.close()


# Функция, позволяющая добавить телефон для существующего клиента:
def add_telephone(mail, telephone):
    conn = psycopg2.connect(database=Name_database, user=User_name, password=Password)
    with conn.cursor() as cur:
        cur.execute("""insert into telephone_number (client_id, "number")
values ((select id from first_name_last_name_email
where email = %s), %s) RETURNING "number"; """, (mail, telephone))
        print(f'Телефон {cur.fetchone()} добавлен')
        conn.commit()
    conn.close()


# Функция, позволяющая изменить данные о клиенте(принимает на вход существующий email, новое имя, новую фамилию, новый
# email, новый телефон. Если какой либо параметр остаётся без изменений, то вписываем его уже существующее значение)
def change_data_client(mail_old, f_name_new, l_name_new, mail_new, telephone_new):
    conn = psycopg2.connect(database=Name_database, user=User_name, password=Password)
    with conn.cursor() as cur:
        cur.execute("""SELECT first_name, last_name, email, "number" FROM first_name_last_name_email
         LEFT JOIN telephone_number ON first_name_last_name_email.id = telephone_number.client_id 
         WHERE email = %s; """, (mail_old,))
        print(f'Данные клиента {cur.fetchone()} изменены на:')
        cur.execute("""SELECT client_id FROM telephone_number WHERE "number" = %s;""", (telephone_new,))
        if cur.fetchone():
            cur.execute("""UPDATE first_name_last_name_email SET first_name = %s, last_name = %s, email = %s 
            where email = %s;""", (f_name_new, l_name_new, mail_new, mail_old,))
            cur.execute("""SELECT first_name, last_name, email, "number" FROM first_name_last_name_email
                     LEFT JOIN telephone_number ON first_name_last_name_email.id = telephone_number.client_id 
                     WHERE email = %s; """, (mail_new,))
            print(f'               {cur.fetchone()}')
            conn.commit()
        else:
            cur.execute("""UPDATE first_name_last_name_email SET first_name = %s, last_name = %s, email = %s 
                       where email = %s;""", (f_name_new, l_name_new, mail_new, mail_old,))
            cur.execute("""UPDATE telephone_number SET "number" = %s 
            WHERE client_id = (select id from first_name_last_name_email
            where email = %s);""", (telephone_new, mail_new,))
            cur.execute("""SELECT first_name, last_name, email, "number" FROM first_name_last_name_email
                     LEFT JOIN telephone_number ON first_name_last_name_email.id = telephone_number.client_id 
                     WHERE email = %s; """, (mail_new,))
            print(f'               {cur.fetchone()}')
            conn.commit()
    conn.close()


# Функция, позволяющая удалить телефон для существующего клиента:
def del_telephone(telephone):
    conn = psycopg2.connect(database=Name_database, user=User_name, password=Password)
    with conn.cursor() as cur:
        cur.execute("""SELECT "number" FROM telephone_number WHERE "number" = %s;""", (telephone,))
        print(f'Телефон {cur.fetchone()} удалён')
        cur.execute("""DELETE FROM telephone_number WHERE "number" = %s;""", (telephone,))
        conn.commit()
    conn.close()


# Функция, позволяющая удалить существующего клиента:
def del_client(mail):
    conn = psycopg2.connect(database=Name_database, user=User_name, password=Password)
    with conn.cursor() as cur:
        cur.execute("""SELECT first_name, last_name, email FROM first_name_last_name_email
        WHERE email = %s;""", (mail,))
        print(f'Клиент {cur.fetchone()} удалён из базы данных')
        cur.execute("""DELETE FROM first_name_last_name_email WHERE id = (select id from first_name_last_name_email
            where email = %s);""", (mail,))
        conn.commit()
    conn.close()


# Функция, позволяющая найти клиента по его данным (имени, фамилии, email-у):
def search_fn_ln_mail(f_name, l_name, mail):
    conn = psycopg2.connect(database=Name_database, user=User_name, password=Password)
    with conn.cursor() as cur:
        cur.execute("""SELECT first_name, last_name, email, "number" FROM first_name_last_name_email
         LEFT JOIN telephone_number ON first_name_last_name_email.id = telephone_number.client_id 
         WHERE first_name = %s and last_name = %s and email = %s;""", (f_name, l_name, mail))
        pprint(cur.fetchall())
        conn.commit()
    conn.close()


# Функция, позволяющая найти клиента по его данным (телефону):
def search_tel(telephone):
    conn = psycopg2.connect(database=Name_database, user=User_name, password=Password)
    with conn.cursor() as cur:
        cur.execute("""SELECT first_name, last_name, email, "number" FROM first_name_last_name_email
         LEFT JOIN telephone_number ON first_name_last_name_email.id = telephone_number.client_id 
         WHERE "number" = %s;""", (telephone,))
        print(cur.fetchone())
        conn.commit()
    conn.close()


# Примеры:

# del_all_table()
#
# create_table()
#
# add_client('Дмитрий', 'Конев', 'dsfds@fdsf.ru')
# add_client('Алексей', 'Фомин', 'dad@,mn,m.ru')
# add_client('Павел', 'Коркин', 'poup@oiu.ru')
# add_client('Евгений', 'Петров', 'ewqae@mnb.ru')
# add_client('Андрей', 'Мартынов', 'nbvn@poiu.ru')
# add_client('Матвей', 'Петров', 'qwe@uyt.ru')
# add_client('Сергей', 'Васильев', 'poi@nmh.ru')
# add_client('Василий', 'Дронов', 'zxc@tyu.ru')
#
# add_telephone('dsfds@fdsf.ru', '89001245781')
# add_telephone('dad@,mn,m.ru', '89205698952')
# add_telephone('poup@oiu.ru', '89152365478')
# add_telephone('ewqae@mnb.ru', '89993652589')
# add_telephone('nbvn@poiu.ru', '89002154878')
# add_telephone('qwe@uyt.ru', '89102369889')
# add_telephone('poi@nmh.ru', '89007775858')
# add_telephone('zxc@tyu.ru', '89108975214')
# add_telephone('zxc@tyu.ru', '11111111111')
# add_telephone('zxc@tyu.ru', '22222222222')

# change_data_client('dsfds@fdsf.ru', 'Дмитрий', 'Конев', '123@123.ru', '44444444444')

# del_telephone('22222222222')

# del_client('123@123.ru')

# search_fn_ln_mail('Василий', 'Дронов', 'zxc@tyu.ru')

# search_tel('89007775858')
