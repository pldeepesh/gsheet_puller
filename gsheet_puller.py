import pandas as pd
import numpy as np
from psycopg2 import connect
import pygsheets
import json
import io
from sqlalchemy import create_engine

GSHEET_NAME = 'Orders - Toothsi'
creds = json.loads(open('config.json','r').read())

def get_engine():
    #Db creds
    postgres_creds = creds['postgres']

    DATABASE = postgres_creds['DATABASE']
    USER_NAME = postgres_creds['USER']
    PASSWORD = postgres_creds['PASS']
    HOST = postgres_creds['HOST']

    engine = create_engine(f'postgresql+psycopg2://{USER_NAME}:{PASSWORD}@{HOST}:5432/{DATABASE}')
    return engine




try:
    postgres_config = creds['postgres']

    DATABASE = postgres_config['DATABASE']
    HOST = postgres_config['HOST']
    USER_NAME = postgres_config['USER'] 
    PASSWORD = postgres_config['PASS']

    psycop_conn = connect(database=DATABASE,
    user=USER_NAME,
    password=PASSWORD,
    host=HOST)
    cursor = psycop_conn.cursor()

    print('connected to the DB')
except:
    print('Not connected')

def read_gsheet_data():
    gc = pygsheets.authorize(service_file='creds.json')
    sh = gc.open(GSHEET_NAME)
    wks = sh.worksheet_by_title('Orders')
    data = wks.get_as_df()
    return data

def push_data(df):
    conn = get_engine()
    # cursor = conn.cursor()

    df['created_at'] = pd.to_datetime(df["created_at"]).dt.strftime("%Y-%m-%d %H:%M:%S.%f")
    df['mobile_no'] = df['mobile_no'].astype(str)
    df['modified_at'] = pd.datetime.now()

    try:
        q = 'drop table temp_orders;'
        conn.execute(q)
        print('dropped temp orders table ...')

        q = 'create table temp_orders as (select * from orders limit 0)'
        conn.execute(q)
        print('created temp_orders table ...')
    except:
        q = 'create table temp_orders as (select * from orders limit 0)'
        conn.execute(q)
        print('created temp_orders table ...')


    # pushing data into temp_orders table
    raw_conn = conn.raw_connection()
    cur = raw_conn.cursor()
    output = io.StringIO()
    df.to_csv(output, sep='\t', header=False, index=False)
    output.seek(0)
    contents = output.getvalue()
    cur.copy_from(output, 'temp_orders') # null values become ''
    raw_conn.commit()

    # deleteing removed ids
    q = '''delete from orders where id not in (select id from temp_orders);'''
    cursor.execute(q)
    psycop_conn.commit()
    print('deleted from orders table')

    q = '''WITH DATA AS
            (SELECT DISTINCT t.id AS id
            FROM temp_orders AS t
            LEFT JOIN orders o ON o.id = t.id
            WHERE (t.first_name<>o.first_name)
                OR (t.last_name<>o.last_name)
                OR (t.mobile_no<>o.mobile_no)
                OR (t.created_at<>o.created_at)
                OR (o.id IS NULL)
                )
            DELETE
            FROM orders
            WHERE id IN
                (SELECT id
                FROM DATA);'''

    cursor.execute(q)
    psycop_conn.commit()
    print('dropped modified columns from orders')

    q='''WITH DATA AS
        (SELECT t.*
        FROM temp_orders AS t
        LEFT JOIN orders o ON o.id = t.id
        WHERE (t.first_name<>o.first_name)
            OR (t.last_name<>o.last_name)
            OR (t.mobile_no<>o.mobile_no)
            OR (t.created_at<>o.created_at)
            OR (o.id IS NULL))
        insert into orders (SELECT *
        FROM DATA);'''
    
    cursor.execute(q)
    psycop_conn.commit()

    # final_data_to_be_pushed = pd.read_sql(q,con=conn)
    # print(final_data_to_be_pushed.head())


    # output = io.StringIO()
    # final_data_to_be_pushed.to_csv(output, sep='\t', header=False, index=False)
    # output.seek(0)
    # cur.copy_from(output, 'orders') 
    # raw_conn.commit()

    print('data pushed into orders')

    q = '''drop table temp_orders;'''
    conn.execute(q)
    print('dropped the temp table')


data = read_gsheet_data()
print(data.head())

data = push_data(data)


