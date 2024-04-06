# dags/churn.py

import pendulum
from airflow.decorators import dag, task

@dag(
    schedule='@once',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["ETL"]
)
def prepare_churn_dataset():
    import pandas as pd
    import numpy as np
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    import sqlalchemy

    @task()
    def create_table():
        from sqlalchemy import inspect
        from sqlalchemy import MetaData, Table, Column, String, Integer, DateTime, Float, UniqueConstraint
   
        hook = PostgresHook('destination_db')
        engine = hook.get_sqlalchemy_engine()
        meta = MetaData()
        users_churn = Table('users_churn', meta,
            Column('id', Integer, primary_key=True),
            Column('customer_id', String(60)),
            UniqueConstraint('customer_id', name='unique_customer_id_constraint'),
            Column('begin_date', DateTime),
            Column('end_date', DateTime),
            Column('type', String(60)),
            Column('paperless_billing', String(60)),
            Column('payment_method', String(60)),
            Column('monthly_charges', Float),
            Column('total_charges', Float),
            Column('internet_service', String(60)),
            Column('online_security', String(60)),
            Column('online_backup', String(60)),
            Column('device_protection', String(60)),
            Column('tech_support', String(60)),
            Column('streaming_tv', String(60)),
            Column('streaming_movies', String(60)),
            Column('gender', String(60)),
            Column('senior_citizen', Integer),
            Column('partner', String(60)),
            Column('dependents', String(60)),
            Column('multiple_lines', String(60)),
            Column('target', Integer),
        )
        if not inspect(engine).has_table(users_churn.name): 
            meta.create_all(engine) 
            
        
    @task()
    def extract(**kwargs):

        hook = PostgresHook('source_db')
        conn = hook.get_conn()
        sql = f"""
        select
            c.customer_id, c.begin_date, c.end_date, c.type, c.paperless_billing, c.payment_method, c.monthly_charges, c.total_charges,
            i.internet_service, i.online_security, i.online_backup, i.device_protection, i.tech_support, i.streaming_tv, i.streaming_movies,
            p.gender, p.senior_citizen, p.partner, p.dependents,
            ph.multiple_lines
        from contracts as c
        left join internet as i on i.customer_id = c.customer_id
        left join personal as p on p.customer_id = c.customer_id
        left join phone as ph on ph.customer_id = c.customer_id
        """
        data = pd.read_sql(sql, conn)
        conn.close()
        return data

    @task()
    def transform(data: pd.DataFrame):
        data['target'] = (data['end_date'] != 'No').astype(int)
        data['end_date'].replace({'No': None}, inplace=True)
        return data
    
    @task()
    def load(data: pd.DataFrame):
        hook = PostgresHook('destination_db')
        hook.insert_rows(
            table="users_churn",
            replace=True,
            target_fields=data.columns.tolist(),
            replace_index=['customer_id'],
            rows=data.values.tolist()
    )

    create_table()
    data = extract()
    transformed_data = transform(data)
    load(transformed_data)
    

prepare_churn_dataset()