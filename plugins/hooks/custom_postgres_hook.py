from airflow.hooks.base import BaseHook
import pandas as pd
from sqlalchemy import create_engine


class CustomPostgresHook(BaseHook):

    def __init__(self, postgres_conn_id, **kwargs):
        super().__init__(**kwargs)
        self.postgres_conn_id = postgres_conn_id
        # 에어플로우 커넥션을 가져옵니다.
        self.airflow_conn = BaseHook.get_connection(self.postgres_conn_id)

        self.host = self.airflow_conn.host
        self.user = self.airflow_conn.login
        self.password = self.airflow_conn.password
        self.dbname = self.airflow_conn.schema
        self.port = self.airflow_conn.port

        # SQLAlchemy URI를 생성합니다.
        self.uri = f'postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}'

    # get_conn() 메서드를 삭제합니다.

    def get_sqlalchemy_engine(self):
        # SQLAlchemy Engine 객체를 반환하는 메서드입니다.
        return create_engine(self.uri)

    def bulk_load(self, table_name, file_name, delimiter: str, is_header: bool, is_replace: bool):
        self.log.info('적재 대상파일:' + file_name)
        self.log.info('테이블 :' + table_name)

        header = 0 if is_header else None
        if_exists = 'replace' if is_replace else 'append'

        # CSV 파일을 pandas DataFrame으로 읽어옵니다.
        try:
            file_df = pd.read_csv(file_name, header=header, delimiter=delimiter, encoding='utf-8')
        except UnicodeDecodeError:
            file_df = pd.read_csv(file_name, header=header, delimiter=delimiter, encoding='cp949')

        for col in file_df.columns:
            if file_df[col].dtype == 'object':
                # 문자열 열의 개행 문자, 탭 등을 제거합니다.
                file_df[col] = file_df[col].astype(str).str.replace('\r\n','')
                self.log.info(f'{table_name}.{col}: 개행문자, 탭 제거')

        self.log.info('적재 건수:' + str(len(file_df)))

        # SQLAlchemy Engine 객체를 생성합니다.
        engine = self.get_sqlalchemy_engine()

        # pandas.to_sql에 engine 객체를 전달하여 데이터를 적재합니다.
        file_df.to_sql(name=table_name,
                       con=engine,
                       schema='public',
                       if_exists=if_exists,
                       index=False,
                       chunksize=1000,
                       method='multi'
                       )