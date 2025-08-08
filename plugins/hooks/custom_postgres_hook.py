from airflow.hooks.base import BaseHook
import pandas as pd
from sqlalchemy import create_engine


class CustomPostgresHook(BaseHook):

    def __init__(self, postgres_conn_id, **kwargs):
        super().__init__(**kwargs)
        self.postgres_conn_id = postgres_conn_id
        conn = BaseHook.get_connection(self.postgres_conn_id)
        self.uri = f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}'

    def bulk_load(self, table_name, file_name, delimiter: str, is_header: bool, is_replace: bool):
        self.log.info('적재 대상파일:' + file_name)
        self.log.info('테이블 :' + table_name)

        header = 0 if is_header else None
        if_exists = 'replace' if is_replace else 'append'

        file_df = pd.read_csv(file_name, header=header, delimiter=delimiter)

        for col in file_df.columns:
            if file_df[col].dtype == 'object':
                file_df[col] = file_df[col].astype(str).str.replace('\r\n', '', regex=True)
                self.log.info(f'{table_name}.{col}: 개행문자 제거')

        self.log.info('적재 건수:' + str(len(file_df)))

        # SQLAlchemy Engine 생성
        engine = create_engine(self.uri)

        # 'con' 인수에 engine 객체 전달
        file_df.to_sql(name=table_name,
                       con=engine,
                       schema='public',
                       if_exists=if_exists,
                       index=False,
                       )