from airflow.hooks.base import BaseHook
from psycopg import connect as pg_connect
from vertica_python import connect as vertica_connect


def create_pg_connection(conn_id: str):
    config = BaseHook.get_connection(conn_id)

    pg_config = {
        'host': str(config.host),
        'port': str(config.port),
        'dbname': str(config.schema),
        'user': str(config.login),
        'password': str(config.password),
    }

    return pg_connect(**pg_config)


def create_vertica_connection(conn_id: str):
    config = BaseHook.get_connection(conn_id)

    vertica_config = {
        'host': str(config.host),
        'port': str(config.port),
        'database': str(config.schema),
        'user': str(config.login),
        'password': str(config.password),
    }

    return vertica_connect(**vertica_config)
