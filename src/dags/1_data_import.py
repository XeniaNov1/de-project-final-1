from logging import Logger
from pydantic import BaseModel
from psycopg.rows import class_row
from vertica_python import Connection as VerticaConnection
import pendulum
from datetime import datetime
from lib.db_connect import create_pg_connection, create_vertica_connection
import contants

from lib.dict_util import json2str
from setting_repository import StgEtlSettingsRepository, EtlSetting
import logging
from typing import Any, List

from airflow.decorators import dag, task

log = logging.getLogger(__name__)


class TransactionDto(BaseModel):
    operation_id: str
    account_number_from: int
    account_number_to: int
    currency_code: int
    country: str
    status: str
    transaction_type: str
    amount: int
    transaction_dt: datetime


class CurrencyDto(BaseModel):
    currency_code: int
    currency_code_with: int
    currency_with_div: float
    date_update: datetime


class TransactionRepositoryService:
    def __init__(self, pg: Any) -> None:
        self._db = pg

    def list_trans(self, threshold: datetime) -> List[TransactionDto]:
        with self._db.cursor(row_factory=class_row(TransactionDto)) as cur:
            cur.execute(
                """
                    SELECT
                    operation_id,
                    account_number_from,
                    account_number_to,
                    currency_code,
                    country,
                    status,
                    transaction_type,
                    amount,
                    transaction_dt
                    from public.transactions
                    WHERE transaction_dt > %(threshold)s
                    ORDER BY transaction_dt ASC
                """, {
                    "threshold": threshold
                }
            )
            objs = cur.fetchall()
        return objs


class TransactionInsertService:
    def insert_trans(
            self,
            conn: VerticaConnection,
            trans: List[TransactionDto]
    ):
        with conn.cursor() as cur:
            params_seq = [
                (
                    tran.operation_id,
                    tran.account_number_from,
                    tran.account_number_to,
                    tran.currency_code,
                    tran.country,
                    tran.status,
                    tran.transaction_type,
                    tran.amount,
                    tran.transaction_dt
                ) for tran in trans
            ]

            cur.executemany(
                """INSERT INTO STV202506270__STAGING.transactions (
                    operation_id,
                    account_number_from,
                    account_number_to,
                    currency_code,country,
                    status,
                    transaction_type,
                    amount,
                    transaction_dt
                )
                values (%s, %s, %s, %s, %s, %s, %s, %s, %s);""",
                params_seq,
                use_prepared_statements=False,
            )
        conn.commit()


class CurrencyRepositoryService:
    def __init__(self, pg: Any) -> None:
        self._db = pg

    def list_currencies(self, threshold: datetime) -> List[CurrencyDto]:
        with self._db.cursor(row_factory=class_row(CurrencyDto)) as cur:
            cur.execute(
                """
                    SELECT
                    currency_code,
                    currency_code_with,
                    currency_with_div,
                    date_update
                    from currencies
                    WHERE date_update > %(threshold)s
                    ORDER BY date_update ASC
                """, {
                    "threshold": threshold.strftime("%Y-%m-%d")
                }
            )
            objs = cur.fetchall()
        return objs


class CurrencyInsertService:
    def __init__(self, log: Logger):
        self.log = log

    def insert_currencies(
        self,
        conn: VerticaConnection,
        currs: List[CurrencyDto]
    ):
        with conn.cursor() as cur:
            params_seq = [
                (
                    curr.currency_code,
                    curr.currency_code_with,
                    curr.currency_with_div,
                    curr.date_update
                ) for curr in currs
            ]

            self.log.info(f"trying to insert: {params_seq}")

            cur.executemany(
                """INSERT INTO STV202506270__STAGING.currencies (
                    currency_code,
                    currency_code_with,
                    currency_code_div,
                    date_update
                ) VALUES (%s,%s,%s,%s);""",
                params_seq,
                use_prepared_statements=False
            )

            conn.commit()


class TransactionLoaderService:
    WF_KEY = "transactions_load"
    LAST_LOADED_ID_KEY = "last_loaded_id"

    def __init__(self, pg: Any, vr: VerticaConnection, log: Logger) -> None:
        self.pg = pg
        self.origin = TransactionRepositoryService(pg)
        self.stg = TransactionInsertService()
        self.vr = vr
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_trans(self):
        wf_setting = self.settings_repository.get_setting(
            self.pg, self.WF_KEY)
        if not wf_setting:
            wf_setting = EtlSetting(
                id=0,
                workflow_key=self.WF_KEY,
                workflow_settings={
                    self.LAST_LOADED_ID_KEY: datetime.fromtimestamp(0)
                }
            )

        last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
        load_queue = self.origin.list_trans(last_loaded)
        self.log.info(f"Found {len(load_queue)} new data to load.")
        if not load_queue:
            self.log.info("Quitting.")
            return

        self.stg.insert_trans(self.vr, load_queue)

        wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max(
            [t.operation_id for t in load_queue])
        wf_setting_json = json2str(wf_setting.workflow_settings)
        self.settings_repository.save_setting(
            self.pg, wf_setting.workflow_key, wf_setting_json)

        last_id = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
        self.log.info(
            f"Load finished on {last_id}"
        )


class CurrencyLoaderService:
    WF_KEY = "currency_load"
    LAST_LOADED_ID_KEY = "last_loaded_id"

    def __init__(self, pg: Any, vr: VerticaConnection, log: Logger) -> None:
        self.pg = pg
        self.origin = CurrencyRepositoryService(pg)
        self.stg = CurrencyInsertService(log)
        self.vr = vr
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_currency(self):
        wf_setting = self.settings_repository.get_setting(
            self.pg, self.WF_KEY)
        if not wf_setting:
            wf_setting = EtlSetting(
                id=0,
                workflow_key=self.WF_KEY, workflow_settings={
                    self.LAST_LOADED_ID_KEY: datetime.fromtimestamp(0)
                }
            )
        last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
        load_queue = self.origin.list_currencies(last_loaded)
        self.log.info(f"Found {len(load_queue)} new data to load.")
        if not load_queue:
            self.log.info("Quitting.")
            return

        self.stg.insert_currencies(self.vr, load_queue)

        wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max(
            [t.date_update for t in load_queue])
        wf_setting_json = json2str(wf_setting.workflow_settings)
        self.settings_repository.save_setting(
            self.pg, wf_setting.workflow_key, wf_setting_json)

        last_id = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
        self.log.info(
            f"Load finished on {last_id}"
        )


@dag(
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprintf', 'ddl'],
    is_paused_upon_creation=True
)
def sprint_import_dag():
    dwh_pg_connect = create_pg_connection(contants.PG_CONNECTION_ID)
    dwh_vr_connect = create_vertica_connection(contants.VERTICA_CONNECTION_ID)

    @task(task_id="transaction_load")
    def task1():
        rest_loader = TransactionLoaderService(
            dwh_pg_connect, dwh_vr_connect, log)
        rest_loader.load_trans()

    @task(task_id="currency_load")
    def task2():
        rest_loader = CurrencyLoaderService(
            dwh_pg_connect,
            dwh_vr_connect,
            log
        )
        rest_loader.load_currency()
    task1()
    task2()


dag = sprint_import_dag()
dag
