import logging
from airflow.decorators import dag, task
import pendulum
from vertica_python import Connection
from lib.db_connect import create_vertica_connection
from contants import VERTICA_CONNECTION_ID

log = logging.getLogger(__name__)

class CdmInsertService:
    def __init__(self, vr: Connection):
        self.vr = vr

    def load_datamart(self):
        with self.vr.cursor() as cur:
            cur.execute("""
                SELECT t.transaction_dt AS date_update, --дата расчёта
                currency_code AS currency_from, -- код валюты транзакции
                sum(amount) AS amount_total, -- общая сумма транзакций по валюте в долларах;
                count(amount) AS cnt_transactions, -- общий объём транзакций по валюте;
                avg(avg_trans.avg_amount) AS avg_transactions_per_account, -- средний объём транзакций с аккаунта;
                sum(acc_count.acc_count) AS cnt_accounts_make_transactions -- количество уникальных аккаунтов с совершёнными транзакциями по валюте

                FROM STV202506270__STAGING.transactions t
                LEFT JOIN
                (WITH acc1 AS
                    (SELECT DISTINCT account_number_from AS acc1,
                                    transaction_dt
                    FROM STV202506270__STAGING.transactions
                    UNION ALL SELECT DISTINCT account_number_to AS acc2,
                                                transaction_dt
                    FROM STV202506270__STAGING.transactions) SELECT transaction_dt,
                                                                    count(DISTINCT acc1) AS acc_count
                FROM acc1
                GROUP BY transaction_dt) AS acc_count ON t.transaction_dt = acc_count.transaction_dt
                LEFT JOIN
                (SELECT transaction_dt,
                        avg(amount) OVER (PARTITION BY transaction_dt, account_number_from) AS avg_amount
                FROM STV202506270__STAGING.transactions) AS avg_trans ON t.transaction_dt = avg_trans.transaction_dt,

                (SELECT max(load_dttm)
                FROM STV202506270__DWH.latest_load_dates
                LIMIT 1) lld
                GROUP BY currency_code,
                        t.transaction_dt
            """)

            response = cur.fetchall()

            cur.executemany("""INSERT INTO STV202506270__DWH.global_metrics (
                date_update,
                currency_from,
                amount_total,
                cnt_transactions,
                avg_transactions_per_account,
                cnt_accounts_make_transactions)
                values(%s, %s,  %s,  %s,  %s,  %s);
            """, response, use_prepared_statements=False)

            cur.execute("""
                select
                    greatest(
                    coalesce(max(date_update), now()))
                from STV202506270__DWH.global_metrics
            """)

            load_dttm = cur.fetchall()
            cur.executemany("""INSERT INTO STV202506270__DWH.latest_load_dates (load_dttm)
                            values (%s);""", load_dttm, use_prepared_statements=False)
        self.vr.commit()


@dag(
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprintf', 'ddl'],
    is_paused_upon_creation=True
)
def sprint_update_dag():
    dwh_vr_connect = create_vertica_connection(VERTICA_CONNECTION_ID)

    @task(task_id="cdm_load")
    def load_cdm():
        rest_loader = CdmInsertService(dwh_vr_connect)
        rest_loader.load_datamart()

    load_cdm()


dag = sprint_update_dag()
dag
