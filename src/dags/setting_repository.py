from typing import Dict, Optional, Any

from psycopg.rows import class_row
from pydantic import BaseModel


class EtlSetting(BaseModel):
    id: int
    workflow_key: str
    workflow_settings: Dict


class StgEtlSettingsRepository:
    def get_setting(self, conn: Any, etl_key: str) -> Optional[EtlSetting]:
        with conn.cursor(row_factory=class_row(EtlSetting)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        workflow_key,
                        workflow_settings
                    FROM public.srv_wf_settings
                    WHERE workflow_key = %(etl_key)s;
                """,
                {"etl_key": etl_key},
            )
            obj = cur.fetchone()

        return obj

    def save_setting(self, conn: Any, workflow_key: str, workflow_settings: str) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO public.srv_wf_settings(workflow_key, workflow_settings)
                    VALUES (%(etl_key)s, %(etl_setting)s)
                    ON CONFLICT (workflow_key) DO UPDATE
                    SET workflow_settings = EXCLUDED.workflow_settings;
                """,
                {
                    "etl_key": workflow_key,
                    "etl_setting": workflow_settings
                },
            )
