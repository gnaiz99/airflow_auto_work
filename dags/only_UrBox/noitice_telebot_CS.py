from datetime import datetime

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.smtp.hooks.smtp import SmtpHook
from airflow_clickhouse_plugin.hooks.clickhouse_dbapi import ClickHouseDbApiHook

from plugins.helpers.telegram_bot import task_fail_telegram_alert

# a4_db = Variable.get("a4-db")
chatbot = Variable.get("chatbot")
a4_list_contract_ids = Variable.get("a4_list_contract_ids", deserialize_json=True)
a4_email_html_content = Variable.get("a4_email_html_content")


def send_email(
    date_from: datetime,
    date_to,
    contract: str,
    file_names: list[str],
) -> None:
    receivers = ["cskh@urbox.vn"]
    message_subject = f"Cảnh Báo Tổng Hợp: Hoạt Động Truy Cập Bot Hệ Thống Của Các Agent ngày {date_from} đến ngày {date_to}"
    with SmtpHook(smtp_conn_id="smtp_conn") as smtp_hook:
        return smtp_hook.send_email_smtp(
            to=receivers,
            cc=receivers,
            subject=message_subject,
            files=file_names,
            html_content=a4_email_html_content,
        )


@dag(
    schedule_interval="30 10 * * *",
    start_date=datetime(2025, 5, 10),
    max_active_runs=1,
    catchup=False,
    default_args={"owner": "Data-warehouse"},
    on_failure_callback=task_fail_telegram_alert
    # ,tags=["a4", "urcontract", "reconciliation"],
)
def a4_send_data():
    @task()
    def get_dict_send_email(execution_date=None):
        hook = MySqlHook(mysql_conn_id="mysql_config", schema=chatbot)
        query_get_list_reconciliation_schedule = f"""
            SELECT rs.id as reconciliation_schedule_id,
                rs.contract_id as contract_id,
                c.contract_code as contract_code,
                case when rs.start_at = 0 then null else from_unixtime(rs.start_at) + interval 7 HOUR end     as start_at,
                case when rs.end_at = 0 then null else from_unixtime(rs.end_at) + interval 7 HOUR end         as end_at
            FROM reconciliation_schedule rs
            left join contract c on rs.contract_id = c.id
            WHERE date((from_unixtime(rs.end_at) + interval 7 HOUR)) = '{execution_date.strftime("%Y-%m-%d")}'
            AND rs.status = 2 and rs.contract_id in ({','.join([str(i) for i in a4_list_contract_ids])})
        """

        query_get_list_discount_schedule = f"""
            SELECT ds.id as discount_schedule_id,
                ds.contract_id as contract_id,
                c.contract_code as contract_code,
                case when ds.start_at = 0 then null else from_unixtime(ds.start_at) + interval 7 HOUR end     as start_at,
                case when ds.end_at = 0 then null else from_unixtime(ds.end_at) + interval 7 HOUR end         as end_at
            FROM discount_schedule ds
            left join contract c on ds.contract_id = c.id
            WHERE date((from_unixtime(ds.end_at) + interval 7 HOUR)) = '{execution_date.strftime("%Y-%m-%d")}'
            AND ds.status = 2 and ds.contract_id in ({','.join([str(i) for i in a4_list_contract_ids])})
        """

        df_reconciliation_info = hook.get_pandas_df(
            sql=query_get_list_reconciliation_schedule
        )

        # had discount schedule due in this run
        df_discount_info = hook.get_pandas_df(sql=query_get_list_discount_schedule)

        if len(df_reconciliation_info) > 0:
            query_get_reconciliation_group = """
                select id as reconciliation_group_id, title as reconciliation_group_title
                from reconciliation_group
            """
            df_reconciliation_group_info = hook.get_pandas_df(
                sql=query_get_reconciliation_group
            )
            reconciliation_schedule_ids = df_reconciliation_info[
                "reconciliation_schedule_id"
            ].to_list()
            reconciliation_schedule_ids = ",".join(
                str(i) for i in reconciliation_schedule_ids
            )
            reconciliation_schedule_ids = "(" + reconciliation_schedule_ids + ")"
            clickhouse_hook = ClickHouseDbApiHook(
                clickhouse_conn_id="clickhouse_conn", schema="urcontract"
            )
            df = clickhouse_hook.get_pandas_df(
                sql=f"""
                select *
                from urcontract.discount_transaction
                where reconciliation_schedule_id in {reconciliation_schedule_ids}
            """
            )
            df = df.merge(
                df_reconciliation_group_info, on=["reconciliation_group_id"], how="left"
            )
            for _, i in df_reconciliation_info.iterrows():
                attach_file_names = []

                print(df.columns)
                print(i["reconciliation_schedule_id"])
                file_name = f"data_{i['contract_id']}.xlsx"
                df.loc[
                    (
                        df["reconciliation_schedule_id"]
                        == i["reconciliation_schedule_id"]
                    )
                    & (df["contract_id"] == i["contract_id"])
                ].to_excel(file_name, index=False)
                attach_file_names.append(file_name)

                list_contract_due_discount_schedule = df_discount_info[
                    "contract_id"
                ].to_list()
                # if there is a discount schedule of the contract due in this run
                if i["contract_id"] in list_contract_due_discount_schedule:
                    full_schedule_file_name = f"data_{i['contract_id']}_full.xlsx"
                    discount_schedule_id = df_discount_info[
                        df_discount_info["contract_id"] == i["contract_id"]
                    ]["discount_schedule_id"].to_list()[0]

                    df = clickhouse_hook.get_pandas_df(
                        sql=f"""
                        select *
                        from urcontract.discount_transaction
                        where discount_schedule_id = {discount_schedule_id}
                        and deleted_at is null
                    """
                    )

                    query_get_reconciliation_group = """
                            select
                                id as reconciliation_group_id,
                                title as reconciliation_group_title,
                                subject reconciliation_group_subject
                            from reconciliation_group
                        """
                    df_reconciliation_group_info = hook.get_pandas_df(
                        sql=query_get_reconciliation_group
                    )

                    df = df.merge(
                        df_reconciliation_group_info,
                        on=["reconciliation_group_id"],
                        how="left",
                    )

                    df = df.drop_duplicates(
                        ["code_using", "brand_office_id", "reconciliation_group_id"],
                        keep="last",
                    )
                    df.to_excel(full_schedule_file_name, index=False)

                    attach_file_names.append(full_schedule_file_name)

                return send_email(
                    date_from=i["start_at"],
                    date_to=i["end_at"],
                    contract=i["contract_code"],
                    file_names=attach_file_names,
                )
        else:
            return []

    get_dict_send_email()


dag = a4_send_data()