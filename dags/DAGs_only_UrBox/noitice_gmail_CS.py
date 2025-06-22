from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.smtp.hooks.smtp import SmtpHook
from datetime import datetime
from email.mime.text import MIMEText


def send_email():
    hook = SmtpHook(smtp_conn_id="smtp_gmail_conn")  # Kết nối đã cấu hình trong UI

    msg = MIMEText("Đây là email gửi từ DAG Airflow sử dụng SmtpHook.")
    msg["Subject"] = "Test Email từ Airflow"
    msg["From"] = hook.conn.login
    msg["To"] = "giangntg2301@gmail.com"

    hook.send_email(
        to=msg["To"],
        subject=msg["Subject"],
        html_content=msg.as_string(),  # hoặc chỉ text nếu không cần HTML
        mime_subtype='plain'
    )


with DAG(
        dag_id="send_email_via_smtphook",
        schedule_interval=None,
        start_date=datetime(2024, 1, 1),
        catchup=False,
        tags=["email", "smtp"],
) as dag:
    task_send_email = PythonOperator(
        task_id="send_email_task",
        python_callable=send_email,
    )
    task_send_email