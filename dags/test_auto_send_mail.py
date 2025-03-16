from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.email import send_email
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd

# Google Sheets API credentials
GOOGLE_SHEET_URL = "https://docs.google.com/spreadsheets/d/19AtCkKfcXNQp8d1hjPEK6wfGZhJv6cKxLI6YzP7cyQc/edit?gid=0"
GOOGLE_CREDENTIALS_FILE = "/path/to/your/credentials.json"

# Email settings
EMAIL_SUBJECT = "Thông báo kết quả"

# Function to get emails from Google Sheets
def get_emails():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_CREDENTIALS_FILE, scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_url(GOOGLE_SHEET_URL).sheet1
    data = sheet.get_all_records()
    return pd.DataFrame(data)

# Function to send emails
def send_email_task():
    df = get_emails()
    for _, row in df.iterrows():
        email = row["email"]
        level = row["level"]
        message = "Thành công" if level == 1 else "Thất bại"
        send_email(to=email, subject=EMAIL_SUBJECT, html_content=f"<p>{message}</p>")

# Define DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "retries": 1,
}

dag = DAG(
    "send_emails_from_gsheet",
    default_args=default_args,
    schedule_interval="@monthly",
    catchup=False,
)

send_email_op = PythonOperator(
    task_id="send_emails",
    python_callable=send_email_task,
    dag=dag,
)
