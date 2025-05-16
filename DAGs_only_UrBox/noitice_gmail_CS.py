import smtplib
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow_clickhouse_plugin.operators.clickhouse_dbapi import ClickHouseDbApiHook

from plugins.helpers.telegram_bot import task_fail_telegram_alert

# Email
EMAIL_SENDER_NAME = "Team Data"
EMAIL_SENDER_ADDRESS = Variable.get("EMAIL_SENDER_ADDRESS")
EMAIL_SENDER_PASSWORD = Variable.get("EMAIL_SENDER_PASSWORD")

body_mail = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Title</title>
</head>
<body>
<p style="color:black;">Dear team CSKH,</p>
<p style="color:black;">Data gửi thông tin số lượng check bot ngày {day}: {count}</p>
{data_frame_html}
<p style="color:black;"><i>Chi tiết trong file đính kèm.</i></p>
<p><br></p>
<p style="color:rgb(102, 35, 179);"><b> URBOX - DIGITAL REWARD AND LOYALTY SOLUTION</b></p>
<p style="color:rgb(102, 35, 179);"><b>Thanks & Best Regard</b></p>
<p>--------------------------</p>
<p><a href="https://urbox.vn/">Website</a> | <a href="https://www.facebook.com/Urbox.vn">Facebook</a> | <a href="https://www.linkedin.com/company/urboxvn/">Linkedin</a></p>
<p>Address: Ha Noi Office: 4th Floor, GP Invest Building, 170 De La Thanh, Dong Da District, Ha Noi.</p>
<p>Address: Ho Chi Minh Office: 10th Floor, Blue Sky Building, 1 Bach Dang, Ward 2, Tan Binh District, Ho Chi Minh.</p>
</body>
</html>"""


def get_data_raw():
    current_date = datetime.now()
    previous_date = current_date - timedelta(days=1)
    # first_day_of_current_month = current_date.replace(day=1)
    # last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)
    # first_day_of_previous_month = last_day_of_previous_month.replace(day=1)

    raw_data = f"""SELECT u.email as email_agent,
   			m.conversation_id as conversation_id,
   			m.content as content,
   			m.created_at as created_at,
   			m.is_answerable as is_answerable 
   FROM chatbot.message m 
   left join chatbot.conversations c on m.conversation_id = c.id 
   left join chatbot.users u on c.user_id = u.id 
   where 1=1
   and match(LOWER(m.content) , 'lg|urcard|urgift')
   AND m.role = 'user'
   and toDate(m.created_at) between '{current_date.date()}' and '{previous_date.date()}'
   """

    pivot_data = f"""SELECT u.email as email_agent,
   			count(*) as so_luot_hoi_bot 
   FROM chatbot.message m 
   left join chatbot.conversations c on m.conversation_id = c.id 
   left join chatbot.users u on c.user_id = u.id 
   where 1=1
   and match(LOWER(m.content) , 'lg|urcard|urgift')
   AND m.role = 'user'
   and toDate(m.created_at) between '{current_date.date()}' and '{previous_date.date()}'
   group by u.email 
   order by count(*) desc
   """

    count_ask = f"""count(*) as so_luot_hoi_bot 
   FROM chatbot.message m 
   left join chatbot.conversations c on m.conversation_id = c.id 
   left join chatbot.users u on c.user_id = u.id 
   where 1=1
   and match(LOWER(m.content) , 'lg|urcard|urgift')
   AND m.role = 'user'
   and toDate(m.created_at) between '{current_date.date()}' and '{previous_date.date()}'
   """

    list_agent_ask_bot = ClickHouseDbApiHook(
        clickhouse_conn_id="clickhouse_conn_da", schema="chatbot"
    ).get_pandas_df(sql=pivot_data)

    return list_agent_ask_bot


def send_mail(receiver, subject, content):
    try:
        message = MIMEMultipart()
        message["From"] = f"{EMAIL_SENDER_NAME} <{EMAIL_SENDER_ADDRESS}>"
        message["To"] = ",".join(receiver)
        message["Subject"] = subject
        message.attach(MIMEText(content, "html"))
        session = smtplib.SMTP("smtp.gmail.com", 587)
        session.starttls()
        session.login(EMAIL_SENDER_ADDRESS, EMAIL_SENDER_PASSWORD)
        text = message.as_string()
        session.sendmail(EMAIL_SENDER_ADDRESS, receiver, text)
        session.quit()
        print("Mail Sent!!!")
    except Exception as e:
        print(e)


@dag(
    schedule_interval="30 10 * * *",
    start_date=datetime(2025, 5, 15),
    default_args={
        "retries": 3,
        # "retry_delay": timedelta(minutes=5),
        "owner": "Data-warehouse",
    },
    on_failure_callback=task_fail_telegram_alert,
    catchup=False,
    # tags=["hn", "sending_mail", "mail", "sms", "data_team"],
)
def send_email_list_app_charge_fee():
    @task()
    def send_email():
        # last_month = (datetime.now() - timedelta(days=10)).month
        last_day = (datetime.now() - timedelta(days=1)).day
        list_agent_ask_bot = get_data_raw()


        html_table = list_agent_ask_bot.to_html(index=False)
        CONTENT = body_mail.format(day=last_day, data_frame_html=html_table)
        RECEIVER = [
            "cskh@urbox.vn"
        ]
        SUBJECT = f"""Cảnh Báo Tổng Hợp: Hoạt Động Truy Cập Bot Hệ Thống Của Các Agent ngày {last_day}"""

        send_mail(subject=SUBJECT, content=CONTENT, receiver=RECEIVER)

    send_email()


dag = send_email_list_app_charge_fee()