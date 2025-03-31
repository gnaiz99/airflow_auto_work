import gspread
import pandas as pd
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


#1. Kết nối API - đọc file ggsheet
# Đọc file API kết nối ggsheet
gs = gspread.service_account(r'C:\Users\Admin\Downloads\test-connect-ggsheet-454216-7e9f4c02bdda.json') #Địa chỉ file test-connect-ggsheet trong máy local
# gs = gspread.service_account(r"C:\Users\giang.nt\Downloads\file hoàn thành\Py\test-connect-ggsheet-454216-7e9f4c02bdda.json") #Địa chỉ file trong máy cty
#Kết nối bằng key trong url
list_mail = gs.open_by_key('19AtCkKfcXNQp8d1hjPEK6wfGZhJv6cKxLI6YzP7cyQc').sheet1 #Lấy sheet đầu tiên
list_mail = list_mail.get_all_records()

# role_level = gs.open_by_key('19AtCkKfcXNQp8d1hjPEK6wfGZhJv6cKxLI6YzP7cyQc').get_worksheet(1) #Lấy sheet thứ 2 - C1: .get_worksheet(n) lấy sheet thứ n+1
role_level = gs.open_by_key('19AtCkKfcXNQp8d1hjPEK6wfGZhJv6cKxLI6YzP7cyQc').worksheet("roles") #C2 lấy sheet theo tên
role_level = role_level.get_all_records()

#Data ở dạng dict
df_list_mail = pd.DataFrame(list_mail)
df_role = pd.DataFrame(role_level)
df_info = df_list_mail.merge(df_role, on="id", how="left")
# conect done #

#2. gửi mail theo level được phân

# Cấu hình SMTP Server (Dùng Gmail)
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
EMAIL_SENDER = "giangntg2301@gmail.com"
#Phải dùng mật khẩu ứng dụng để đăng nhập vào mail
#https://myaccount.google.com/apppasswords link lấy mk ứng dụng
EMAIL_PASSWORD = "hotb aynh lagg crzo"

# Hàm gửi email
def send_email(to_email, subject, body):
    msg = MIMEMultipart()
    msg["From"] = EMAIL_SENDER
    msg["To"] = to_email
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

    try:
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()
        server.login(EMAIL_SENDER, EMAIL_PASSWORD)
        server.sendmail(EMAIL_SENDER, to_email, msg.as_string())
        server.quit()
        print(f"✅ Email sent to {to_email}")
    except Exception as e:
        print(f"❌ Failed to send email to {to_email}: {e}")


# Hàm gửi email cho danh sách lấy từ Google Sheets
def send_bulk_emails():
    email_data = df_info["email"].tolist()
    rlevel = df_info["level"].tolist()

    # Xem lại logic gửi mail, đang gửi mail cho tất cả trạng thái của tất cả mail
    for row in email_data:
        email = row
        for rl in rlevel:
            level = rl

            if email and level:
                subject = "Thông báo kết quả"
                body = "Thành công" if level == 1 else "Thất bại" if level == 2 else None

                if body:
                    send_email(email, subject, body)

# Chạy hàm gửi email
send_bulk_emails()
