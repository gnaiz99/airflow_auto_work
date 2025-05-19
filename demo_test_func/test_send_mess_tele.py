from datetime import datetime, timedelta
from airflow.providers.telegram.hooks.telegram import TelegramHook

TELEGRAM_TOKEN = '8021130212:AAHx7u70ptnb4VpZjDrsMu5i-yYY-dlBzyA' # tuyệt đối ko ghi thông tin credential lên code
TELEGRAM_CHAT_ID = '-1004841407782'
filepath = r"C:\Users\giang.nt\Downloads\file hoàn thành\cskh_14052025.xlsx"
def send_telegram_message(message: str):
    # tele_hook = TelegramHook(
    #     telegram_conn_id=self.telegram_conn_id,
    #     token=self.token,
    #     chat_id=TELEGRAM_CHAT_ID
    # )
    # tele_hook.send_message(api_params={"text": self.message,
    #                                    "parse_mode": "HTML"})
    # tele_hook.send_file(filepath,caption='Đây là báo cáo hôm nay')

    telegram_hook = TelegramHook(
        telegram_conn_id=self.telegram_conn_id,
        token=TELEGRAM_TOKEN,
        chat_id=TELEGRAM_CHAT_ID,
    )
    telegram_hook.send_message(self.telegram_kwargs)


def get_data_and_send():
    date_str = datetime.now().strftime("%d/%m/%Y")
    message = f"""<b>Dear team CSKH,</b>
                \nData gửi thông tin số lượng check bot ngày {date_str}
                \n<b>xincamon!</b>"""
    send_telegram_message(message)

get_data_and_send()

# Chưa done!!!!!!!!!!!!!!!!!!!!!!!!
