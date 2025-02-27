from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# from airflow.providers.telegram.operators.telegram import TelegramOperator
from datetime import datetime, timedelta

import os.path

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By

# Tạo hàm craw data
def craw_data():
    # 1. Create webdriver and connect website

    # Thiết lập option cho web
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    service = Service()

    webside = 'https://danhmuchanhchinh.gso.gov.vn/'
    driver = webdriver.Chrome(service=service, options=options)
    driver.get(webside)

    wait = WebDriverWait(driver, 10)

    # Func đợi web load
    def wait_for_page_load(driver, table_xpath, timeout=10):
        table = driver.find_element(By.XPATH, table_xpath)
        count = len(table.find_elements(By.TAG_NAME, "tr"))

        WebDriverWait(driver, timeout).until(
            lambda d: len(d.find_element(By.XPATH, table_xpath).find_elements(By.TAG_NAME, "tr")) > count
        )

    # 2. Interactive to website
    cap_dropdown = wait.until(EC.element_to_be_clickable((By.XPATH, '//input[@id="ctl00_PlaceHolderMain_cmbCap_I"]')))
    cap_dropdown.click()

    option_cap = wait.until(
        EC.element_to_be_clickable((By.XPATH, '//td[@id="ctl00_PlaceHolderMain_cmbCap_DDD_L_LBI3T0"]')))
    option_cap.click()

    action_button = driver.find_element(By.XPATH, '//td[@id="ctl00_PlaceHolderMain_ASPxButton1_B"]')
    action_button.click()

    # Đợi load bảng chỉ định
    wait_for_page_load(driver, "//table[contains(@id, 'ctl00_PlaceHolderMain_grid3_DXMainTable')]")

    # 3. Get dataaaaaaa!!!!!!
    all_data = []
    while True:
        try:
            # Xác định vị trí bảng dữ liệu
            table = wait.until(EC.presence_of_element_located(
                (By.XPATH, "//table[contains(@id, 'ctl00_PlaceHolderMain_grid3_DXMainTable')]")))
            rows = table.find_elements(By.TAG_NAME, "tr")
            for row in rows:
                # print(row.text)
                cols = row.find_elements(By.TAG_NAME, "td")
                cols = [col.text.strip() for col in cols]  # Lấy nội dung từng cột
                if cols:
                    all_data.append(cols)

            next_button = driver.find_elements(By.XPATH,
                                               '//td[@onclick="aspxGVPagerOnClick(\'ctl00_PlaceHolderMain_grid3\',\'PBN\');"]')  # Hoặc dùng class của nút
            if len(next_button) == 0 or "disabled" in next_button[0].get_attribute(
                    "class"):  # Kiểm tra nếu nút bị vô hiệu hóa
                break
            next_button[0].click()  # Click để sang trang
            # Chờ bảng cũ biến mất và bảng mới xuất hiện trước khi tiếp tục
            wait.until(EC.staleness_of(table))
            table = wait.until(EC.presence_of_element_located(
                (By.XPATH, "//table[contains(@id, 'ctl00_PlaceHolderMain_grid3_DXMainTable')]")))
        except Exception as e:
            print(e)
            break  # Không còn trang nào nữa

    driver.quit()

    df = pd.DataFrame(all_data)
    # df = df[[0,1,4,5,6,7]]
    df = df[[6, 7, 4, 5, 0, 1]]
    # df.columns = ["ma","ten_xa","ma_quan_huyen", "quan_huyen","ma_tinh","tinh"]
    df.columns = ["ma_tinh", "tinh", "ma_quan_huyen", "quan_huyen", "ma_xa_phuong", "xa_phuong", ]
    # print(df)

    xlsx_file_path = r"/danh_muc_hanh_chinh.xlsx"
    if os.path.exists(xlsx_file_path):
        os.remove(xlsx_file_path)
    df.to_excel("danh_muc_hanh_chinh.xlsx", index=False, engine="openpyxl")

# Thông tin Telegram Bot
TELEGRAM_CHAT_ID = "your_chat_id"
TELEGRAM_TOKEN = "your_telegram_bot_token"

# # Kết nối vào DB
def load_data():
    pass



# Tạo DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025,1,1),
    "retries": 1
}

with DAG(
    "crawl_and_update",
    default_args = default_args,
    schedule_interval= "0 1 * * *",
    catchup = False
) as dag:

    crawl_task = PythonOperator(task_id = "crawl_data",
                                python_callable = craw_data)

    etl_task = PythonOperator(task_id = "load_data",
                              python_callable = load_data)

    crawl_task >> etl_task
    