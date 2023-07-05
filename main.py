import datetime
import random
import sys
import uuid
from time import sleep

import psycopg2 as psycopg2
from pywinauto import keyboard

from config import logger, robot_name, db_host, db_port, db_name, db_user, db_pass
from rpamini import Web, App
from tools import take_screenshot


def sql_create_table():
    conn = psycopg2.connect(host=db_host, port=db_port, database=db_name, user=db_user, password=db_pass)
    table_create_query = f'''
        CREATE TABLE IF NOT EXISTS ROBOT.{robot_name.replace("-", "_")} (
            id text PRIMARY KEY,
            started_time timestamp,
            ended_time timestamp,
            store_id int UNIQUE,
            store_name text,
            status text,
            error_reason text,
            error_saved_path text,
            execution_time text
            )
        '''
    c = conn.cursor()
    c.execute(table_create_query)

    conn.commit()
    c.close()
    conn.close()


def delete_by_id(id):
    conn = psycopg2.connect(host=db_host, port=db_port, database=db_name, user=db_user, password=db_pass)
    table_create_query = f'''
                DELETE FROM ROBOT.{robot_name.replace("-", "_")} WHERE id = '{id}'
                '''
    c = conn.cursor()
    c.execute(table_create_query)
    conn.commit()
    c.close()
    conn.close()


def get_all_data():
    conn = psycopg2.connect(host=db_host, port=db_port, database=db_name, user=db_user, password=db_pass)
    table_create_query = f'''
            SELECT * FROM ROBOT.{robot_name.replace("-", "_")}
            '''
    c = conn.cursor()
    c.execute(table_create_query)
    rows = c.fetchall()

    for row in rows:
        print(row)

    c.close()
    conn.close()


def insert_data_in_db(started_time, store_id, store_name, status, error_reason, error_saved_path, execution_time):
    conn = psycopg2.connect(host=db_host, port=db_port, database=db_name, user=db_user, password=db_pass)

    insert_query = f'''
        INSERT INTO ROBOT.{robot_name.replace("-", "_")} (id, started_time, ended_time, store_id, store_name, status, error_reason, error_saved_path, execution_time)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
    '''

    cursor = conn.cursor()

    data = (str(uuid.uuid4()), started_time, datetime.datetime.now().strftime("%d.%m.%Y %H:%M:%S.%f"), store_id, store_name, status, error_reason, error_saved_path, execution_time)
    cursor.execute(insert_query, data)
    conn.commit()

    cursor.close()
    conn.close()


if __name__ == '__main__':
    sql_create_table()
    start = datetime.datetime.now().strftime("%d.%m.%Y %H:%M:%S.%f")
    sleep(10)
    insert_data_in_db(start, 4, 'Алматинский филиал №1', 'success', '', '', '10s')
    get_all_data()
    exit()
    # logger.info('Started')
    #
    web = Web()
    web.run()
    web.get('https://cabinet.stat.gov.kz/')
    # sleep(10)
    # web.get('https://stat.gov.kz/')
    #
    # web.wait_element('/html/body/header/div/div[2]/ul/li[3]/a')
    # web.find_element('/html/body/header/div/div[2]/ul/li[3]/a').click()
    #
    # web.wait_element('/html/body/div[4]/div/div/div[1]/a')
    # web.find_element('/html/body/div[4]/div/div/div[1]/a').click()
    #
    # app = App('')
    # el = app.find_element({"title": "Подтвердите действие на странице stat.gov.kz", "class_name": "", "control_type": "TitleBar", "visible_only": True, "enabled_only": True, "found_index": 0})
    # parent = el.parent(2)
    # parent.type_keys(app.keys.ENTER)
    #
    web.wait_element('//*[@id="idLogin"]')
    web.find_element('//*[@id="idLogin"]').click()

    web.wait_element('//*[@id="button-1077-btnEl"]')
    web.find_element('//*[@id="button-1077-btnEl"]').click()

    web.wait_element('//*[@id="lawAlertCheck"]')
    web.find_element('//*[@id="lawAlertCheck"]').click()

    web.find_element('//*[@id="loginButton"]').click()

    app = App('')
    # el__ = app.find_element({"title": "Имя файла:", "class_name": "Edit", "control_type": "Edit", "visible_only": True,
    #                         "enabled_only": True, "found_index": 0})

    ecp_path = r'M:\Stuff\_06_Бухгалтерия\! Актуальные ЭЦП\Торговый зал АСФ №1\AUTH_RSA256_913dc2beca1b810e0b0d8bc6adf56c474219831a.p12'

    app.wait_element({"title": "Открыть файл", "class_name": "SunAwtDialog", "control_type": "Window", "visible_only": True, "enabled_only": True, "found_index": 0})

    keyboard.send_keys(ecp_path, pause=0.01, with_spaces=True)

    keyboard.send_keys('{ENTER}')
    app.wait_element({"title": "Формирование ЭЦП в формате CMS", "class_name": "SunAwtDialog", "control_type": "Window", "visible_only": True, "enabled_only": True, "found_index": 0})

    keyboard.send_keys('Aa123456')
    sleep(1.5)
    # web.find_element("//button[@type='button' and contains(text(), 'Ok')]").click()
    keyboard.send_keys('{ENTER}')
    sleep(1.5)
    # web.find_element("//button[@type='button' and contains(text(), 'Ok')]").click()
    keyboard.send_keys('{ENTER}')
    logged_in = web.wait_element('//*[@id="idLogout"]/a')

    if logged_in:

        sleep(0.5)
        web.find_element('//*[@id="tab-1168-btnInnerEl"]').click()
        sleep(0.5)
        web.find_element('//*[@id="radio-1131-boxLabelEl"]').click()
        sleep(0.5)
        web.find_element('//*[@id="radio-1132-boxLabelEl"]').click()
        sleep(0.5)
        web.find_element("//div[contains(text(), '2-торговля')]").click()

        sleep(0.5)

        web.find_element('//*[@id="createReportId-btnIconEl"]').click()

        sleep(1)
        web.driver.switch_to.window(web.driver.window_handles[-1])
        sleep(1)

        print('Here')

        web.wait_element('//*[@id="td_select_period_level_1"]/span')
        web.execute_script_click("#btn-opendata")
        # web.find_element('//*[@id="btn-opendata"]').click()  # Открыть
        sleep(10)
        web.wait_element('//*[@id="sel_statcode_accord"]/div/p/b[1]')
        web.execute_script_click("body > div:nth-child(16) > div.ui-dialog-buttonpane.ui-widget-content.ui-helper-clearfix > div > button:nth-child(1) > span")
        # web.find_element('/html/body/div[15]/div[11]/div/button[1]/span').click()  # Выбрать

        web.wait_element('//*[@id="sel_rep_accord"]/h3[1]/a')
        web.execute_script_click("body > div:nth-child(18) > div.ui-dialog-buttonpane.ui-widget-content.ui-helper-clearfix > div > button:nth-child(1)")
        # web.find_element('/html/body/div[17]/div[11]/div/button[1]/span').click()  # Открыть

        groups = ['Объем оптовой торговли',
                  'Объем розничной торговли',
                  'Товарные запасы на конец отчетного месяца',
                  'Рис',
                  'гречневая',
                  'подсолнечн',
                  'белокоч',
                  'репчатый',
                  'Морковь',
                  'Картофель',
                  'Сахар',
                  'Соль']

        for ind, group in enumerate(groups):
            if ind < 3:
                web.execute_script(f"//*[contains(text(), '{group}')]/following-sibling::*[contains(@role, 'gridcell')][1]", random.randint(100, 1000))
            web.execute_script(f"//*[contains(text(), '{group}')]/following-sibling::*[contains(@role, 'gridcell')][2]", random.randint(100, 1000))

        web.find_element('//*[@id="tabs-panel"]/ul/li[2]/a').click()
        web.execute_script(element_type="value", xpath="//*[@id='inpelem_1_0']", value='Қалдыбек Б.Ғ.')
        web.execute_script(element_type="value", xpath="//*[@id='inpelem_1_1']", value='87073332438')
        web.execute_script(element_type="value", xpath="//*[@id='inpelem_1_2']", value='87073332438')
        web.execute_script(element_type="value", xpath="//*[@id='inpelem_1_3']", value='KALDYBEK.B@magnum.kz')

        take_screenshot()

        sleep(100)
