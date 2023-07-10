import datetime
import os
import random
import shutil
import sys
import uuid
from pathlib import Path
from time import sleep

import numpy as np
import openpyxl
import pandas as pd

import psycopg2 as psycopg2
from openpyxl import load_workbook
from pywinauto import keyboard

from config import logger, robot_name, db_host, db_port, db_name, db_user, db_pass, owa_username, owa_password
from rpamini import Web, App
from tools import take_screenshot, update_credentials


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


def start_one_branch(filepath):
    web = Web()
    web.run()
    web.get('https://cabinet.stat.gov.kz/')

    web.wait_element('//*[@id="idLogin"]')
    web.find_element('//*[@id="idLogin"]').click()

    web.wait_element('//*[@id="button-1077-btnEl"]')
    web.find_element('//*[@id="button-1077-btnEl"]').click()

    web.wait_element('//*[@id="lawAlertCheck"]')
    web.find_element('//*[@id="lawAlertCheck"]').click()

    web.find_element('//*[@id="loginButton"]').click()

    app = App('')

    ecp_path = r'M:\Stuff\_06_Бухгалтерия\! Актуальные ЭЦП\Торговый зал АСФ №1\AUTH_RSA256_913dc2beca1b810e0b0d8bc6adf56c474219831a.p12'
    # r'\\vault.magnum.local\common\Stuff\_06_Бухгалтерия\! Актуальные ЭЦП'

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


def get_all_branches_with_codes():
    import psycopg2
    import csv
    import pandas as pd

    conn = psycopg2.connect(dbname='adb', host='172.16.10.22', port='5432',
                            user='rpa_robot', password='Qaz123123+')

    cur = conn.cursor(name='1583_first_part')

    query = f"""
        select db.id_sale_object, ds.store_name from dwh_data.dim_branches db
    left join dwh_data.dim_store ds on db.id_sale_object = ds.sale_source_obj_id
    where ds.store_name like '%Торговый%' and current_date between ds.datestart and ds.dateend
    group by db.id_sale_object, ds.store_name
    """

    cur.execute(query)

    print('Executed')

    df1 = pd.DataFrame(cur.fetchall())
    df1.columns = ['store_id', 'store_name']

    cur.close()
    conn.close()

    return df1


if __name__ == '__main__':
    # sql_create_table()
    # start = datetime.datetime.now().strftime("%d.%m.%Y %H:%M:%S.%f")
    # sleep(5)
    # insert_data_in_db(start, 4, 'Алматинский филиал №1', 'success', '', '', '10s')

    all_branches = []

    counter = 2

    df = pd.DataFrame(columns=['id', 'branch', 'data'])

    for file in os.listdir(r'C:\Users\Abdykarim.D\Desktop\downloads'):

        if file == '!result.xlsx':

            book = load_workbook(os.path.join(r'C:\Users\Abdykarim.D\Desktop\downloads', file))
            sheet = book.active

            while sheet[f'A{counter}'].value is not None:
                all_branches.append([sheet[f'A{counter}'].value, sheet[f'B{counter}'].value, [sheet[f'I{counter}'].value, sheet[f'I{counter + 1}'].value, sheet[f'I{counter + 2}'].value]])

                row = pd.DataFrame({'id': sheet[f'A{counter}'].value, 'branch': sheet[f'B{counter}'].value, 'data': [[sheet[f'I{counter}'].value, sheet[f'I{counter + 1}'].value, sheet[f'I{counter + 2}'].value]]})

                df = pd.concat([df, row], ignore_index=True)
                counter += 3

    # for i in all_branches:
    #     print(i)
    print(df.iloc[0])
    df1 = get_all_branches_with_codes()

    df['name'] = None

    for i in range(len(df)):
        df['name'].loc[i] = df1[df1['store_id'] == df['id'].iloc[i]]['store_name'].iloc[0]

    # print(df)
    # df.to_excel(r'C:\Users\Abdykarim.D\Desktop\loadfl.xlsx')

    for i in df['name']:

        ecp_path = fr'\\vault.magnum.local\common\Stuff\_06_Бухгалтерия\! Актуальные ЭЦП\{i}'
        # update_credentials(Path(ecp_path), "Abdykarim.D@magnum.kz", "Фф123456+")
        # for fole in os.listdir(ecp_path):
        #     print(fole)
        # print(ecp_path)

        if os.path.exists(ecp_path) and os.path.isdir(ecp_path):
            print('Yes')
        else:
            print('NOOOOOOOO!!!!', ecp_path)

    # get_all_data()

    # path = ''
    # start_one_branch(path)
