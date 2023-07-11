import datetime
import os
import random
import shutil
import sys
import time
import uuid
from pathlib import Path
from time import sleep

import numpy as np
import openpyxl
import pandas as pd

import psycopg2 as psycopg2
from openpyxl import load_workbook
from pywinauto import keyboard

from config import logger, robot_name, db_host, db_port, db_name, db_user, db_pass, owa_username, owa_password, working_path
from rpamini import Web, App
from tools import update_credentials
from pyautogui import screenshot


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

    query = f"""
        INSERT INTO ROBOT.{robot_name.replace("-", "_")} (id, started_time, ended_time, store_id, store_name, status, error_reason, error_saved_path, execution_time)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (store_id) DO UPDATE
        SET started_time = EXCLUDED.started_time,
            ended_time = EXCLUDED.ended_time,
            store_name = EXCLUDED.store_name,
            status = EXCLUDED.status,
            error_reason = EXCLUDED.error_reason,
            error_saved_path = EXCLUDED.error_saved_path,
            execution_time = EXCLUDED.execution_time;
    """

    values = (
        str(uuid.uuid4()),
        started_time,
        datetime.datetime.now().strftime("%d.%m.%Y %H:%M:%S.%f"),
        store_id,
        store_name,
        status,
        error_reason,
        error_saved_path,
        execution_time
    )

    cursor = conn.cursor()
    cursor.execute(query, values)
    conn.commit()

    cursor.close()
    conn.close()


def get_all_branches_with_codes():
    import psycopg2
    import pandas as pd

    conn = psycopg2.connect(dbname='adb', host='172.16.10.22', port='5432',
                            user='rpa_robot', password='Qaz123123+')

    cur = conn.cursor(name='1583_first_part')

    query = f"""
        select db.id_sale_object, ds.source_store_id, ds.store_name from dwh_data.dim_branches db
        left join dwh_data.dim_store ds on db.id_sale_object = ds.sale_source_obj_id
        where ds.store_name like '%Торговый%' and current_date between ds.datestart and ds.dateend
        group by db.id_sale_object, ds.source_store_id, ds.store_name
        order by ds.source_store_id
    """

    cur.execute(query)

    print('Executed')

    df1 = pd.DataFrame(cur.fetchall())
    df1.columns = ['branch_id', 'store_id', 'store_name']

    cur.close()
    conn.close()

    return df1


def sign_ecp(ecp):
    app = App('')

    app.wait_element({"title": "Открыть файл", "class_name": "SunAwtDialog", "control_type": "Window", "visible_only": True, "enabled_only": True, "found_index": 0})

    keyboard.send_keys(ecp, pause=0.01, with_spaces=True)

    keyboard.send_keys('{ENTER}')
    app.wait_element({"title_re": "Формирование ЭЦП.*", "class_name": "SunAwtDialog", "control_type": "Window", "visible_only": True, "enabled_only": True, "found_index": 0})

    keyboard.send_keys('Aa123456')
    sleep(1.5)

    keyboard.send_keys('{ENTER}')
    sleep(1.5)

    keyboard.send_keys('{ENTER}')


def start_single_branch(filepath, store, values_first_part, values_second_part):
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

    ecp_auth = ''
    ecp_sign = ''
    for files in os.listdir(filepath):
        if 'AUTH' in files:
            ecp_auth = os.path.join(filepath, files)
        if 'GOST' in files:
            ecp_sign = os.path.join(filepath, files)

    sign_ecp(ecp_auth)

    logged_in = web.wait_element('//*[@id="idLogout"]/a')

    if logged_in:
        if web.find_element("//a[text() = 'Выйти']"):

            print(web.wait_element('//*[@id="dontAgreeId-inputEl"]', timeout=5), end=' ')
            print(web.wait_element("//span[contains(text(), 'Пройти позже')]", timeout=5), end='==========\n')

            if web.wait_element("//span[contains(text(), 'Пройти позже')]", timeout=5):
                try:
                    web.find_element("//span[contains(text(), 'Пройти позже')]").click()
                except:

                    print('HUETA')
                    sleep(200)

            if web.wait_element('//*[@id="dontAgreeId-inputEl"]', timeout=5):
                web.find_element('//*[@id="dontAgreeId-inputEl"]').click()
                sleep(0.3)
                web.find_element('//*[@id="saveId-btnIconEl"]').click()
                sleep(1)
                web.find_element('//*[@id="ext-gen1893"]').click()
                web.find_element('//*[@id="boundlist-1327-listEl"]/ul/li').click()
                # web.find_element('//*[@id="boundlist-1327-listEl"]/ul').select('Персональный компьютер')
                sleep(1)
                web.find_element('//*[@id="button-1326-btnIconEl"]').click()
                print('Done lol')
                sign_ecp(ecp_sign)

                try:
                    web.wait_element("//span[contains(text(), 'Пройти позже')]", timeout=5)
                    web.find_element("//span[contains(text(), 'Пройти позже')]").click()
                except:
                    pass

            web.wait_element('//*[@id="tab-1168-btnInnerEl"]')
            web.find_element('//*[@id="tab-1168-btnInnerEl"]').click()
            sleep(0.5)
            web.wait_element('//*[@id="radio-1131-boxLabelEl"]')
            web.find_element('//*[@id="radio-1131-boxLabelEl"]').click()
            sleep(0.5)
            web.find_element('//*[@id="radio-1132-boxLabelEl"]').click()  # ? УБРАТЬ В БОЮ
            sleep(0.5)
            if web.wait_element("//div[contains(text(), '2-торговля')]", timeout=5):
                web.find_element("//div[contains(text(), '2-торговля')]").click()
            else:
                scr = screenshot()
                scr_path = str(os.path.join(working_path, str(store + '.png')))
                scr.save(scr_path)

                web.close()
                web.quit()

                return 'HUETA'

            sleep(0.5)

            web.find_element('//*[@id="createReportId-btnIconEl"]').click()

            sleep(1)
            web.driver.switch_to.window(web.driver.window_handles[-1])

            web.wait_element('//*[@id="td_select_period_level_1"]/span')
            web.execute_script_click_js("#btn-opendata")

            web.wait_element('//*[@id="sel_statcode_accord"]/div/p/b[1]')
            web.execute_script_click_js("body > div:nth-child(16) > div.ui-dialog-buttonpane.ui-widget-content.ui-helper-clearfix > div > button:nth-child(1) > span")

            web.wait_element('//*[@id="sel_rep_accord"]/h3[1]/a')
            web.execute_script_click_js("body > div:nth-child(18) > div.ui-dialog-buttonpane.ui-widget-content.ui-helper-clearfix > div > button:nth-child(1)")

            groups = ['Объем розничной торговли',
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

            web.wait_element("//a[contains(text(), 'Страница 1')]")
            web.find_element("//a[contains(text(), 'Страница 1')]").click()

            for ind, group in enumerate(groups):

                if group == 'Объем розничной торговли':
                    web.execute_script(xpath=f"//*[contains(text(), '{group}')]/following-sibling::*[contains(@role, 'gridcell')][1]", value=str(values_first_part[0]))
                    web.execute_script(xpath=f"//*[contains(text(), '{group}')]/following-sibling::*[contains(@role, 'gridcell')][2]", value=str(values_first_part[1]))

                elif group == 'Товарные запасы на конец отчетного месяца':
                    web.execute_script(xpath=f"//*[contains(text(), '{group}')]/following-sibling::*[contains(@role, 'gridcell')][1]", value=str(values_first_part[2]))

                else:
                    web.execute_script(xpath=f"//*[contains(text(), '{group}')]/following-sibling::*[contains(@role, 'gridcell')][2]", value=str(random.randint(100, 1000)))

            sleep(30)
            web.find_element("//a[contains(text(), 'Данные исполнителя')]").click()
            web.execute_script(element_type="value", xpath="//*[@id='inpelem_1_0']", value='Қалдыбек Б.Ғ.')
            web.execute_script(element_type="value", xpath="//*[@id='inpelem_1_1']", value='87073332438')
            web.execute_script(element_type="value", xpath="//*[@id='inpelem_1_2']", value='87073332438')
            web.execute_script(element_type="value", xpath="//*[@id='inpelem_1_3']", value='KALDYBEK.B@magnum.kz')

            # web.execute_script_click_xpath("//span[text() = 'Сохранить']")
            sleep(30)
            web.close()
            web.quit()

            return 'success'


if __name__ == '__main__':
    # sql_create_table()
    # start = datetime.datetime.now().strftime("%d.%m.%Y %H:%M:%S.%f")
    # # sleep(5)
    # # insert_data_in_db(start, 4, 'Алматинский филиал №1', 'success', '', '', '10s')
    # insert_data_in_db(started_time=start, store_id=int(4), store_name='Торговый зал АФ №1', status='success', error_reason='No error', error_saved_path='', execution_time=str(10))
    # exit()
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

    df1 = get_all_branches_with_codes()

    df['name'] = None
    df['store_id'] = None

    for i in range(len(df)):
        df['name'].loc[i] = df1[df1['branch_id'] == df['id'].iloc[i]]['store_name'].iloc[0]
        df['store_id'].loc[i] = df1[df1['branch_id'] == df['id'].iloc[i]]['store_id'].iloc[0]

    for ind, branch in enumerate(df['name'].iloc[40:]):

        ecp_path = fr'\\vault.magnum.local\common\Stuff\_06_Бухгалтерия\! Актуальные ЭЦП\{branch}'

        if os.path.exists(ecp_path) and os.path.isdir(ecp_path):
            start = datetime.datetime.now().strftime("%d.%m.%Y %H:%M:%S.%f")
            start_time = time.time()
            if True:
                print('Started', ind, branch)
                status = start_single_branch(ecp_path, branch, df['data'].iloc[ind], '')
                end_time = time.time()
                if status != 'success':
                    insert_data_in_db(started_time=start, store_id=int(df['store_id'].iloc[ind]), store_name=str(branch), status=status, error_reason=status, error_saved_path='', execution_time=str(end_time - start_time))
                else:
                    insert_data_in_db(started_time=start, store_id=int(df['store_id'].iloc[ind]), store_name=str(branch), status='success', error_reason='No error', error_saved_path='', execution_time=str(end_time - start_time))
            # except:
            #     end_time = time.time()
            #     insert_data_in_db(started_time=start, store_id=int(df['store_id'].iloc[ind]), store_name=str(branch), status='failed', error_reason='Error', error_saved_path='', execution_time=str(end_time - start_time))

