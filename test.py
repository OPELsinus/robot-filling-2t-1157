import psycopg2

from config import db_host, robot_name, db_port, db_name, db_user, db_pass

import pandas as pd

from openpyxl import load_workbook

from openpyxl.styles import PatternFill, Alignment


def get_all_data():
    conn = psycopg2.connect(host=db_host, port=db_port, database=db_name, user=db_user, password=db_pass)
    table_create_query = f'''
            SELECT * FROM ROBOT.{robot_name.replace("-", "_")}
            '''
    cur = conn.cursor()
    cur.execute(table_create_query)

    df1 = pd.DataFrame(cur.fetchall())
    df1.columns = ['started_time', 'ended_time', 'store_id', 'store_name', 'status', 'error_reason', 'error_saved_path', 'execution_time']

    cur.close()
    conn.close()

    return df1


if __name__ == '__main__':

    df = get_all_data()

    df.columns = ['Время начала1', 'Время окончания1', 'Номер филиала', 'Название филиала', 'Статус', 'Причина ошибки', 'Пусть сохранения скриншота', 'Время исполнения (сек)']

    df['Время исполнения (сек)'] = df['Время исполнения (сек)'].astype(float)
    df['Время исполнения (сек)'] = df['Время исполнения (сек)'].round()

    df['Время начала'] = df['Время начала1']
    df['Время окончания'] = df['Время окончания1']

    df = df.drop(['Время начала1', 'Время окончания1'], axis=1)

    df.to_excel('output.xlsx', index=False)

    workbook = load_workbook('output.xlsx')
    sheet = workbook.active

    red_fill = PatternFill(start_color="FFA864", end_color="FFA864", fill_type="solid")
    green_fill = PatternFill(start_color="A6FF64", end_color="A6FF64", fill_type="solid")

    for cell in sheet['C']:
        if cell.value == 'failed':
            cell.fill = red_fill
        if cell.value == 'success':
            cell.fill = green_fill

    for col in 'ABCEFGH':

        max_length = max(len(str(cell.value)) for cell in sheet[col])

        if col == 'A' or col == 'B':
            max_length += 5
        # if col == 'D':
        #     max_length += 5
        # if col == 'A':
        #     max_length -= 3

        sheet.column_dimensions[col].width = max_length

    for col in 'ABCDGEFGH':
        for cell in sheet[col]:
            cell.alignment = Alignment(horizontal='center')

    workbook.save('output.xlsx')

