import psycopg2

from config import db_host, db_port, db_name, db_user, db_pass

import pandas as pd

conn = psycopg2.connect(host=db_host, port=db_port, database=db_name, user=db_user, password=db_pass)
table_create_query = f'''
select * from robot.robot_encashment where sdate = '26.09.2023' or sdate = '27.09.2023' order by to_date(sdate, 'DD.MM.YYYY') DESC
        '''
cur = conn.cursor()
cur.execute(table_create_query)

df1 = pd.DataFrame(cur.fetchall())
cur.close()
conn.close()

print(df1)

df1.to_excel('encash.xlsx')
