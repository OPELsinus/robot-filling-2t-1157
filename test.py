import os
import shutil
from pathlib import Path

for file in os.listdir(r'C:\Users\Abdykarim.D\Downloads\Отчёты 2т'):

    if '_1' in file:
        print(file)
    # print(file, file.split('.')[0].split('_')[0] + '.jpg')
        shutil.move(os.path.join(r'C:\Users\Abdykarim.D\Downloads\Отчёты 2т', file), os.path.join(r'C:\Users\Abdykarim.D\Downloads\Отчёты 2т', file.split('.')[0].split('_')[0] + '.jpg'))
