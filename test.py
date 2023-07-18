import os
import shutil
from time import sleep

while True:
    for file in os.listdir(r'C:\Users\Abdykarim.D\Documents'):
        if 'doc' in file and '.pdf' in file:
            print(file)
            try:
                shutil.move(fr'C:\Users\Abdykarim.D\Documents\{file}', fr'C:\Users\Abdykarim.D\Documents\hueta\{file}')
            except:
                sleep(1)
                shutil.move(fr'C:\Users\Abdykarim.D\Documents\{file}', fr'C:\Users\Abdykarim.D\Documents\hueta\{file}')
