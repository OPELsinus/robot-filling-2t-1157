import os

import pyautogui
from pywinauto import keyboard

from config import working_path
from tools import take_screenshot

scr = pyautogui.screenshot()
screenshot_path = str(os.path.join(working_path, 'kekus.png'))
scr.save(screenshot_path)
