from rpamini import App

app = App('')

app.find_element({"title": "Открыть файл", "class_name": "SunAwtDialog", "control_type": "Window", "visible_only": True, "enabled_only": True, "found_index": 0}).click()

