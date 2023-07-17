import requests
r = requests.get('https://qlik-sense.magnum.kz/').text

print(r)
