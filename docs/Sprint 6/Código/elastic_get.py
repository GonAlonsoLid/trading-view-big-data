import requests
from requests.auth import HTTPBasicAuth

url = 'http://192.168.80.37:9201/'
index = 'gittba00_test'
type = '/_doc/'
id = 'LYHObJ0BZBoeEvh9AglU'

user = 'user'
password = 'password'

response = requests.get(
    url + index + type + id,
    auth=HTTPBasicAuth(user, password)
)

res = response.json()
print(res)