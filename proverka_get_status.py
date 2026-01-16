import requests

API_KEY = "eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjUwNTIwdjEiLCJ0eXAiOiJKV1QifQ.eyJlbnQiOjEsImV4cCI6MTc2NDE1OTkyMCwiaWQiOiIwMTk3MTQ0Ny04NDBkLTdlNDAtYjk5Ni05NjllODMyNTQ4YTMiLCJpaWQiOjIxNjY2MjY2OSwib2lkIjo0NDYxODQ2LCJzIjoxNiwic2lkIjoiMGYzZTQwMDMtZmQ3Yi00MTczLTljM2EtNWI0MzY3MGRlMjU4IiwidCI6ZmFsc2UsInVpZCI6MjE2NjYyNjY5fQ.6P1DXZrRPAvYnXr3aV55Ndl1rJS9QEDOWQPM369YC_AZ3QCTtBC92GrzESXFZy3GuMAIKDCkn3LYKCFg6hKtzw"
HEADERS = {"Authorization": API_KEY}

URL = "https://marketplace-api.wildberries.ru/api/v3/orders/status"

# Пример ID сборочного задания
order_ids = [3547036956]

payload = {"orders": order_ids}

response = requests.post(URL, headers=HEADERS, json=payload)
response.raise_for_status()

print(response.json())
