from http.server import BaseHTTPRequestHandler, HTTPServer
from io import BytesIO
import json
import requests

URL_PATH = "http://127.0.0.1:5000/get-info"
HEADERS = {
    'Content-Type': 'application/json'
}

DATA = {
    "doi"  : "some_doi",
    "date_start"  : "some_date_st",
    "cl_words"      : ["and"],
    "authors"      : ["Wang"],
}

# R = requests.request("POST", URL_PATH, data=payload, headers=HEADERS)  # suggested by Postman
R = requests.post(URL_PATH, json=json.dumps(DATA))

if R.ok:

    print("Result: ", R.content)
else:
    R.raise_for_status()