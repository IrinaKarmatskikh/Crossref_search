from http.server import BaseHTTPRequestHandler, HTTPServer
from io import BytesIO
import json
import requests

URL_PATH = "http://127.0.0.1:3000/get-info"
HEADERS = {
    'Content-Type': 'application/json'
}

DATA = {
    "issn"  : "some_issn",
    "date_start"  : "some_date_st",
    "cl_words"      : ["word1", "word2"],
    "authtors"      : "some_date_st",
}

# R = requests.request("POST", URL_PATH, data=payload, headers=HEADERS)  # suggested by Postman
R = requests.post(URL_PATH, json=json.dumps(DATA))

if R.ok:
    print("JSON: ", R.content)
else:
    R.raise_for_status()