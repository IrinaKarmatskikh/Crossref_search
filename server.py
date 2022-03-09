"""
Usage::
    ./server.py [--port <port>] [--service <"ocr-quality"|"pdfinfo"|"medtitles">]

Send a POST request with JSON payload::
    curl -d '{"text": "A case of aroducing small cell lung cancer"}' http://localhost:9000
"""

import time
import json
from http.server import BaseHTTPRequestHandler, HTTPServer
import argparse

from classifier import Classifier
from ocr_quality_classifier import Classifier as OCRQualityClassifier
from pdfinfo_classifier import Classifier as PDFInfoClassifier

HOST_NAME = '185.40.30.175'
HOST_NAME = '172.16.5.9'
HOST_NAME = '10.101.10.5'
PORT_NUMBER = 9000

# Соответствие названий классификаторов и классов, которые их реализуют.
classifiers_classes = {
        'ocr': OCRQualityClassifier,
        'titles': Classifier,
        'pdf': PDFInfoClassifier
        }

# Объекты
classifiers = {}



class RequestsHandler(BaseHTTPRequestHandler):
    def _set_headers(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()

    def do_HEAD(self):
        self._set_headers()

    def do_GET(self):
        self._set_headers()
        self.wfile.write(bytes('Not implemented, use POST.', 'UTF-8'))

    def do_POST(self):
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)

        self._set_headers()
        try:
            if self.path == '/tclass/ocr-quality':
                message = json.loads(post_data.decode('utf-8')).get('text')
                c = classifiers['ocr'] # OCRQualityClassifier()
            elif self.path == '/tclass/pdfinfo':
                message = json.loads(post_data.decode('utf-8')).get('text')
                c = classifiers['pdf'] # PDFInfoClassifier()
            else:
                message = json.loads(post_data.decode('utf-8')).get('text')
                c = classifiers['titles'] # Classifier()
            result = c.classify(message)
            json_response = {'result': result}
        except Exception as e:
            print(e)
            json_response =  {'error': str(e)}
        print(json_response)
        content = json.dumps(json_response)
        self.wfile.write(bytes(content, 'UTF-8'))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='FD.img services')
    parser.add_argument('--port', '-p', action="store", type=int, default=PORT_NUMBER, help='port to listen on')
    parser.add_argument('--service', '-s', action="store", default=None, help='service to manage, all if not specified')

    myargs = parser.parse_args()
    port_number = myargs.port
    service_to_run = myargs.service

    print(time.asctime(), 'Starting server on port %s for %s' % (port_number, service_to_run or 'ALL'))

    make_classifiers(service_to_run)

    server_class = HTTPServer
    httpd = server_class((HOST_NAME, port_number), RequestsHandler)
    print(time.asctime(), 'Server Starts - %s:%s' % (HOST_NAME, port_number))
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    httpd.server_close()
    print(time.asctime(), 'Server Stops - %s:%s' % (HOST_NAME, port_number))
