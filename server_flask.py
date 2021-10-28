from flask import Flask, jsonify
from flask import request
from flask_api import status as http_status
from load_crossref import Database

API_PORT = 3000
DATABASE_DIRSAVE = r'C:\Users\Ирина\Documents\Search (for medic)\DATABASE\db_python_app.sqlite'

app = Flask(__name__)

# Реализуем функцию, которая будет вызываться при обращении по адресу
# /get-info?id=123
#
@app.route("/get-info", methods=['GET', 'POST'])
def hello_world():
    #some_id = request.args.get('id', default = 1, type = int)  # Получаем значение параметра из URL.
    request_data = request.get_json()
    #post_data_as_json = request.args.get('temperature') # Если JSON передавался POST-запросом.
    #data = request_data['method']
    database = Database()
    connection = database.create_connection(DATABASE_DIRSAVE)


    print(request_data)

    status = http_status.HTTP_200_OK
    return_data = {
        'key': 'Любые данные, которые хотим вернуть.',
    }
    return '''<h1>The language value is: {}</h1>'''.format(request_data)
    #return jsonify(data=return_data), status
if __name__ == '__main__':
    app.run(port = API_PORT, debug = 1)