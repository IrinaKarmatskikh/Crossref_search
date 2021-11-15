from flask import Flask, jsonify
from flask import request
from flask_api import status as http_status
import json
import ast
from load_crossref import Database
import sqlite3
from sqlite3 import Error

API_PORT = 3000
DATABASE_DIRSAVE = r'C:\Users\Ирина\Documents\Search (for medic)\DATABASE\db_python_app.db'

app = Flask(__name__)

# Реализуем функцию, которая будет вызываться при обращении по адресу
# /get-info?id=123
#
@app.route("/get-info", methods=['GET', 'POST'])
def hello_world():
    #some_id = request.args.get('id', default = 1, type = int)  # Получаем значение параметра из URL.
    request_data = request.get_json()
    request_data = ast.literal_eval(request_data)

    if len(request_data.get('cl_words')) > 0:
        param_words_in_art = "article.title LIKE '{}'".format("%" + request_data.get('cl_words')[0] + "%")
        if len(request_data.get('cl_words')) > 1:
            for i in range(1, len(request_data.get('cl_words'))):
                param_words_in_art += " AND " + \
                                      "article.title LIKE '{}'".format("%" + request_data.get('cl_words')[i] + "%")
        # param_words_in_art = title LIKE %word1% AND title LIKE %word2% snd ...
    else:
        param_words_in_art = 'true'

    if len(request_data.get('authors')) > 0:
        param_authors = "surnames LIKE '{}'".format("%" + request_data.get('authors')[0] + "%")
        if len(request_data.get('authors')) > 1:
            for i in range(1, len(request_data.get('cl_words'))):
                param_authors += " AND " + "surnames LIKE '{}'".format("%" + request_data.get('authors')[i] + "%")
        # param_authors = surnames LIKE %auth1% AND surnames LIKE  %auth2% snd ...
    else:
        param_authors = 'true'

    """How to conduct consistent environmental, economic, and social assessment during the building design process. A BIM-based Life Cycle Sustainability Assessment method HEEY
RuBisCO from alfalfa – native subunits preservation through sodium sulfite addition and reduced solubility after acid precipitation followed by freeze-drying HEEY
Antioxidant properties and digestion behaviors of polysaccharides from Chinese yam fermented by Saccharomyces boulardii HEEY
Hydrodynamic cavitation (HC) degradation of tetracycline hydrochloride (TC) HEEY
Selection of sensitive seeds for evaluation of compost maturity with the seed germination index HEEY"""

    sql = """   SELECT title, surnames, doi, issn, date, address FROM
                (SELECT article.*, GROUP_CONCAT(DISTINCT author.surname) as surnames
                 FROM 
                    article LEFT JOIN auth_article 
                        ON article.id = auth_article.article_id
                    LEFT JOIN author
                        ON author.id = auth_article.auth_id
                WHERE {first}
                    GROUP BY article.id ) 
            WHERE {second}

        """.format(first=param_words_in_art, second=param_authors)
        #first="article.title LIKE '{}'".format("%and%"), second="true")
    database = Database()
    connection = database.create_connection(DATABASE_DIRSAVE)
    cursor = connection.cursor()
    sql1 = """     SELECT article.*, GROUP_CONCAT(author.surname) as surnames
                 FROM 
                    article  JOIN auth_article 
                        ON article.id = auth_article.article_id
                     JOIN author
                        ON author.id = auth_article.auth_id
                WHERE article.title Like '%and%'
                    GROUP BY article.id 
                                    """
    cursor.execute(sql)
    result = cursor.fetchall()
    print(len(result))
    print(result)
    #args = sum(permList, [])
    #cursor.execute(sql, args)
    #post_data_as_json = request.args.get('temperature') # Если JSON передавался POST-запросом.
    #data = request_data['method']
    #database = Database()
    #connection = database.create_connection(DATABASE_DIRSAVE)
    # print(sql)

    status = http_status.HTTP_200_OK
    return '''{}'''.format(result)
    #return jsonify(data=return_data), status

if __name__ == '__main__':
    app.run(port = API_PORT, debug = 1)