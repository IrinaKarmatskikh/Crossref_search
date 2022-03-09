from flask import Flask, jsonify
from flask import request
from flask_api import status as http_status
import json
import ast
from load_crossref import Database
import sqlite3
from sqlite3 import Error

API_PORT = 5000
DATABASE_DIRSAVE = r'C:\Users\Ирина\Documents\Search (for medic)\DATABASE\db_python_app.db'

app = Flask(__name__)

# Реализуем функцию, которая будет вызываться при обращении по адресу
# /get-info?id=123
#
@app.route("/get-info", methods=['GET', 'POST'])
def search():
    request_data = request.get_json()
    request_data = ast.literal_eval(request_data)

    param_words_in_art = ""
    date_str = ""
    param_authors = ""
    cl_word_par = []
    date_par = []
    issn_par = []
    authors = []

    # DATE SELECT

    flag_date = 0
    if request_data.get('date_start') != "":
        date_str = "article.date >= ?"
        date_par.append(request_data.get('date_start'))
        if request_data.get('date_end') != "":
            date_str += "AND article.date <= ?"
            date_par.append(request_data.get('date_end'))
        flag_date = 1
    elif request_data.get('date_end') != "":
        date_str += "article.date <= ?"
        date_par.append(request_data.get('date_end'))
        flag_date = 1

    # WORD IN TITLE SELECT

    if len(request_data.get('cl_words')) > 0:

        param_words_in_art = "article.title LIKE '%' || ? || '%'"
        cl_word_par.append(request_data.get('cl_words')[0])
        if len(request_data.get('cl_words')) > 1:
            for i in range(1, len(request_data.get('cl_words'))):
                param_words_in_art += " AND " + "article.title LIKE '%' || ? || '%'"
                cl_word_par.append(request_data.get('cl_words')[i])
        # param_words_in_art = title LIKE %word1% AND title LIKE %word2% snd ...
        if flag_date == 1:
            param_words_in_art += " AND " + date_str
    elif flag_date == 1:
            param_words_in_art = date_str
    else:
        param_words_in_art = "1"

    # ISSN SELECT

    if request_data.get('issn') != "":
        if param_words_in_art == "1":
            param_words_in_art = "article.issn = ?"
        else:
            param_words_in_art += "AND article.issn LIKE '%' || ? || '%'"
        issn_par.append(request_data.get('issn'))

    # AUTHORS SELECT

    if len(request_data.get('authors_family')) > 0:
        param_authors = "surnames LIKE '%' || ? || '%'"
        authors.append(request_data.get('authors_family')[0])

        if len(request_data.get('authors_family')) > 1:
            for i in range(1, len(request_data.get('authors_family'))):
                param_authors += " AND " + "surnames LIKE '%' || ? || '%'"
                authors.append(request_data.get('authors_family')[i])
        # param_authors = surnames LIKE %auth1% AND surnames LIKE  %auth2% snd ...

        if len(request_data.get('authors_full_name')) > 0:
            for i in range(1, len(request_data.get('authors_full_name'))):
                param_authors += " AND " + "surnames LIKE '%' || ? || '%'"
                authors.append(request_data.get('authors_full_name')[i])

    elif len(request_data.get('authors_full_name')) > 0:
        param_authors = "surnames LIKE '%' || ? || '%'"
        authors.append(request_data.get('authors_full_name')[0])
        if len(request_data.get('authors_full_name')) > 1:
            for i in range(1, len(request_data.get('authors_full_name'))):
                param_authors += " AND " + "surnames LIKE '%' || ? || '%'"
                authors.append(request_data.get('authors_full_name')[i])
    else:
        param_authors = '1'


    params = cl_word_par + date_par + issn_par + authors
    print(params)

    database = Database()
    connection = database.create_connection(DATABASE_DIRSAVE)
    cursor = connection.cursor()

    cursor.execute(""" SELECT title, surnames, doi, issn, date, address FROM
                    (SELECT article.*, GROUP_CONCAT(DISTINCT author.full_name) as surnames
                     FROM 
                        article LEFT JOIN auth_article 
                            ON article.id = auth_article.article_id
                        LEFT JOIN author
                            ON author.id = auth_article.auth_id
                    WHERE {first}
                        GROUP BY article.id ) 
                WHERE {second}
            """.format(first=param_words_in_art, second=param_authors), params)

    result = cursor.fetchall()
    print(len(result))
    print(result)

    status = http_status.HTTP_200_OK
    return '''{}'''.format(result)
    #return jsonify(data=return_data), status

if __name__ == '__main__':
    app.run(port = API_PORT, debug = 1)