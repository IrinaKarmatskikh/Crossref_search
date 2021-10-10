# -*- coding: utf-8 -*-
"""
Скрипт загрузки всех записей DOI с сайта crossref.org за сутки.
"""
import urllib.request
import urllib.parse
import urllib.error
import json
from datetime import date, timedelta
import time
import logging
import os
import gzip
import sqlite3
from sqlite3 import Error
import ast

CROSSREF_ENDPOINT = 'https://api.crossref.org'
CROSSREF_MAILTO = 'serg@msu.ru'
CROSSREF_DIRSAVE = r'C:\Users\Ирина\Documents\Search (for medic)\JSON\ '
DATABASE_DIRSAVE = r'C:\Users\Ирина\Documents\Search (for medic)\DATABASE\db_python_app.sqlite'

logging.basicConfig(filename='doi_fetcher.log', level=logging.INFO)


#ok
def fetch_url(url, params=None, data=None):
    """
    Загрузка JSON по указанному адресу URL.

    Выполняет обращение к адресу URL с параметрами PARAMS. Параметры PARAMS кодируются в адрксной строке.
    Если DATA не задано, то выполняется GET-запрос. Иначе словарь DATA передается в POST-запросе
    в формате JSON. Возвращает полученный JSON-ответ в виде словаря, либо None, если не удалось
    загрузить данные. 

    :param url: Базовый адрес ресурса, без закодированных параметров.
    :type url: str
    :param params: словарь параметров для подстановки в URL.
    :type params: dict, optional
    :param data: данные для отправки на сервер в POST запросе.
    :type data: dict, optional
    :return: JOSN-сообщение, полученное от сервера.
    :rtype: dict or None
    """
    actual_url, post_data = url, None
    if params:
        #строка представляющая ряд пар key=value, разделенных символами '&'.
        post_args = urllib.parse.urlencode(params)
        # полный адрес откуда мы скачаем
        actual_url = '{base}?{params}'.format(base=url, params=post_args)
    if data:#??
        post_data = json.dumps(data).encode('utf-8')
    #This class is an abstraction of a URL request.
    request = urllib.request.Request(actual_url, headers={"Accept" : "application/json"}, data=post_data)
    if data:
        request.add_header('Content-Type', 'application/json; charset=utf-8')

    for attempt in range(1, 4):
        timeout = attempt * 10
        try:
            with urllib.request.urlopen(request) as response:
                # response -  class http.client.HTTPResponse(sock, debuglevel=0, method=None, url=None)
                # Class whose instances are returned upon save_itemsessful connection. Not instantiated directly by user.
                message_bytes = response.read() #read?
                message = message_bytes.decode('utf-8')
                print("Success! 20 new downloads")
                return json.loads(message) #json в объект питона
        except urllib.error.URLError as err:
            print(err)
            logging.warning('Fetch error: %s', err)
            logging.warning('Complete URL: %s', actual_url)
            logging.warning('Waiting for %s seconds before next download attempt.', timeout)
            time.sleep(timeout)
    logging.error('Unable to download URL: %s', actual_url)
    return None


class CrossrefFetcher:
    """
    Загрузчик из crossref.org.
    """
    def process_response_chunk(self, data):
        """
        Обрабатывает все библиографические записи на однйо старнице выдачи.
        :return: курсор следующей страницы.
        :rtype: string or None
        """
        items_found = False
        next_crossref_cursor = None
        items = data.get('message', {}).get('items') #??
        if items:
            next_crossref_cursor = data['message'].get('next-cursor')
            return next_crossref_cursor
        return None
#Ok
    def retrieve_new_dois(self, start_date, end_date, connection, initial_cursor='*'):
        """
        Скачевает все записи, начиная с даты start_date и заканчивая end_date (включитльно).
        Значение initial_cursor может использоваться для возобновления закачки предыдущего запроса. (А, типа если не все закачи начать с того же места?)
        """
        chunk_size = 20 # Количество записей, которе crossref должен вернуть за один HTTP-запрос
        crossref_cursor = initial_cursor # ??
        # словарь параметров
        post_params = {
            'filter': 'from-update-date:{start},until-update-date:{end}'.format(start=start_date, end=end_date),
            'rows': chunk_size,
            'mailto': CROSSREF_MAILTO,
            'cursor': crossref_cursor
            }

        #откуда загружаем
        url = '{base}/works'.format(base=CROSSREF_ENDPOINT)
        i = 0
        while crossref_cursor is not None:
            #записываем курсор в словарь
            post_params['cursor'] = crossref_cursor
            json_response = fetch_url(url, post_params)
            if not json_response:
                break
            save = CrossrefSave()
            save.save_items(json_response, i, start_date, connection)
            i = i + 1
            print("Success! 20 new files")
            print("Success! 20 records added to the database")
            crossref_cursor = self.process_response_chunk(json_response)
            logging.info('Next cursor: %s', crossref_cursor)

    def replay_cached_responses(self):
        """Отладочная функция для загрузки ранее скаченных файлов."""
        import glob
        for filepath in glob.iglob('/space/1G/serg/crossref/confs/crossref/proc-04*.json'):
            with open(filepath) as f:
                data = json.load(f)
                json_response = data if data.get('message') else {'message': data}
                # print(filepath, len(json_response.get('message', {}).get('items', [])))
                self.process_response_chunk(json_response)


class CrossrefSave:
    def save_items(self, json_response, number_dirs, start_date, connection):
        items = json_response.get('message', {}).get('items')
        data = start_date.strftime('%d-%m-%Y')
        dir_name = str(number_dirs)
        os.makedirs(CROSSREF_DIRSAVE + data + r'\ ' + dir_name, exist_ok = True)
        data_base = Database()

        if items:
            for i in range(len(items)):
                i_str = str(i)
                full_dir_name = CROSSREF_DIRSAVE + data + r'\ ' + dir_name + r'\ ' + i_str + r'.json.gz'
                with gzip.GzipFile(full_dir_name, 'w') as outfile:
                    outfile.write(json.dumps(items[i]).encode('utf-8'))
                #connection = data_base.create_connection(DATABASE_DIRSAVE)
                data_base.insert_record(items[i], full_dir_name, connection)


class Database:

    def create_connection(self, path):
        connection = None
        try:
            connection = sqlite3.connect(path)
            print("Connection to SQLite DB successful")
            logging.info('Connection to SQLite DB successful')
        except Error as e:
            print(f"The error '{e}' occurred")
            logging.info(f"The error '{e}' occurred")
        return connection

    def execute_query(self, connection, query):
        cursor = connection.cursor()
        try:
            cursor.execute(query)
            connection.commit()
        except Error as e:
            logging.info(f"The error '{e}' occurred")

    def execute_read_query(self,connection, query):
        cursor = connection.cursor()
        result = None
        try:
            cursor.execute(query)
            result = cursor.fetchall()
            return result
        except Error as e:
            print(f"The error '{e}' occurred")

    def count_records(self, connection, table_name):
        query = """SELECT
         Count(*)
         FROM """ + table_name
        result = self.execute_read_query(connection, query)
        #print (result, table_name)
        return result

    def create_table(self, connection):
        create_pragma = """
         PRAGMA foreign_keys = on;
        """

        create_article_table = """
        CREATE TABLE IF NOT EXISTS article(
         id INTEGER PRIMARY KEY AUTOINCREMENT,
         doi TEXT NOT NULL,
         title TEXT NOT NULL,
         data TEXT,
         address TEXT,
         UNIQUE(id, doi, title, data, address)
        );
        """

        create_auth_table = """
        CREATE TABLE IF NOT EXISTS auth(
         id INTEGER PRIMARY KEY  AUTOINCREMENT,
         name TEXT NOT NULL,
         UNIQUE(id, name)
        );
        """

        create_auth_article_table = """ 
        CREATE TABLE IF NOT EXISTS auth_article (
         auth_id INTEGER NOT NULL,
         article_id INTEGER NOT NULL,
         FOREIGN KEY (auth_id) REFERENCES auth(id),
         FOREIGN KEY (article_id) REFERENCES article(id)
        );
        """

        self.execute_query(connection, create_pragma)
        self.execute_query(connection, create_article_table)
        self.execute_query(connection, create_auth_table)
        self.execute_query(connection, create_auth_article_table)

        print("Database successfully created")
        logging.info('Database successfully created')

    def insert_record(self, item, address, connection):
        try:
            doi_it = item.get('DOI')
            title_it = item.get('title')
            dat_it = item.get('indexed').get('date-time')
            doi, title, dat = "", "", ""
            if doi_it != None and title_it != None and dat_it != None:
                doi = str(doi_it).replace("'", "''")
                title = str(title_it[0]).replace("'", "''")
                dat = str(dat_it).replace("'", "''")

                cursor = connection.cursor()
                cursor.execute("select * from article where doi =?", (doi,))
                result = cursor.fetchall()
                if len(result) == 0:
                    insert_article = """ 
                    INSERT OR IGNORE INTO
                    article(doi, title, data, address)
                    VALUES 
                    ('""" + doi + """', '""" + title + """', '""" + dat + """', '""" + address + """');
                    """
                    self.execute_query(connection, insert_article)
                    logging.info('Title, DOI, data successfully inserted. Total records in article now %s', self.count_records(connection, "article"))

                it = item.get('author')
                if it != None:
                    for j in range(len(item.get('author'))):
                        auth = item.get('author')[j].get('family')

                        cursor = connection.cursor()
                        cursor.execute("select * from auth where name =?", (auth,))
                        result = cursor.fetchall()

                        if len(result) == 0:
                            insert_auth = """
                            INSERT OR REPLACE INTO auth (name)
                            VALUES ('""" + str(auth) + """');
                            """
                            self.execute_query(connection, insert_auth)

                        logging.info('List of authors successfully inserted. Total records in authors now %s',
                                     self.count_records(connection, "auth"))

                        cursor = connection.cursor()
                        cursor.execute("select * from auth where name =?", (auth,))
                        result = cursor.fetchall()
                        id_auth, id_art = "", ""
                        for row in result:
                            id_auth = str(row[0])

                        cursor.execute("select * from article where doi = ?", (doi,))
                        result = cursor.fetchall()
                        for row in result:
                            id_art = str(row[0])

                        insert_auth_article = """
                         INSERT OR IGNORE INTO auth_article (auth_id, article_id)
                         VALUES (' """ + id_auth + """ ', '""" + id_art + """' );
                         """
                        self.execute_query(connection, insert_auth_article)
                        logging.info("id_auth , id_art successfully inserted in auth_art")

        except Error as e:
            print(f"The error in insert :'{e}' file", address)
            logging.info(f"The error in insert :'{e}' file", address)

def main():
    """Загрузка за предыдущие сутки."""
    # количество дней для загрузки
    number_of_days_to_fetch = 1
    # объект datetime из текущей даты и времени
    today = date.today()
    # создаем элемент класса CrossrefFetcher
    fetcher = CrossrefFetcher()
    database = Database()
    connection = database.create_connection(DATABASE_DIRSAVE)
    database.create_table(connection)
    for delay in range(number_of_days_to_fetch, 0, -1):
        yesterday = today - timedelta(delay)
        the_day_before_yesterday = yesterday - timedelta(1)
        logging.info('Fetching DOIs for %s...', the_day_before_yesterday)

        data = the_day_before_yesterday.strftime('%d-%m-%Y')
        newDirName = data
        folder = os.makedirs(r'C:\Users\Ирина\Documents\Search (for medic)\JSON\ ' + newDirName, exist_ok=True)
        #Скачевает все записи, начиная с даты start_date и заканчивая end_date

        fetcher.retrieve_new_dois(start_date=the_day_before_yesterday, end_date=the_day_before_yesterday, connection=connection)
        #Все
        logging.info('Done!')
    print("Done!")

main()
exit(0)

# Debug
fetcher = CrossrefFetcher()
fetcher.replay_cached_responses()
exit(0) 

