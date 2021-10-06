# -*- coding: utf-8 -*-
"""
Скрипт загрузки всех записей DOI с сайта crossref.org за сутки.
"""
import urllib.request
import urllib.parse
import urllib.error
import json
import datetime
from datetime import date, timedelta
import time
import logging
import re

CROSSREF_ENDPOINT = 'https://api.crossref.org'
CROSSREF_MAILTO = 'serg@msu.ru'

AUTHORS_MAPPING_ENDPOINT = 'http://127.0.0.1:5000/search/'
NAME_FILTERING_ENDPOINT = 'http://127.0.0.1:5010/filtering'
DEDUPLICATION_ENDPOINT = 'http://127.0.0.1:5020/'

logging.basicConfig(filename='doi_fetcher.log', level=logging.INFO)

def split_given_name(name):
    """Разделяет строчку given-name на поля имя (first name) и отчество (middle name)."""
    if not name:
        return None, None
    parts = re.split(r'[., \t]+', name)
    first_name = parts[0]
    middle_name = (' '.join(parts[1:])).strip(' \t-.,;')
    if middle_name == '':
        middle_name = None
    return first_name, middle_name


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
        post_args = urllib.parse.urlencode(params)
        actual_url = '{base}?{params}'.format(base=url, params=post_args)
    if data:
        post_data = json.dumps(data).encode('utf-8')
    request = urllib.request.Request(actual_url, headers={"Accept" : "application/json"}, data=post_data)
    if data:
        request.add_header('Content-Type', 'application/json; charset=utf-8')

    for attempt in range(1, 4):
        timeout = attempt * 10
        try:
            with urllib.request.urlopen(request) as response:
                message_bytes = response.read()
                message = message_bytes.decode('utf-8')
                return json.loads(message)
        except urllib.error.URLError as err:
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
    def show_item(self, item):
        """Отладочная печать записи."""
        authors = ', '.join(['{0} {1}'.format(a.get('given', ''), a.get('family')) for a in item.get('author')[:3] if a.get('family')])
        print(item.get('DOI'), authors, item.get('title'), item.get('event', {}).get('name'))

    def process_items(self, items, message):
        """Обработка метаданных публикаций.

        :param items: список записей с описанием статей в формате ответа crossref.org.
        :param message: полное сообщение crossref.
        """
        # Фильтруем по именам пользователей
        json_response = fetch_url(NAME_FILTERING_ENDPOINT, data=message) or {} # FIXME: better error handling
        indexes_to_keep = json_response.get("items", [])
        items_to_load = []
        for seq, item in enumerate(items):
            if seq in indexes_to_keep:
                items_to_load.append(item)
            items_found = True
        logging.info('%s out of %s items remain after name filtering.', len(items_to_load), len(items))
        if not items_to_load:
            return

        # for item in items_to_load[:10]:
        #     self.show_item(item)

        # Deduplication 
        dedup_request = {
            'items': items_to_load
        }
        json_response = fetch_url(DEDUPLICATION_ENDPOINT, data=dedup_request) or {} # FIXME: better error handling
        indexes_to_drop = json_response.get("items", [])
        if indexes_to_drop:
            new_items = []
            for seq, item in enumerate(items_to_load):
                if seq not in indexes_to_drop:
                    new_items.append(item)
            items_to_load = new_items
        logging.info('%s items remain after deduplication.', len(items_to_load))
        if not items_to_load:
            return

        # Определение авторов
        for item in items_to_load:
            authors_list = item.get('author', [])
            # Составим словарь авторов статьи в виде {'порядковый_номер': (F, I, O)} 
            original_authors = {}
            for seq, au in enumerate(authors_list):
                last_name = au.get('family')
                first_name, middle_name = split_given_name(au.get('given'))
                if not last_name: # пропускаем коллаборации
                    continue
                original_authors[str(seq)] = (last_name, first_name, middle_name)
            if original_authors == {}:
                continue
            # print(original_authors)
            # original_authors = {0: ('Afonin', 'S', None), 1: ('Zenzinov', 'A', None)]}, # DEBUG
            name_mapping_query = {
                'data': [[seq, fio] for seq, fio in original_authors.items()],
            }
            naming_mapping_data = fetch_url(AUTHORS_MAPPING_ENDPOINT, data=name_mapping_query)
            # пример: {'data': [{'0': 7759743, '1': 4705445}], 'msg': 'Search complete', 'stat': 'OK'}
            if naming_mapping_data and naming_mapping_data.get('stat') == 'OK':
                for naming_map in naming_mapping_data.get('data', []):
                    authors_found = False
                    for seq in original_authors.keys():
                        matched_id = naming_map.get(seq, -1)
                        if matched_id > -1:
                            print(seq, original_authors[seq], matched_id)
                            authors_found = True
                    if authors_found:
                        self.save_item(item, naming_map)
                        break;  # Сохраняем перый вариант выбора авторов
            else:
                print("An error has occurred: item:")
                self.show_item(item)
 
    def save_item(self, item, naming_map):
        """
        Функция сохранения метаданных публикации.
        :param item: запись из crossref.
        :param naming_map: словарь из порядковых номеров авторов в идентификаторы F_MAN_ID.
        """
        self.show_item(item)
        print()

    def process_response_chunk(self, data):
        """
        Обрабатывает все библиографические записи на однйо старнице выдачи.
        :return: курсор следующей страницы.
        :rtype: string or None
        """
        items_found = False
        next_crossref_cursor = None
        items = data.get('message', {}).get('items')
        if items:
            self.process_items(items, data['message'])
            next_crossref_cursor = data['message'].get('next-cursor')
            return next_crossref_cursor
        return None

    def retrieve_new_dois(self, start_date, end_date, initial_cursor='*'):
        """
        Скачевает все записи, начиная с даты start_date и заканчивая end_date (включитльно).
        Значение initial_cursor может использоваться для возобновления закачки предыдущего запроса.
        """
        chunk_size = 20 # Количество записей, которе crossref должен вернуть за один HTTP-запрос
        crossref_cursor = initial_cursor
        post_params = {
            'filter' : 'from-update-date:{start},until-update-date:{end}'.format(start=start_date, end=end_date),
            'rows': chunk_size,
            'mailto' : CROSSREF_MAILTO,
            'cursor' : crossref_cursor
            }
        url = '{base}/works'.format(base=CROSSREF_ENDPOINT)
        while crossref_cursor is not None:
            post_params['cursor'] = crossref_cursor
            json_response = fetch_url(url, post_params)
            if not json_response:
                break
            with open('data.json', 'a') as outfile:
                json.dump(json_response, outfile)
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


def main():
    """Загрузка за предыдущие сутки."""
    number_of_days_to_fetch = 1
    today = date.today()
    fetcher = CrossrefFetcher()
    for delay in range(number_of_days_to_fetch, 0, -1):
        yesterday = today - timedelta(delay)
        the_day_before_yesterday = yesterday - timedelta(1)
        logging.info('Fetching DOIs for %s...', the_day_before_yesterday)
        fetcher.retrieve_new_dois(start_date=the_day_before_yesterday, end_date=the_day_before_yesterday)
        logging.info('Done!')
    print("Done!")

main()
exit(0)

# Debug
fetcher = CrossrefFetcher()
fetcher.replay_cached_responses()
exit(0) 

