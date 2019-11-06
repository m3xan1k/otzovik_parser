import asyncio
import csv
import os
import random
import logging
from datetime import datetime

from bs4 import BeautifulSoup
import aiohttp
import aiofiles
import lxml


BASE_DIR = os.path.dirname(os.path.realpath(__file__))


class Downloader:
    '''
    Класс для http запросов
    :params: proxy - строка с url или ip прокси-сервера 'http://proxyadress.com'
    '''
    @classmethod
    def create(cls):
        pass

    def __init__(self, proxy_list=None, ua_list=None, timeout=15):
        '''
        proxy_list: файл со списком проксей
        ua_list: файл со списком user-агентов
        '''
        self.timeout = aiohttp.ClientTimeout(total=timeout)

        '''Если есть, то загружаем список проксей в память'''
        if proxy_list:
            with open(f'{BASE_DIR}/{proxy_list}', 'r') as f:
                self.proxy_list = f.readlines()
                self.proxy = random.choice(self.proxy_list)
        else:
            self.proxy_list = None
            self.proxy = None

        if ua_list:
            with open(f'{BASE_DIR}/{ua_list}') as f:
                self.ua_list = f.readlines()
                self.user_agent = random.choice(self.ua_list)
        else:
            self.ua_list = None
            self.user_agent = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36'

    def set_new_proxy(self):
        if self.proxy_list:
            self.proxy = random.choice(self.proxy_list)

    def set_new_user_agent(self):
        if self.ua_list:
            self.user_agent = random.choice(self.ua_list)

    '''
    Частенько может возникнуть исключение,
    если прокся не доступна,
    тогда пробуем новую проксю и так до 10ти раз
    '''
    async def failsafe_connect(self, url):
        reconnect_counter = 0
        while True:
            if reconnect_counter > 10:
                logging.warning('Reconnect counter is more than 10!')
            try:
                logging.info(f'Current proxy={self.proxy}, requesting {url}')
                html = await self.run(url)
                if not html:
                    logging.warning(html)
                    self.set_new_proxy()
                    self.set_new_user_agent()
                    continue
                return html
            except Exception as e:
                logging.warning(e)
                reconnect_counter += 1
                self.set_new_proxy()
                self.set_new_user_agent()
                logging.warning(f'Try to set new proxy={self.proxy}')

    async def fetch(self, session, url):
        async with session.get(url, proxy=self.proxy) as response:
            if response.status != 200:
                return
            return await response.text()

    async def run(self, url):
        async with aiohttp.ClientSession(timeout=self.timeout, headers={'User-Agent': self.user_agent}) as client:
            # await asyncio.sleep(random.randint(2, 5))
            html = await self.fetch(client, url)
            return html


class Parser:
    downloader = Downloader


class Writer:
    '''
    Класс для записи данных в csv-файл,
    всё банально на стандартной библиотеке
    '''
    def __init__(self, dirpath, filepath, fieldnames):
        '''
        dirpath: имя папки
        filepath: имя файла
        fieldnames: поля csv-файла
        '''
        self.dirpath = os.path.join(BASE_DIR, dirpath)
        self.filepath = os.path.join(BASE_DIR, dirpath, filepath)
        self.fieldnames = fieldnames

    '''
    Если csv-файл существует,
    прибавляем единицу к индексу в названии
    '''
    def try_make_new_filepath_version(self):
        while os.path.isfile(self.filepath):
            version = int(self.filepath.split('_')[-1].replace('.csv', ''))
            base = '_'.join(self.filepath.split('_')[:-1])
            self.filepath = f'{base}_{version + 1}.csv'

    def write_row(self, row):
        if not os.path.exists(self.dirpath):
            os.mkdir(self.dirpath)
        if os.path.isfile(self.filepath):
            mode = 'a'
        else:
            mode = 'w'
        with open(self.filepath, mode) as file:
            writer = csv.DictWriter(file, fieldnames=self.fieldnames)
            if mode == 'w':
                writer.writeheader()
            writer.writerow(row)
