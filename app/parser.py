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


class Settings:
    BASE_URL = 'https://otzovik.com'
    BASE_DIR = os.path.dirname(os.path.realpath(__file__))
    URLS = [
        'https://otzovik.com/reviews/ivi_ru-besplatniy_videoservis_s_licenzionnim_polnometrazhnim_kontentom/',
        'https://otzovik.com/reviews/okko_tv-onlayn_kinoteatr/',
        'https://otzovik.com/reviews/megogo_net_onlayn-kinoteatr/',
        'https://otzovik.com/reviews/interaktivnoe_televidenie_wink_russia/',
        'https://otzovik.com/reviews/tvzavr_ru-on-layn_kinoteatr/',
        'https://otzovik.com/reviews/amediateka_ru-onlayn_kinoteatr/',
        'https://otzovik.com/reviews/start-kinoteatr_onlayn/',
        'https://otzovik.com/reviews/usluga_megafon_tv/',
        'https://otzovik.com/reviews/kinotv1_ru-onlayn_kinoteatr/',
        'https://otzovik.com/reviews/vipplay_ru_onlayn_kinoteatr/',
    ]


class Downloader:
    '''
    Класс для http запросов
    :params: proxy - строка с url или ip прокси-сервера 'http://proxyadress.com'
    '''
    def __init__(self, proxy_list=None, ua_list=None, timeout=15):
        '''
        proxy_list: файл со списком проксей
        ua_list: файл со списком user-агентов
        '''
        self.timeout = aiohttp.ClientTimeout(total=timeout)

        '''Если есть, то загружаем список проксей в память'''
        if proxy_list:
            with open(f'{Settings.BASE_DIR}/{proxy_list}', 'r') as f:
                self.proxy_list = f.readlines()
                self.proxy = random.choice(self.proxy_list)
        else:
            self.proxy_list = None
            self.proxy = None

        if ua_list:
            with open(f'{Settings.BASE_DIR}/{ua_list}') as f:
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


class Parser(BeautifulSoup):
    """
    Парсер html, он же нормализует данные
    """
    def get_next_page_url(self):
        next_anchor = self.find(class_='pager-item next tooltip-top')
        if next_anchor:
            return next_anchor.get('href')
        return

    def get_review_urls(self):
        review_btns = self.find_all(class_='review-btn review-read-link')
        return [btn.get('href') for btn in review_btns]

    '''
    Парсинг html кроме отзывов,
    если нормализация занимает больше строчки,
    то она вынесена в отдельный метод
    '''
    def get_data(self):
        logins = self.find_all(class_='user-login')
        normalized_logins = [{'login': login.text.strip()} for login in logins]
        dates = self.find_all(class_='review-postdate')
        normalized_dates = self.normalize_dates(dates)
        ratings = self.find_all(class_='product-rating tooltip-right')
        normalized_ratings = [{'rate': rate.get('title')[-1]} for rate in ratings]
        user_info = self.find_all(class_='user-info')
        countries, cities = self.normalize_places(user_info)
        data = [
            normalized_logins,
            normalized_dates,
            normalized_ratings,
            countries,
            cities
        ]
        return data

    '''
    Парсинг страницы с отзывом,
    разбираем плюсы, минусы и описание
    '''
    def get_review(self):
        plus = self.find(class_='review-plus')
        minus = self.find(class_='review-minus')
        content = self.find(class_='review-body description')
        summary = self.find(class_='summary')
        plus = plus.text if plus else ''
        minus = minus.text if minus else ''
        content = '\n'.join([string for string in content.strings]) if content else ''
        summary = summary.text if summary else ''
        return {
            'plus': plus,
            'minus': minus,
            'content': f'{summary}\n{plus}\n{minus}\n{content}',
            'summary': summary,
        }

    '''
    Парсим и нормализуем страну и город
    '''
    @classmethod
    def normalize_places(cls, user_info):
        places = [info.find_all('div')[-1].text for info in user_info]
        splitted_places = []
        for place in places:
            if ',' not in place:
                splitted_places.append([place, ''])
            else:
                place = place.split(', ')
                splitted_places.append(place)
        countries = [{'country': country} for country, city in splitted_places]
        cities = [{'city': city} for country, city in splitted_places]
        return countries, cities

    '''
    Немного преобразуем дату
    '''
    @classmethod
    def normalize_dates(cls, dates):
        normalized_dates = []
        for date_val in dates:
            date_val = date_val.text.strip()
            date_val = date_val.split('.')
            date_val = list(map((lambda x: int(x)), date_val))
            day, month, year = date_val
            normalized_dates.append({'date': datetime(year, month, day).strftime('%d-%m-%Y')})
        return normalized_dates


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
        self.dirpath = os.path.join(Settings.BASE_DIR, dirpath)
        self.filepath = os.path.join(Settings.BASE_DIR, dirpath, filepath)
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


async def main():
    # TODO collect dead proxies to skip them
    logging.basicConfig(level=logging.DEBUG)

    '''
    Готовим первый запрос — собираем url,
    инициализируем загрузчика и выбираем новый прокси,
    новый user-agent, инициализируем счетчик переподключений
    '''
    base_url = Settings.BASE_URL

    client = Downloader(
        proxy_list='http_proxies.txt',
        ua_list='whatismybrowser-user-agent-database.txt'
    )
    fieldnames = [
        'login', 'date', 'content',
        'rate', 'country', 'city',
        'plus', 'minus', 'summary'
    ]

    for url in Settings.URLS:
        '''
        Задаем имя csv-файла по названию url-а страницы,
        если уже существует,
        то создаем новую версию
        '''
        csv_filename = url.split('/')[-2]
        writer = Writer(
            dirpath='results',
            filepath=f'{csv_filename}_1.csv',
            fieldnames=fieldnames
        )
        writer.try_make_new_filepath_version()

        '''
        По скольку на сайте нельзя понять общее количество страниц,
        запускаем бесконечный цикл, пока страницы есть
        '''
        while True:
            html = await client.failsafe_connect(url)
            logging.info(f'Connected {url}')
            soup = Parser(html, 'lxml')
            data = soup.get_data()

            '''Отзывы находятся на отдельных страницах'''
            review_urls = soup.get_review_urls()
            for r_url, t_row in zip(review_urls, zip(*data)):
                client.set_new_proxy()
                logging.info(f'Current proxy={client.proxy}')

                '''Пытаемся скачать html со страницы отзывов'''
                # TODO убрать дублирование этой конструкции
                r_html = await client.failsafe_connect(r_url)
                logging.info('Request passed')

                '''Парсим html'''
                r_soup = Parser(r_html, 'lxml')
                review = r_soup.get_review()

                '''Сцепляем данные и записываем в csv'''
                row = {}
                for elem in t_row:
                    row.update(elem)
                row.update(review)
                writer.write_row(row)
            logging.info(f'All reviews on {url} done')

            '''
            Находим url следующей страницы,
            если нет, значит страница последняя,
            если да меняем проксю
            '''
            url = soup.get_next_page_url()
            if not url:
                logging.info('Last page done')
                break
            url = base_url + url
            client.set_new_proxy()
            client.set_new_user_agent()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
