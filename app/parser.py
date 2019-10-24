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

import settings


class Downloader:
    """
    Класс для http запросов
    :params: proxy - строка с url или ip прокси-сервера 'http://proxyadress.com'
    """
    def __init__(self, proxy=None, user_agent=None):
        self.proxy = proxy
        self.user_agent = user_agent
        self.timeout = aiohttp.ClientTimeout(total=15)

    def set_new_proxy(self):
        with open('app/http_proxies.txt', 'r') as f:
            lines = f.readlines()
            proxy = random.choice(lines)
        self.proxy = proxy

    def set_new_user_agent(self):
        with open('app/whatismybrowser-user-agent-database.txt') as f:
            lines = f.readlines()
            user_agent = random.choice(lines)
        self.user_agent = user_agent

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

    def get_review(self):
        plus = self.find(class_='review-plus')
        minus = self.find(class_='review-minus')
        description = self.find(class_='review-body description')
        plus = plus.text if plus else ''
        minus = minus.text if minus else ''
        description = description.text if description else ''
        return {
            'plus': plus,
            'minus': minus,
            'description': f'{plus} {minus} {description}',
        }

    @classmethod
    def normalize_places(cls, user_info):
        places = [info.find_all('div')[-1].text for info in user_info]
        splitted_places = []
        for place in places:
            place = place.split(', ')
            splitted_places.append(place)
        countries = [{'country': country} for country, city in splitted_places]
        cities = [{'city': city} for country, city in splitted_places]
        return countries, cities

    @classmethod
    def normalize_dates(cls, dates):
        normalized_dates = []
        for date_val in dates:
            date_val = date_val.text.strip()
            date_val = date_val.split('.')
            date_val = list(map((lambda x: int(x)), date_val))
            day, month, year = date_val
            normalized_dates.append({'date': datetime(year, month, day)})
        return normalized_dates


class Writer:
    def __init__(self, filepath):
        self.filepath = filepath

    def write(self, data):
        if os.path.isfile(self.filepath):
            mode = 'a'
        else:
            mode = 'w'
        with open(self.filepath, mode) as file:
            fieldnames = [
                'login', 'date', 'description',
                'rate', 'country', 'city',
                'plus', 'minus'
            ]
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            if mode == 'w':
                writer.writeheader()
            for row in data:
                writer.writerow(row)


async def main():
    # TODO collect dead proxies to skip them
    logging.basicConfig(level=logging.DEBUG)
    all_data = []

    '''
    Готовим первый запрос — собираем url,
    инициализируем загрузчика и выбираем новый прокси
    '''
    base_url = settings.BASE_URL
    url = '/reviews/bukmekerskaya_kontora_liga_stavok/'
    client = Downloader()
    client.set_new_proxy()
    client.set_new_user_agent()
    reconnect_counter = 0

    '''
    По скольку на сайте нельзя понять общее количество страниц,
    запускаем бесконечный цикл, пока страницы есть
    '''
    while True:
        '''Частенько может возникнуть исключение,
        если прокся не доступна,
        тогда пробуем новую проксю и так до 10ти раз
        '''
        while True:
            if reconnect_counter > 10:
                logging.warning('Reconnect counter is more than 10!')
            try:
                logging.info(f'Current proxy={client.proxy}, requesting {base_url + url}')
                html = await client.run(base_url + url)
                if not html:
                    client.set_new_proxy()
                    client.set_new_user_agent()
                    continue
                reconnect_counter = 0
                break
            except:
                reconnect_counter += 1
                client.set_new_proxy()
                client.set_new_user_agent()
        logging.info(f'Connected {base_url + url}')
        soup = Parser(html, 'lxml')
        data = soup.get_data()

        '''Отзывы находятся на отдельных страницах'''
        reviews = []
        review_urls = soup.get_review_urls()
        for r_url in review_urls:
            client.set_new_proxy()
            logging.info(f'Current proxy={client.proxy}')

            while True:
                if reconnect_counter > 10:
                    logging.warning('Reconnect counter is more than 10!')
                try:
                    logging.info(f'Requesting {r_url}')
                    r_html = await client.run(r_url)
                    if not r_html:
                        client.set_new_proxy()
                        client.set_new_user_agent()
                        continue
                    reconnect_counter = 0
                    break
                except:
                    reconnect_counter += 1
                    client.set_new_proxy()
                    client.set_new_user_agent()
                    logging.warning(f'Try to set new proxy={client.proxy}')
            logging.info('Request passed')
            r_soup = Parser(r_html, 'lxml')
            reviews.append(r_soup.get_review())
        logging.info(f'All reviews on {url} done')
        data.append(reviews)
        all_data.append(zip(*data))
        logging.info(f'all_data={all_data}')

        '''
        Находим url следующей страницы,
        если нет, значит страница последняя,
        если да меняем проксю
        '''
        url = soup.get_next_page_url()
        if not url:
            logging.info('Last page done')
            break
        client.set_new_proxy()
        client.set_new_user_agent()
        for block in all_data:
            for item in block:
                print(item)

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
