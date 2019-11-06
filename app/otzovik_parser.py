from datetime import datetime
import logging
import os
import asyncio

from bs4 import BeautifulSoup

from base_parser import Parser, Writer


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


class OtzovikParser(Parser):
    """
    Парсер html, он же нормализует данные
    """
    def get_next_page_url(self, soup):
        next_anchor = soup.find(class_='pager-item next tooltip-top')
        if next_anchor:
            return next_anchor.get('href')
        return

    def get_review_urls(self, soup):
        review_btns = soup.find_all(class_='review-btn review-read-link')
        return [btn.get('href') for btn in review_btns]

    '''
    Парсинг html кроме отзывов,
    если нормализация занимает больше строчки,
    то она вынесена в отдельный метод
    '''
    def get_data(self, soup):
        logins = soup.find_all(class_='user-login')
        normalized_logins = [{'login': login.text.strip()} for login in logins]
        dates = soup.find_all(class_='review-postdate')
        normalized_dates = self.normalize_dates(dates)
        ratings = soup.find_all(class_='product-rating tooltip-right')
        normalized_ratings = [{'rate': rate.get('title')[-1]} for rate in ratings]
        user_info = soup.find_all(class_='user-info')
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
    def get_review(self, soup):
        plus = soup.find(class_='review-plus')
        minus = soup.find(class_='review-minus')
        content = soup.find(class_='review-body description')
        summary = soup.find(class_='summary')
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


async def main():
    # TODO collect dead proxies to skip them
    logging.basicConfig(level=logging.DEBUG)

    '''
    Готовим первый запрос — собираем url,
    инициализируем загрузчика и выбираем новый прокси,
    новый user-agent, инициализируем счетчик переподключений
    '''
    base_url = BASE_URL

    parser = OtzovikParser()
    client = parser.downloader(
        proxy_list='http_proxies.txt',
        ua_list='whatismybrowser-user-agent-database.txt'
    )
    fieldnames = [
        'login', 'date', 'content',
        'rate', 'country', 'city',
        'plus', 'minus', 'summary'
    ]

    for url in URLS:
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
            soup = BeautifulSoup(html, 'lxml')
            data = parser.get_data(soup)

            '''Отзывы находятся на отдельных страницах'''
            review_urls = parser.get_review_urls(soup)
            for r_url, t_row in zip(review_urls, zip(*data)):
                client.set_new_proxy()
                logging.info(f'Current proxy={client.proxy}')

                '''Пытаемся скачать html со страницы отзывов'''
                # TODO убрать дублирование этой конструкции
                r_html = await client.failsafe_connect(r_url)
                logging.info('Request passed')

                '''Парсим html'''
                r_soup = BeautifulSoup(r_html, 'lxml')
                review = parser.get_review(r_soup)

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
            url = parser.get_next_page_url(soup)
            if not url:
                logging.info('Last page done')
                break
            url = base_url + url
            client.set_new_proxy()
            client.set_new_user_agent()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
