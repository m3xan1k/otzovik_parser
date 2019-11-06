import asyncio
import os
import logging

from bs4 import BeautifulSoup
import lxml

from base_parser import Parser, Writer


BASE_URL = 'https://irecommend.ru'
URLS = [
    # 'https://irecommend.ru/content/iviru',
    # 'https://irecommend.ru/content/sait-internet-kinoteatr-okko',
    'https://irecommend.ru/content/sait-megogonet',
    'https://irecommend.ru/content/tvzavrru',
    'https://irecommend.ru/content/amediateka-amediateka',
    # 'https://irecommend.ru/content/sait-onlain-kinoteatr-start',
    # 'https://irecommend.ru/content/mts-tv',
    # 'https://irecommend.ru/content/kompyuternaya-programma-filmy-na-google-play',
]


class IrecommendParser(Parser):
    '''
    Получаем все url на полные отзывы
    '''
    def get_reviews_urls(self, soup):
        anchors = soup.find_all(class_='more')
        return [f'{BASE_URL}/{a.get("href")}' for a in anchors]

    '''
    Собираем все данные
    '''
    def get_review_data(self, soup):
        author = soup.find('div', {'itemprop': 'author'}).text.strip()
        rate = soup.find('meta', {'itemprop': 'ratingValue'}).get('content')
        date = soup.find('meta', {'itemprop': 'datePublished'}).get('content')
        title = soup.find(class_='reviewTitle').text
        review_body_list = soup.find('div', {'itemprop': 'reviewBody'}).strings
        review_body = ''.join(review_body_list)
        normalized_date = self.normalize_date(date)
        return {
            'author': author,
            'rate': rate,
            'date': normalized_date,
            'title': title,
            'content': f'{title}\n{review_body}',
        }

    def normalize_date(self, date):
        date_only = date.split('T')[0]
        return date_only


async def main():
    logging.basicConfig(level=logging.DEBUG)
    parser = IrecommendParser()
    client = parser.downloader(
        # proxy_list='http_proxies.txt',
        ua_list='whatismybrowser-user-agent-database.txt'
    )
    fieldnames = [
        'author', 'publication_date', 'content',
        'rating', 'country', 'city',
        'plus', 'minus', 'title'
    ]

    for url in URLS:
        csv_filename = url.split('/')[-2]
        writer = Writer(
            dirpath='irecommend_results',
            filepath=csv_filename,
            fieldnames=fieldnames
        )
        writer.try_make_new_filepath_version()

        main_html = await client.failsafe_connect(url)
        logging.info(f'Connected to {url}')
        soup = BeautifulSoup(main_html, 'lxml')
        reviews_urls = parser.get_reviews_urls(soup)

        for r_url in reviews_urls:
            r_html = await client.failsafe_connect(r_url)
            r_soup = BeautifulSoup(r_html, 'lxml')
            data = parser.get_review_data(r_soup)
            print(data)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
