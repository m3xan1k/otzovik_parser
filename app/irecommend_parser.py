import asyncio
import os
import logging
from datetime import datetime

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
        author = soup.find('div', {'itemprop': 'author'})
        rating = soup.find('meta', {'itemprop': 'ratingValue'})
        date = soup.find('meta', {'itemprop': 'datePublished'})
        title = soup.find(class_='reviewTitle')
        review_body_list = soup.find('div', {'itemprop': 'reviewBody'})
        review_body = self.normalize_review_body(review_body_list) if review_body_list else ''
        normalized_date = self.normalize_date(date.get('content')) if date else ''
        minus = soup.find(class_='minus')
        normalized_minus = self.normalize_pros_and_cons(minus)
        plus = soup.find(class_='plus')
        normalized_plus = self.normalize_pros_and_cons(plus)
        return {
            'author': author.text.strip() if author else '',
            'rating': rating.get('content') if rating else '',
            'publication_date': normalized_date,
            'collection_date': datetime.today(),
            'title': title.text if title else '',
            'content': f'{title}\n{normalized_plus}\n{normalized_minus}\n{review_body}',
            'positive': normalized_plus,
            'negative': normalized_minus,
            'source': BASE_URL.split('/')[-1],
        }

    def normalize_date(self, date):
        date_only = date.split('T')[0]
        return date_only

    def normalize_pros_and_cons(self, ratio):
        if ratio:
            items = ratio.find_all('li')
            return ' '.join([item.text for item in items])
        return ''

    def normalize_review_body(self, body):
        to_replace = '\n' * 5
        return ''.join(body.strings).replace(to_replace, '\n') if body else ''



async def main():
    logging.basicConfig(level=logging.DEBUG)
    parser = IrecommendParser()
    client = parser.downloader(
        # proxy_list='http_proxies.txt',
        ua_list='whatismybrowser-user-agent-database.txt'
    )
    fieldnames = [
        'title', 'content', 'author', 'rating',
        'positive', 'negative', 'publication_date', 
        'collection_date', 'source'
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
