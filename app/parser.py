import asyncio
import csv
from random import randint
from datetime import datetime, date

from bs4 import BeautifulSoup
import aiohttp
import lxml


class Downloader:
    async def fetch(self, session, url):
        async with session.get(url) as response:
            if response.status == 404:
                return
            return await response.text()

    async def run(self, url):
        async with aiohttp.ClientSession() as client:
            await asyncio.sleep(randint(5, 10))
            html = await self.fetch(client, url)
            return html


class Parser(BeautifulSoup):
    def get_next_page_url(self):
        next_anchor = self.find(class_='pager-item next tooltip-top')
        if next_anchor:
            return next_anchor.get('href')
        return

    def get_review_urls(self):
        review_btns = self.find_all(class_='review-btn review-read-link')
        return [btn.get('href') for btn in review_btns]

    def get_data(self):
        data = []
        logins = self.find_all(class_='user-login')
        normalized_logins = [login.text.strip() for login in logins]
        dates = self.find_all(class_='review-postdate')
        normalized_dates = self.normalize_dates(dates)
        ratings = self.find_all(class_='product-rating tooltip-right')
        normalized_ratings = [rate.get('title')[-1] for rate in ratings]
        user_info = self.find_all(class_='user-info')
        countries, cities = self.normalize_places(user_info)
        review_urls = self.get_review_urls()
        self.get_reviews(review_urls)

    def get_reviews(self, urls):
        for url in urls:
            print(url)

    @classmethod
    def normalize_places(cls, user_info):
        places = [info.find_all('div')[-1].text for info in user_info]
        splitted_places = []
        for place in places:
            place = place.split(', ')
            splitted_places.append(place)
        countries = [country for country, city in splitted_places]
        cities = [city for country, city in splitted_places]
        return countries, cities

    @classmethod
    def normalize_dates(cls, dates):
        normalized_dates = []
        for date_val in dates:
            date_val = date_val.text.strip()
            date_val = date_val.split('.')
            date_val = list(map((lambda x: int(x)), date_val))
            day, month, year = date_val
            normalized_dates.append(datetime(year, month, day))
        return normalized_dates



async def main():
    base_url = 'https://otzovik.com'
    url = '/reviews/bukmekerskaya_kontora_liga_stavok/'
    client = Downloader()
    html = await client.run(base_url + url)

    while url:
        soup = Parser(html, 'lxml')
        soup.get_info()
        url = soup.get_next_page_url()
        if not url:
            break
        html = await client.run(base_url + url)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
