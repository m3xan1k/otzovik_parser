import asyncio
import csv
from random import randint

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
    def get_next_page_link(self):
        next_anchor = self.find(class_='pager-item next tooltip-top').get('href')
        return next_anchor

    def get_reviews(self):
        pass


async def main():
    base_url = 'https://otzovik.com'
    url = '/reviews/bukmekerskaya_kontora_liga_stavok/'
    client = Downloader()
    html = await client.run(base_url + url)

    while html:
        print(url)
        soup = Parser(html, 'lxml')
        url = soup.get_next_page_link()
        html = await client.run(base_url + url)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
