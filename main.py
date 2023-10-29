import asyncio
import datetime
from csv import DictWriter, DictReader
from time import time

from pyppeteer import launch
import pyppeteer.errors
from fake_useragent import UserAgent
from bs4 import BeautifulSoup


# создание браузера
async def create_browser():
    # у меня не работало с Chromium'ом, который скачался при первом запуске
    # поэтому я установил путь до Google Chrome в системе
    browser = await launch({'headless': True,
                            'executablePath': '/usr/bin/google-chrome-stable'})
    return browser


# заходим на нужную страницу и возвращаем отрендернный html и время визита
async def scrape_page(page, ua):
    await page.setUserAgent(ua.random)
    await page.goto(f'https://announcements.bybit.com/en-US/?category=&page=1&1={int(time())}')
    visit_time = datetime.datetime.now()
    html_content = await page.content()
    return html_content, visit_time


# функция в виде класса, чтобы сохранять между вызовами состояние переменной Last_news_title
class Parsing:
    def __init__(self, domain, last_news_title):
        self.domain = domain
        self.last_news_title = last_news_title

    def __call__(self, html_code, visit_time):
        soup = BeautifulSoup(html_code, 'lxml')
        news_data = soup.select_one('a.no-style span:only-child')
        news_title = " ".join(news_data.text.strip().split())
        if news_title != self.last_news_title:
            self.last_news_title = news_title
            link = f"{self.domain}{news_data.find_parent('a')['href']}"
            d = {'time': visit_time, 'title': self.last_news_title, 'link': link}
            with open('data/news.csv', 'a') as file:
                writer = DictWriter(file, fieldnames=list(d.keys()), dialect='excel')
                writer.writeheader()
                writer.writerow(d)
            print("Новая новость записана")
        else:
            print("Новых новостей нет")


async def job(parsing, page, ua):
    html_code, visit_time = await scrape_page(page, ua)
    parsing(html_code, visit_time)


async def main(parsing):
    our_browser = await create_browser()
    user_agent = UserAgent()
    page = await our_browser.newPage()
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3",
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
    }
    await page.setExtraHTTPHeaders(headers)
    try:
        while True:
            job_task = asyncio.create_task(job(parsing, page, user_agent))
            await asyncio.gather(job_task, asyncio.sleep(1))
    # раз в 15-20 попыток возвращается исключение, вызванное ошибкой HTTP2
    # поэтому я перезапускаю браузер в случае таких ошибок
    except pyppeteer.errors.NetworkError or pyppeteer.errors.PageError:
        print("Ошибка, перезапуск браузера")
        await our_browser.close()


if __name__ == '__main__':
    #Пробуем считать последний записанный в файле заголовок новости
    try:
        with open('data/news.csv', 'r') as file:
            reader = DictReader(file, fieldnames=['time', 'title', 'link'], dialect='excel')
            last_row = None
            for row in reader:
                last_row = row
            if last_row:
                last_news_title_from_file = last_row['title']
    except FileNotFoundError:
        last_news_title_from_file = ""
    parsing = Parsing("https://announcements.bybit.com", last_news_title=last_news_title_from_file)
    while True:
        asyncio.run(main(parsing))
