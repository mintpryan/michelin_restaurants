import requests
import aiohttp
import csv
import ssl
import certifi
import asyncio
from dataclasses import dataclass
from aiohttp import ClientTimeout
from bs4 import BeautifulSoup, PageElement


ssl_context = ssl.create_default_context(cafile=certifi.where())

base_url = 'https://guide.michelin.com'
semaphore = asyncio.Semaphore(10)
timeout = ClientTimeout(total=30)


@dataclass
class Restaurant:
    name: str
    country: str = "Undefined"
    city: str = "Undefined"
    cuisine: str = "Undefined"
    
    def __str__(self) -> str:
        return f"Restaurant: {self.name}, cuisine: {self.cuisine}, Location: {self.city}, {self.country}"

    def __repr__(self) -> str:
        return f"Restaurant(name={self.name!r}, country={self.country!r}, city={self.city!r}, cuisine={self.cuisine!r})"


all_restaraunts = []


async def fetch_page(session, url):
    async with semaphore:  # Учитываем ограничение
        try:
            async with session.get(url, ssl=ssl_context, timeout=timeout) as response:
                await asyncio.sleep(2)
                return await response.text()
        except Exception as e:
            print(f"Error occurred: {e}")
            return None


def collect(item: PageElement) -> Restaurant:
    name = item.find(
        'h3', {'class': 'card__menu-content--title'}).a.string.strip()
    restaraunt = Restaurant(name.strip())
    meta_data = item.find_all(
        'div', {'class': 'card__menu-footer--score pl-text'})
    if len(meta_data) >= 1:
        address_array = meta_data[0].string.split(',')
        if len(address_array) == 2:
            city, country = address_array
            restaraunt.country = country.strip()
            restaraunt.city = city.strip()
        elif len(address_array) == 1:
            restaraunt.country =address_array[0].strip()
            restaraunt.city = address_array[0].strip()
    if len(meta_data) == 2:
        cuisine_element = meta_data[1].string.split('·')
        if len(cuisine_element) == 2:
            restaraunt.cuisine = cuisine_element[1].strip()

    return restaraunt


tasks = []


async def scrape():
    first_response = requests.get(base_url+'/en/restaurants')
    first_soup = BeautifulSoup(first_response.content, 'html.parser')
    pagination_element = first_soup.find(
        'div', {'class': 'js-restaurant__bottom-pagination'})

    if pagination_element:
        max_pages = pagination_element.find_all('li')[-2].a.string
        if max_pages:
            max_pages_int = int(max_pages)
            async with aiohttp.ClientSession(headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
            }) as session:
                for page_number in range(1, max_pages_int + 1):
                    tasks.append(fetch_page(
                        session, f'{base_url}/en/restaurants/page/{page_number}'))
                responses = await asyncio.gather(*tasks)
                for html in responses:
                    if html:
                        soup = BeautifulSoup(html, "html.parser")
                        restaurants = soup.find_all(
                            "div", class_="card__menu-content")
                        for restaurant in restaurants:
                            all_restaraunts.append(collect(restaurant))


def save_to_csv(filename, restaurants):
    fieldnames = ["name", "country", "city", "cuisine"]
    with open(filename, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        for restaurant in restaurants:
            writer.writerow({
                "name": restaurant.name,
                "country": restaurant.country,
                "city": restaurant.city,
                "cuisine": restaurant.cuisine,
            })
asyncio.run(scrape())
save_to_csv('michelin_restaurants_dataset.csv',all_restaraunts)