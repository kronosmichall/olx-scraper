import requests
import re

from bs4 import BeautifulSoup
from datetime import datetime

def get_links(url):
    olx = 'https://www.olx.pl'
    res = requests.get(url)
    soup = BeautifulSoup(res.content, 'lxml')

    container = soup.select('[data-testid="listing-grid"]')
    if not container: return []
    
    container = container[0]
    links = [link['href'] for link in container.find_all('a', href=True)]
    links = [olx + link if not link.startswith('http') else link for link in links]
    
    return links


def price_to_int(price):
    price = price.split(",")[0]
    digits = re.findall(r'\d', price)
    string = ''.join(digits)
    price = 0 if not string else int(string)

    return price


def find_address(content):
    pattern_nr = r'ul.*\d+'
    pattern_no_nr =r'ul\.?\s\w+'
    reg = re.compile(r'({})|({})'.format(pattern_nr, pattern_no_nr), re.IGNORECASE)
    return re.search(reg, content).group()

def get_time_in_minutes(time_str):
    x = time_str.split('hours')
    if len(x) == 1: return int(re.search(r'\d+', x[0]).group())
    
    hours = int(re.search(r'\d+', x[0]).group())
    minutes = int(re.search(r'\d+', x[1]).group())
    return hours * 60 + minutes
    
def time_to_centre(origin, gmaps_client):
    if origin == None: return None

    destinations = [
        "Warszawa, Metro Świętokrzyska",
        "Warszawa, PKP Śródmieście",
    ]
    departure_time = datetime.now().replace(hour=8, minute=0, second=0)

    try:    
        total_time = 0
        for destination in destinations:
            directions = gmaps_client.directions(origin, destination, mode="transit", departure_time=departure_time)
            travel_time = directions[0]['legs'][0]['duration']['text']
            total_time += get_time_in_minutes(travel_time)
        total_time = int(total_time / len(destinations))

        return total_time
    except:
        return None

def get_price(url, gmaps_client):
    print(url)
    res = requests.get(url, headers={
        'User-Agent': 'Chrome/96.0.4664.110',
    })
    
    soup = BeautifulSoup(res.content, 'lxml')
    if "olx" in url:
        try:
            container = soup.select('[data-testid="ad-price-container"]')[0]
            price = container.find('h3').text
        except:
            price = "111111"
        try:
            rent = soup.find(lambda tag: tag.name == 'li' and 'Czynsz' in tag.get_text()).text
        except:
            rent = "0"
        try:
            address_container = soup.select('div[data-cy="ad_description"]')[0]
            address = find_address(address_container.text)
        except:
            address = None
    else: #otodom
        try:
            price = soup.select('strong[aria-label="Cena"]')[0].text
        except:
            price = "99999"
        try:
            rent = soup.select('div[aria-label="Czynsz"]')[0].text
        except:
            rent = "0"
        try:
            address_container = soup.select('a[aria-label="Adres"]')[0]
            address = find_address(address_container.text)
        except:
            address = None
    
    return {
        'price': price_to_int(price),
        'rent': price_to_int(rent),
        'url': url,
        'address': address,
        'time': time_to_centre(address, gmaps_client)
    }


def get_all_prices(links, gmaps_client):
    return [get_price(link, gmaps_client) for link in links]


def get_all_prices_strs(links, gmaps_client):
    prices = get_all_prices(links, gmaps_client)
    price_strs = [f'{p["price"]},{p["rent"]},{p["time"]},{p["url"]}' for p in prices]

    return price_strs
