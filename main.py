import numpy as np
import googlemaps
import os

from parse_page import *
from multiprocessing import shared_memory, Process, Lock
from dotenv import load_dotenv
from tqdm import tqdm

STRLEN = 256
LINKS_LIMIT = 1500
MINUTE_WORTH = 20

def worker_func(links, full_data, lock):
    load_dotenv()
    gmaps_client = googlemaps.Client(key=os.environ.get('MAPS_KEY'))
    data = get_all_prices_strs(links, gmaps_client)
    data = [row.encode('ascii') for row in data]

    lock.acquire()
    try:
        i = 0
        while full_data[i]:
            i += 1
            
        for row in data:
            if len(row) > STRLEN:
                raise RuntimeError(f"Data length is out of bounds. Limit is {STRLEN}")

            full_data[i] = row
            i += 1
        lock.release()
    except:
        lock.release()
            

def get_price_from_row(row):
    arr = row.split(b',')
    try:
        time_val = int(MINUTE_WORTH * int(arr[2]))
    except:
        time_val = 10000
    return int(arr[0]) + int(arr[1]) + time_val

def main():
    # hardcode to remove
    olx = 'https://www.olx.pl'
    pages = 25
    proc_count = 13
    procs = []
    full_olx_url = f'{olx}/nieruchomosci/mieszkania/wynajem/warszawa/?page='
    
    shm = shared_memory.SharedMemory(create=True, size=LINKS_LIMIT*STRLEN)
    lock = Lock()
    full_data = np.ndarray(LINKS_LIMIT, buffer=shm.buf, dtype=np.dtype(f'S{STRLEN}'))

    links_progress = tqdm(total=pages, unit_scale=True, unit='', desc="Fetching urls progress")
    links = []
    for page in range(pages):
        links += get_links(f'{full_olx_url}{page}')
        links_progress.update(1)
    links_progress.close()

    links = list(set(links))
    for process in range(proc_count):
        p = Process(target=worker_func, args=(links[process::proc_count], full_data, lock))
        procs.append(p)
        p.start()
        print(f'process {process} started')
        
    for p in procs:
        p.join()
    
    i = 0
    while full_data[i]:
        i += 1

    data = sorted(full_data[:i], key=get_price_from_row)
    
    f = open("result.txt", "w")
    for row in data:
        f.write(f'{row.decode("ascii")}\n')
    f.close()

    shm.close()
    shm.unlink()

if __name__ == "__main__":
    main()
    # load_dotenv()
    # print(get_price("https://www.otodom.pl/pl/oferta/odnowione-mieszkanie-praga-pld-dobra-lokalizacja-ID4pzmM.html"))
    # print(get_price("https://www.olx.pl/d/oferta/rezerwacja-mieszkanie-na-wynajem-warszawa-wola-2-pokoje-garaz-fv-wy-CID3-IDTruGJ.html"))
