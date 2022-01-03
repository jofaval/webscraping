#!/usr/bin/env python
# -*- coding: utf-8 -*-

# CRON CONFIGURATION
# 0 4 * * * /usr/bin/python3 /home/user/johndoe/webscrapping.py

# Last modification: 20/12/2021 2:17

MODULES = [
    'requests',
    'bs4',
    'validators',
    'cchardet',
    'lxml',
]
def import_or_install(package):
    try:
        __import__(package)
    except ImportError:
        pip.main(['install', package]) 
        __import__(package)

# Install/Import all necessary modules
[import_or_install(module) for module in MODULES]

# For requests
from typing import List
from bs4.element import ResultSet, Tag
import requests
# For parsing the HTML
from bs4 import BeautifulSoup
# For visual representation of data, previously used to save to CSV
# import pandas as pd
# For system operations, clean console and create directories
import os
# To validate the URL of the requests
import validators
# To save files in binary (images)
import shutil
# For the log timestamp
import datetime
# For threads and webscraper optimization
import threading
# Used to measure time
import time
# To be able to generate a queue of concurrent threads (simultaneous)
from concurrent.futures import ThreadPoolExecutor
# To install the module dependencies that are required for execution
import pip
# To optimize encoding detection
import cchardet
# To safely close the context of a variable
from contextlib import closing

# Typing imports
from requests.models import Response

# source
# https://www.freecodecamp.org/news/scraping-ecommerce-website-with-python/

# |---------------------------------------|
# |               CONSTANTS               |
# |---------------------------------------|

# basedir = os.path.join('kunden', basedir, 'webscrapping', 'big-data-aplicados')
true = True
false = False

# Speeds up HTTP request requests by reusing the session that is opened in the first request
# each normal request creates a new session, in this way the same session will always be used
# https://thehftguy.com/2020/07/28/making-beautifulsoup-parsing-10-times-faster/
REQUEST_SESSION = requests.Session()

BASE_DOMAIN = 'www.thewhiskyexchange.com'
BASE_URL = 'https://' + BASE_DOMAIN

IMG_DOMAIN = 'img.thewhiskyexchange.com'
BASE_IMG_URL = 'https://' + IMG_DOMAIN

DOWNLOAD_ATTEMPTS = 3
ALL = -1

# TODO: implement conf json script py, and callbacks
CONF = {
    'basedir': os.path.dirname(__file__),
    'DEBUG': False,
    'BASE_DOMAIN': 'www.thewhiskyexchange.com',
    'BASE_URL': 'https://' + BASE_DOMAIN,
    'IMG_DOMAIN': 'img.thewhiskyexchange.com',
    'BASE_IMG_URL': 'https://' + IMG_DOMAIN,
    'URL': BASE_URL,
    'HEADERS': { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36' },
    'TIMEOUT': (3.05, 27),
    'PARSER': 'lxml',
    'FILENAME': '' + datetime.datetime.now().strftime("%d-%m-%Y") + '.csv',
    'IMG_DIR': 'img',
    'RETRY': True,
    'DOWNLOAD_ATTEMPTS': 3,

    'USE_THREADS': True,
    'EMPTY_VALUE': " ",
    'CATEGORY_JOINER': ">",
    'URL_ID_START': '/p/',
    'URL_ID_END': '/',
    'CATEGORY_LINK_QUERY': 'a.subnav__link, a.navbar__link',
    'CATEGORY_PRODUCT_LINK_QUERY': 'a.product-card',
    'SEPARATOR': ";",
    'FILE_NEWLINE': "\n",
    'DATA_FOLDER': 'data',
    'EXTRA_START_COLUMNS': ['id'],
    'EXTRA_END_COLUMNS': ['url'],
    'MAX_IMG_DOWNLOAD_ATTEMPTS': DOWNLOAD_ATTEMPTS,
    'LOG_FILE': 'error.log',
    'THREADS': [],
    # 10 is going great, 15 is getting too much, and 6 is getting too little,
    # WORKERS,
    'THREADS_LIMIT': 25,
    'MAX_PRODUCT_DOWNLOAD_ATTEMPTS': DOWNLOAD_ATTEMPTS,
    'MAX_CATEGORY_PRODUCTS_DOWNLOAD_ATTEMPTS': DOWNLOAD_ATTEMPTS,
    'ALL': -1,

    # The scrape() args, default values
    'category_limit': ALL,
    'product_limit': ALL,
    'limit': ALL,
    'as_csv': True,
    'save_imgs': True,
    'display': False,
    'category_urls': None,
    'products': None,
    'detect_corruption': True,
    'FIELDS': {},
    'is_category_url': lambda x: True,
    'is_product_url': lambda x: True,
}

# |-----------------------------------------|
# |                 METHODS                 |
# |-----------------------------------------|

def is_null(val: any) -> bool:
    """
    Determines wether a variable is null or not

    val : any
        The variable to analyze

    returns bool
    """
    return val is None

def cls() -> None:
    """
    Cleans the console

    returns None
    """
    os.system('cls' if os.name == 'nt' else 'clear')

# The console is cleaned every time it is run
cls()

def create_folder_if_not_exists(folder: str) -> None:
    """
    Creates a folder if doesn't previously exist

    returns None
    """
    if not os.path.exists(folder): os.makedirs(folder)

def is_success(res: Response) -> bool:
    """
    Determines wether a request was successful or not

    returns bool
    """
    return not (res.status_code < 200 | res.status_code >= 300)

def get_page(url: str, display: bool = True) -> str:
    """
    Returns the page content if it could be retrieved

    url : str
        The URL from which to retrieve the page's content
    [deprecated] display : bool
        Will it display the content? It will by default

    returns str
    """
    if CONF['DEBUG']: print('Se va a recuperar el contenido de la página', url)

    if not (validators.url(url)):
        if CONF['DEBUG']: print('La URL no es válida, no se ha podido descargar el contenido')
        return ''

    with closing( REQUEST_SESSION.get(url, timeout=CONF['TIMEOUT'], headers=CONF['HEADERS'], stream=True) ) as res:
        # The answer has failed
        if not is_success(res):
            if CONF['DEBUG']: print('No se ha podido recuperar el contenido de la página correctamente')
            return ''

        # If the .text is returned and not the .content, the return of binary values ​​is avoided
        html = res.text
        if display: print(res.status_code, html)

        return html

def add_detail(dict: dict, name: str, value: any = None) -> None:
    """
    Adds a detail to the detail's dict

    dict : dict
        The dictionary to append the detail to
    name : str
        The name of the detail
    value : any
        The value of the detail, could be None

    returns None
    """
    if is_null(value): value = CONF['EMPTY_VALUE']
    dict[name] = value

def get_value(field: dict, element: ResultSet) -> any:
    """
    Gets the value from an element with the field details

    field : dict
        The field details
    element : ResultSet[Tag]
        The HTML element retrieved

    returns any
    """
    # If the field has a parse function, it is used
    if (('parser' in field) & (not is_null(field['parser']))):
        value = field['parser'](element)
    else:
        value = None
        if len(element) > 0:
            value = element[0].text
            if (str(value).replace('\n', '') == ''): value = element[0].get('src')

    if is_null(value) & (('default' in field) & (not is_null(field['default']))):
        value = field['default']

    if not is_null(value): value = str(value).strip()
    return value

def get_detail(soup: BeautifulSoup, dictionary: dict, name: str) -> str:
    """
    Gets a detail after it's addition to the given dictionary

    soup : BeautifulSoup
        The HTML parser
    dictionary : dict
        The dictionary of details
    name : str
        The name of the detail to retrieve

    returns str
    """
    # print('Attempt to retrieve field', name)

    field = CONF['FIELDS'][name] if name in CONF['FIELDS'] else " "
    query = field['query']
    element = soup.select(query)

    value = get_value(field, element)

    add_detail(dictionary, name, value)

    return value

def get_content(url: str = None, content: str = None) -> str:
    """
    Decorator to retrieve the content from a page

    url : str
        The url from which to retrieve the content
    content : str
        The possible content to analyze, if it exists, use it

    returns str
    """
    if not is_null(url): content = get_page(url, False)
    if is_null(content): return None

    return content

def get_url_id(url: str) -> str:
    """
    Gets the ID from the URL

    url : str
        The url from which to get the ID

    returns str
    """
    start = url.find(CONF['URL_ID_START']) + len(CONF['URL_ID_START'])
    end = url.rfind(CONF['URL_ID_END'])
    id = url[start:end]

    return id
CONF['get_url_id'] = get_url_id

def get_product_details(url: str = None, content: str = None) -> dict:
    """
    Gets the details from a product

    url : str
        The URL from which to get the page content
    content : str
        The possible content to use

    returns dict
    """
    content = get_content(url, content)
    if is_null(content): return None
    if CONF['DEBUG']: print('Se van a recuperar los detalles de una página de producto', url)

    soup = BeautifulSoup(content, features=CONF['PARSER'])

    row = {}
    row['id'] = CONF['get_url_id'](url)
    for name in CONF['FIELDS']:
        get_detail(soup, row, name)
    row['url'] = url

    if CONF['DEBUG']: print('Ya se han recuperado los detalles')

    return row

def get_categories(url: str = None, content: str = None) -> List[str]:
    """
    Gets the categories from a page

    url : str
        The URL from which to get the page content
    content : str
        The possible content to use

    returns dict
    """
    content = get_content(url, content)
    if is_null(content): return None
    if CONF['DEBUG']: print('Se recuperan las categorías')

    soup = BeautifulSoup(content, features=CONF['PARSER'])
    category_tags = soup.select(CONF['CATEGORY_LINK_QUERY'])

    categories = [ c_tag.get('href') for c_tag in category_tags if CONF['is_category_url'](c_tag.get('href')) ]

    categories = [ BASE_URL + c for c in categories ]
    if CONF['DEBUG']: print('Se han recuperado un total de:', len(categories), 'categoría(s)')
    return categories

def get_category_products(url: str = None, content: str = None) -> List[str]:
    """
    Gets the products from a category

    url : str
        The URL from which to get the page content
    content : str
        The possible content to use

    returns dict
    """
    content = get_content(url, content)
    if is_null(content): return None
    if CONF['DEBUG']: print('Se recuperan los productos de la categoría')

    soup = BeautifulSoup(content, features=CONF['PARSER'])
    products = set()

    category_product_tags = soup.select(CONF['CATEGORY_PRODUCT_LINK_QUERY'])

    products = set([ c_tag.get('href') for c_tag in category_product_tags if CONF['is_product_url'](c_tag.get('href')) ])
    # TODO: add a condition for the BASE_URL but bear in mind that it is expected to return a list
    products = [ BASE_URL + p for p in list(products) ]

    if CONF['DEBUG']: print('Se han recuperado un total de:', len(products), 'producto(s)')
    # maybe you could force a return of list here to work properly
    return products

def save_csv(data: dict) -> None:
    """
    Saves the data to CSV

    data : dict
        The data to save and parse to CSV

    returns None
    """
    if CONF['DEBUG']: print('Se guarda el contenido en un fichero .csv', CONF['FILENAME'])
    # keys = ['id'] + list(FIELDS.keys())
    keys = CONF['EXTRA_START_COLUMNS'] + list(CONF['FIELDS'].keys()) + CONF['EXTRA_END_COLUMNS']
    # keys = list(FIELDS.keys())

    filename = os.path.join(CONF['basedir'], CONF['DATA_FOLDER'], CONF['FILENAME'])
    create_folder_if_not_exists(os.path.join(CONF['basedir'], CONF['DATA_FOLDER']))
    with open(filename, 'w+', 2, 'utf-8') as file:
        file.write(CONF['SEPARATOR'].join(['\"' + k + '\"' for k in keys]))
        file.write(CONF['FILE_NEWLINE'])

        for row in data:
            values = list([x.replace(CONF['FILE_NEWLINE'], CONF['EMPTY_VALUE']) for x in row.values()])
            file.write(CONF['SEPARATOR'].join(values))
            file.write(CONF['FILE_NEWLINE'])

def get_image_filename(url: str) -> str:
    """
    Get the image's filename

    url : str
        The raw URL to evaluate

    returns str
    """
    return str(url).replace(BASE_IMG_URL, '').replace('/', '_')

def download_img(url: str) -> None:
    """
    Downloads an image

    url : str
        The image's URL

    returns None
    """
    for attempt in range(0, CONF['MAX_IMG_DOWNLOAD_ATTEMPTS']):
        try:
            filename = get_image_filename(url)

            img_content = requests.get(url, stream = True)
            img_content.raw.decode_content = True
            with open(os.path.join(CONF['IMG_DIR'], filename), "wb") as img_file:
                shutil.copyfileobj(img_content.raw, img_file)

            if CONF['DEBUG']: print('Se ha descargado la imagen', filename)

            break
        except:
            log_error(f'No se ha podido descargar la imagen {url}, {attempt + 1}º intento')

def get_now() -> str:
    """
    Gets the current timestamp in string format

    returns str
    """
    return datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")

def get_timestamp_str() -> str:
    """
    Format the current timestamp

    returns str
    """
    return '[' + get_now() + ']'

def log_error(data) -> None:
    """
    Logs an error to the log file

    returns None
    """
    with open(CONF['LOG_FILE'], 'a+') as log:
        log.write(get_timestamp_str() + ' ' + str(data))
        log.write(CONF['FILE_NEWLINE'])

def save_images(data: List[str]) -> None:
    """
    Save all the images

    data : List[str]
        The array with all the images to download

    returns None
    """
    if CONF['DEBUG']: print('Se empiezan a descargar todas las imágenes de los productos')

    no_img_download = []

    index = 0
    rows = len(data)
    percentage = 100
    for row in data:
        index += 1
        percentage = (index / rows) * 100

        img_url = row['img']
        if CONF['DEBUG']: print('Se va a intentar descargar la imagen', img_url)

        # The images of customizable products are not downloaded, they would be by hand
        try:
            if not (validators.url(img_url)): raise Exception('Imagen no válida')

            threadify(target=download_img, args=(img_url,))
        except:
            log_error('No se ha podido descargar la imagen del producto: ' + str(row['url']))
            if CONF['DEBUG']: print('NO se ha podido descargar la imagen', img_url, row['url'])
            no_img_download.append(row)
        finally:
            if CONF['DEBUG']: print('Porcentaje de descarga de imágenes (', str(index) + '/' + str(rows), ')', percentage, '%')
    
    wait_for_all_threads()

    if CONF['DEBUG']: print('Ya se han descargado las imágenes de todos los productos')
    not_downloaded = len(no_img_download)
    if not_downloaded & CONF['DEBUG']: print('No se ha(n) podido descargar la(s) imágen(es) de', not_downloaded, 'producto(s)')

def empty_threads() -> None:
    """
    Empty all the current THREADS queue

    returns None
    """
    del CONF['THREADS'][:CONF['THREADS_LIMIT']]

def wait_for_all_threads(empty: bool = True, warning: bool = True) -> None:
    """
    Waits for all the current THREAHDS in the queue to finish processing

    empty : bool
        Should it empty the THREAHDS queue after it finishes? It will by default
    warning : bool
        Should it warn that it's waiting for all the THREADS to finish? It will by default

    returns None
    """
    # Wait for all threads to finish before moving on to the next
    for thread in CONF['THREADS']:
        thread.join()

    if warning & CONF['DEBUG']: print('\nSe espera a que terminen el resto de hilos\n')
    if empty: empty_threads()

def threadify(target: callable, args: tuple) -> None:
    """
    Threadifies a function so it runs asynchronously

    target : callable
        The function/method to threadify
    args : tuple
        The callable arguments

    returns None
    """
    # Guard close para evitar errores si solo se pasa un parámetro
    # if type(args) != tuple: args = tuple(args)

    # It is expected before executing more, in this way the limit works correctly
    if (len(CONF['THREADS']) >= CONF['THREADS_LIMIT']):
        # TODO: find a way to avoid doing a wait_all
        wait_for_all_threads()

    thread = threading.Thread(target=target, args=args)
    thread.start()
    CONF['THREADS'].append(thread)

def product_to_thread(data: tuple) -> None:
    """
    Product download/retrieval thread decorator

    data : tuple
        The decorator arguments

    returns None
    """
    details, product = data
    """
    details : dict
        The memory location of the product details
    product : str
        The product URL
    """

    # A loop is made so that the download is attempted n number of times
    for attempt in range(0, CONF['MAX_PRODUCT_DOWNLOAD_ATTEMPTS']):
        try:
            product_detail = get_product_details(product)
            details.append(product_detail)
            break
        except:
            log_error(f'No se ha podido descargar el producto {product}, {attempt + 1}º intento')

# def category_products_to_thread(products: set, category_url: str, product_limit: int, limit: int):
def category_products_to_thread(data) -> None:
    """
    Products from category download/retrieval thread decorator

    data : tuple
        The decorator arguments

    returns None
    """
    products, category_url, product_limit, limit = data
    """
    products : Set[str]
        The memory location to all the products URL set
    category_url : str
        The category URL to evaluate
    product_limit : int
        The maximum amount of products URL to use per category
    limit : int
        The total amount of products that can get downloaded
    """

    product_urls = []
    for attempt in range(0, CONF['MAX_CATEGORY_PRODUCTS_DOWNLOAD_ATTEMPTS']):
        try:
            product_urls = get_category_products(category_url)
        except:
            log_error(f'No se han podido conseguir los productos de la categoría {category_url}, {attempt + 1}º intento')

    num_products = len(products)
    if (limit > 0) & (num_products >= limit): return None

    for product_url in product_urls[:product_limit]:
        products.add(product_url)
        num_products = len(products)

        if (limit > 0) & (num_products >= limit): return None
    if (limit > 0) & (num_products >= limit): return None

    return None

def is_csv_corrupt(filename: str) -> bool:
    """
    Is a CSV corrupted? Checks it

    filename : str
        The full filename path

    returns bool
    """
    # If it is not CSV, it is forced
    if not str(filename).endswith('.csv'): filename = f'{filename}.csv'
    file_path = os.path.join(CONF['basedir'], CONF['DATA_FOLDER'], filename)

    with open(file_path, 'r+') as fr:
        content = fr.read()
        content_lines = content.split(CONF['FILE_NEWLINE'])

    # TODO: optimize so that it does not use set if it does not compare line by line with the previous ones?
    # It seeks to detect all those lines that are repeated in the CSV
    lines = set(content_lines)

    return len(content_lines) != len(lines)

def scrape(
    category_limit: int = ALL,
    product_limit: int = ALL,
    limit: int = ALL,
    as_csv: bool = True,
    save_imgs: bool = True,
    display: bool = False,
    category_urls: List[str] = None,
    products: List[str] = None,
    detect_corruption: bool = True
) -> None:
    """
    Scrapes the data from a website with a given configuration

    category_limit : int = ALL,
        The maximum amount of categories to loop through, all by default
    product_limit : int = ALL,
        The maximum amount of products URL to use per category
    limit : int = ALL,
        The global limit, may be deprecated, sure is, it's poorly documented :(
        The maximum amount of products to loop through, all by default
    as_csv : bool = True,
        Will it be saved as CSV? It will by default
    save_imgs : bool = True,
        Will it also download the images? It will by default
    display : bool = False,
        Will it display the data retrieved? It won't by default.
        Can be time consuming if the dataset gets too big
    category_urls : List[str] = None,
        The list of categories URLs to use, if none given, all that are found will be available
    products : List[str] = None,
        The list of products URLs to use, if none given, all that are found will be available
    detect_corruption : bool = True
        Will it attempt to check if the final CSV is corrupted? It will by default

    returns None
    """
    start_time = time.time()

    was_products_empty = is_null(products)

    # If it has not been passed by parameter, they are retrieved from the page
    if is_null(category_urls):
        category_urls = get_categories(CONF['URL']) if was_products_empty else []
    # It is ensured that there will be no repeating categories
    category_urls = list( set(category_urls) )

    details = []

    # A SET is used for product links because there may be duplicates
    # (it's a statement, not a doubt)
    products = set() if was_products_empty else set(products)

    # It is not an autoincremental because it works with a SET
    num_products = 0

    # The limit does not work with a single element
    if len(category_urls) > 1 & category_limit != ALL:
        category_urls = category_urls[:category_limit]

    # if CONF['DEBUG']: print('\nRecuperando los productos de las categorías\n', len(category_urls), 'categoría(s)\n')
    print('\nRecuperando los productos de las categorías\n', len(category_urls), 'categoría(s)\n')

    # First, the links of all the products are retrieved, taking into account each of the limits
    # implement threadify here, and if then a category takes too long to paginate, it is another problem
    if was_products_empty:
        categories_args = [(products, category_url, product_limit, limit) for category_url in category_urls]

        if not CONF['USE_THREADS']:
            # Wireless version
            [category_products_to_thread(data) for data in categories_args]
        else:
            # String version
            with ThreadPoolExecutor(CONF['THREADS_LIMIT']) as executor:
                executor.map(category_products_to_thread, categories_args)
                executor.shutdown()

    # print('category_urls', products, categories_args)
    # if CONF['DEBUG']: print('\nRecuperando los detalles de los productos\n', len(products), 'producto(s)\n')
    print('\nRecuperando los detalles de los productos\n', len(products), 'producto(s)\n')

    # With threads it is possible to exceed the limit, so force
    if len(products) > product_limit & product_limit != ALL: products = list(products)[:product_limit]

    products_args = [(details, p) for p in products]

    if not CONF['USE_THREADS']:
        # Wireless version
        [product_to_thread(data) for data in products_args]
    else:
        # And then they go through each of the products recovering their information
        with ThreadPoolExecutor(CONF['THREADS_LIMIT']) as executor:
            # print('trying', products_args)
            executor.map(product_to_thread, products_args)
            executor.shutdown()

    if display:
        print(details)
        # df = pd.DataFrame(details)
        # print(df)
        # df.to_csv(FILENAME, SEPARATOR, EMPTY_VALUE)

    if as_csv:
        # if CONF['DEBUG']: print ('\nSave as CSV\n', len (details), 'line(s)\n')
        print('\nSe guarda como CSV\n', len(details), 'línea(s)\n')
        save_csv(details)
    if save_imgs:
        if CONF['DEBUG']: print('\nSe descargan las imágenes\n')
        save_images(details)
    if detect_corruption:
        is_corrupt = is_csv_corrupt(CONF['FILENAME'])
        is_corrupt_str = '' if is_corrupt else 'no'
        print('El archivo', is_corrupt_str, 'es corrupto')

    end_time = time.time()
    total_time = end_time - start_time

    timedelta = format_time(total_time)
    print('Tiempo total de ejecución:', timedelta)

def format_time(seconds: int) -> str:
    """
    Formats a raw timestmap in seconds, mainly used for benchmarking

    seconds : int
        The raw timestamp in seconds

    returns str
    """
    return datetime.timedelta(seconds=seconds)

# Execute only if wanted so, not on import
if (__name__ == '__main__'):
    scrape(limit = ALL, save_imgs=False, display=False, category_urls=CONF['category_urls'], detect_corruption=False)

def set_configuration(configuration: dict) -> dict:
    """
    Sets the configuration values to use

    configuration : dict
        The dictionary object of key, values

    returns dict
    """

    # Set ALL the configuration values
    for key, value in configuration.items():
        CONF[key] = value

    # If no retrys are allowed, only one attempt per category/product
    if not CONF['RETRY']: CONF['DOWNLOAD_ATTEMPTS'] = 1

    # Create the folders if they don't exist here
    create_folder_if_not_exists(os.path.join(CONF['basedir'], CONF['IMG_DIR']))
    create_folder_if_not_exists(os.path.join(CONF['basedir'], CONF['DATA_FOLDER']))
    
    return CONF