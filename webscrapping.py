#!/usr/bin/env python
# -*- coding: utf-8 -*-

# CRON CONFIGURATION
# 0 4 * * * /usr/bin/python3 /home/user/johndoe/webscrapping.py

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

# Instala/Importa todos los módulos necesarios
[import_or_install(module) for module in MODULES]

# Para las peticiones
import requests
# Para el parseo del HTML
from bs4 import BeautifulSoup
# Para la representación visual de los datos, usado anteriormente para guardar en CSV
# import pandas as pd
# Para las operaciones de sistema, limpiar consola y crear directorios
import os
# Para validar la URL de las peticiones
import validators
# Para guardar ficheros en binario (imágenes)
import shutil
# Para el timestamp del log
import datetime
# Para los hilos y la optimización del webscraper
import threading
# Usado para medir el tiempo
import time
# Para poder generar una queue de hilos concurrentes (simultáneos)
from concurrent.futures import ThreadPoolExecutor
# Para instalar las dependencias de módulos que son necesarias para la ejecución
import pip
# Para optimizar la detección de encoding
import cchardet
# Para cerrar el contexto de una variable de forma segura
from contextlib import closing

# Typing imports
from requests.models import Response

# source
# https://www.freecodecamp.org/news/scraping-ecommerce-website-with-python/

# |----------------------------------------|
# |               CONSTANTES               |
# |----------------------------------------|

basedir = os.path.dirname(__file__)
# basedir = os.path.join('kunden', basedir, 'webscrapping', 'big-data-aplicados')
true = True
false = False

# Es el toggle para saber si se imprimirán o no prints de seguimiento
DEBUG = False
BASE_DOMAIN = 'www.thewhiskyexchange.com'
BASE_URL = 'https://' + BASE_DOMAIN

IMG_DOMAIN = 'img.thewhiskyexchange.com'
BASE_IMG_URL = 'https://' + IMG_DOMAIN

# URL = 'https://tienda.consum.es/es/p/mistela-moscatel-do-valencia/72280'
# URL = 'https://www.thewhiskyexchange.com/p/43320/renegade-gin'
URL = BASE_URL

# SITEMAP = 'https://tienda.consum.es/sitemap.xml'
HEADERS = { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36' }
TIMEOUT = (3.05, 27)
# PARSER = 'html.parser'
# lxml funciona mucho más rápido que el html.parser
PARSER = 'lxml'

# FILENAME = 'data.csv'
# FILENAME = datetime.datetime.now().strftime("%d-%m-%Y_%H-%M-%S") + '.csv'
FILENAME = '' + datetime.datetime.now().strftime("%d-%m-%Y") + '.csv'
IMG_DIR = 'img'

RETRY = True
DOWNLOAD_ATTEMPTS = 3
# Si no se quiere volver a intentar los intentos pasan a ser 1 (la primera vez)
if not RETRY: DOWNLOAD_ATTEMPTS = 1


# Acelera las solicitud de peticiones HTTP al reusar la sesión que se abre en el primer request
# cada request de normal crea una nueva sesión, de esta manera se utilizará siempre la misma
# https://thehftguy.com/2020/07/28/making-beautifulsoup-parsing-10-times-faster/
REQUEST_SESSION = requests.Session()

# Controla el uso de Threads en el sistema
USE_THREADS = False

# |-----------------------------------------|
# |                 MÉTODOS                 |
# |-----------------------------------------|

def is_null(val): return val is None

def cls():
    os.system('cls' if os.name == 'nt' else 'clear')

# Se limpia la consola cada vez que se ejecuta
cls()

def create_folder_if_not_exists(folder):
    if not os.path.exists(folder): os.makedirs(folder)

create_folder_if_not_exists(IMG_DIR)

def is_success(res: Response):
    return not (res.status_code < 200 | res.status_code >= 300)

def get_page(url: str, display: bool = True):
    if DEBUG: print('Se va a recuperar el contenido de la página', url)

    if not (validators.url(url)):
        if DEBUG: print('La URL no es válida, no se ha podido descargar el contenido')
        return ''

    with closing( REQUEST_SESSION.get(url, timeout=TIMEOUT, headers=HEADERS, stream=True) ) as res:
        # La respuesta ha fallado
        if not is_success(res):
            if DEBUG: print('No se ha podido recuperar el contenido de la página correctamente')
            return ''

        # Si se devuelve el .text y no el .content se evita la devolución de valores binarios
        html = res.text
        if display: print(res.status_code, html)

        return html

EMPTY_VALUE = " "
def add_detail(dict: dict, name: str, value: any = None):
    if is_null(value): value = EMPTY_VALUE
    dict[name] = value

CATEGORY_JOINER = ">"
def parse_category(element):
    categories = element[0].select('li')
    # El primer valor siempre será "Home", y el último el nombre del producto
    # categories = categories[1:-2] # esto da error, -1 ya ignora el último lugar
    categories = categories[1:-1]

    return CATEGORY_JOINER.join([l.text for l in categories])

def parse_img(element):
    if (len(element) <= 0): return None

    img = element[0]
    src = img.get('src')
    if is_null(src): src = img.get('data-original')

    return src

def parse_price_prev(element, default = '£0.00'):
    if not element: return default

    price_prev = element[0].text

    price_prev = price_prev.replace('(Was ')
    price_prev = price_prev.replace(')')

    return price_prev

FIELDS = {
    'name'       : { 'query': '.product-main__name'                 , 'parser': None            , 'default': 'Sin nombre'           },
    'price'      : { 'query': '.product-action__price'              , 'parser': None            , 'default': '£0.00'                },
    'price_prev' : { 'query': '.product-offer__price'               , 'parser': parse_price_prev, 'default': '£0.00'                },
    'description': { 'query': '.product-main__description'          , 'parser': None            , 'default': 'Sin descripción'      },
    'img'        : { 'query': '.product-main__image-container img'  , 'parser': parse_img       , 'default': 'Imagen no encontrada' },
    'category'   : { 'query': '.breadcrumb__list'                   , 'parser': parse_category  , 'default': 'Sin categoría'        },
    'unit'       : { 'query': '.product-main__data'                 , 'parser': None            , 'default': 'Sin unidad'           },
    'rating'     : { 'query': '.review-overview__rating.star-rating', 'parser': None            , 'default': 'Sin valoración'       },
}

def get_value(field, element):
    # Si el campo tiene una función de parseo, se utiliza
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

def get_detail(soup, dictionary: dict, name: str):
    # print('Se intenta recueprar el campo', name)

    field = FIELDS[name] if name in FIELDS else " "
    query = field['query']
    element = soup.select(query)

    value = get_value(field, element)

    add_detail(dictionary, name, value)

    return value

def get_content(url: str = None, content: str = None):
    if not is_null(url): content = get_page(url, False)
    if is_null(content): return None

    return content

URL_ID_START = '/p/'
URL_ID_END = '/'
def get_url_id(url: str):
    start = url.find(URL_ID_START) + len(URL_ID_START)
    end = url.rfind(URL_ID_END)
    id = url[start:end]

    return id

def get_product_details(url: str = None, content: str = None):
    content = get_content(url, content)
    if is_null(content): return None
    if DEBUG: print('Se van a recuperar los detalles de una página de producto', url)

    soup = BeautifulSoup(content, features=PARSER)

    row = {}
    row['id'] = get_url_id(url)
    for name in FIELDS:
        get_detail(soup, row, name)
    row['url'] = url

    if DEBUG: print('Ya se han recuperado los detalles')

    return row

def is_category_url(url):
    return str(url).startswith('/c')

CATEGORY_LINK_QUERY = 'a.subnav__link, a.navbar__link'
def get_categories(url: str = None, content: str = None):
    content = get_content(url, content)
    if is_null(content): return None
    if DEBUG: print('Se recuperan las categorías')

    soup = BeautifulSoup(content, features=PARSER)
    category_tags = soup.select(CATEGORY_LINK_QUERY)

    categories = [ c_tag.get('href') for c_tag in category_tags if is_category_url(c_tag.get('href')) ]

    categories = [ BASE_URL + c for c in categories ]
    if DEBUG: print('Se han recuperado un total de:', len(categories), 'categoría(s)')
    return categories

def is_product_url(url: str):
    return str(url).startswith('/p')

CATEGORY_PRODUCT_LINK_QUERY = 'a.product-card'
def get_category_products(url: str = None, content: str = None):
    content = get_content(url, content)
    if is_null(content): return None
    if DEBUG: print('Se recuperan los productos de la categoría')

    soup = BeautifulSoup(content, features=PARSER)
    products = set()

    category_product_tags = soup.select(CATEGORY_PRODUCT_LINK_QUERY)

    products = set([ c_tag.get('href') for c_tag in category_product_tags if is_product_url(c_tag.get('href')) ])
    # TODO: añadir un condicionar para el BASE_URL pero tener en cuenta que se espera devolver un list
    products = [ BASE_URL + p for p in list(products) ]

    if DEBUG: print('Se han recuperado un total de:', len(products), 'producto(s)')
    # tal vez se podría forzar aquí un retorno de list para que funcione correctamente
    return products

SEPARATOR = ";"
FILE_NEWLINE = "\n"
DATA_FOLDER = 'data'
EXTRA_START_COLUMNS = ['id']
EXTRA_END_COLUMNS = ['url']
def save_csv(data: list):
    if DEBUG: print('Se guarda el contenido en un fichero .csv', FILENAME)
    # keys = ['id'] + list(FIELDS.keys())
    keys = EXTRA_START_COLUMNS + list(FIELDS.keys()) + EXTRA_END_COLUMNS
    # keys = list(FIELDS.keys())

    filename = os.path.join(basedir, DATA_FOLDER, FILENAME)
    create_folder_if_not_exists(DATA_FOLDER)
    with open(filename, 'w+', 2, 'utf-8') as file:
        file.write(SEPARATOR.join(['\"' + k + '\"' for k in keys]))
        file.write(FILE_NEWLINE)

        for row in data:
            values = list([x.replace(FILE_NEWLINE, EMPTY_VALUE) for x in row.values()])
            file.write(SEPARATOR.join(values))
            file.write(FILE_NEWLINE)

def get_image_filename(url: str):
    return str(url).replace(BASE_IMG_URL, '').replace('/', '_')

MAX_IMG_DOWNLOAD_ATTEMPTS = DOWNLOAD_ATTEMPTS
def download_img(url: str):
    for attempt in range(0, MAX_IMG_DOWNLOAD_ATTEMPTS):
        try:
            filename = get_image_filename(url)

            img_content = requests.get(url, stream = True)
            img_content.raw.decode_content = True
            with open(os.path.join(IMG_DIR, filename), "wb") as img_file:
                shutil.copyfileobj(img_content.raw, img_file)

            if DEBUG: print('Se ha descargado la imagen', filename)

            break
        except:
            log_error(f'No se ha podido descargar la imagen {url}, {attempt + 1}º intento')

def get_now():
    return datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")

def get_timestamp_str():
    return '[' + get_now() + ']'

LOG_FILE = 'error.log'
def log_error(data):
    with open(LOG_FILE, 'a+') as log:
        log.write(get_timestamp_str() + ' ' + str(data))
        log.write(FILE_NEWLINE)

def save_images(data: list):
    if DEBUG: print('Se empiezan a descargar todas las imágenes de los productos')

    no_img_download = []

    index = 0
    rows = len(data)
    percentage = 100
    for row in data:
        index += 1
        percentage = (index / rows) * 100

        img_url = row['img']
        if DEBUG: print('Se va a intentar descargar la imagen', img_url)

        # Las imágenes de productos personalizables no se llegan a descargar, serían a mano
        try:
            if not (validators.url(img_url)): raise Exception('Imagen no válida')

            threadify(target=download_img, args=(img_url,))
        except:
            log_error('No se ha podido descargar la imagen del producto: ' + str(row['url']))
            if DEBUG: print('NO se ha podido descargar la imagen', img_url, row['url'])
            no_img_download.append(row)
        finally:
            if DEBUG: print('Porcentaje de descarga de imágenes (', str(index) + '/' + str(rows), ')', percentage, '%')
    
    wait_for_all_threads()

    if DEBUG: print('Ya se han descargado las imágenes de todos los productos')
    not_downloaded = len(no_img_download)
    if not_downloaded & DEBUG: print('No se ha(n) podido descargar la(s) imágen(es) de', not_downloaded, 'producto(s)')

def empty_threads():
    del THREADS[:THREADS_LIMIT]

def wait_for_all_threads(empty = True, warning = True):
    # Espera a que terminen todos los hilos antes de pasar al siguiente
    for thread in THREADS:
        thread.join()

    if warning & DEBUG: print('\nSe espera a que terminen el resto de hilos\n')
    if empty: empty_threads()

THREADS = []
# 10 va genial, 15 empieza a ser demasiado, y 6 demasiado poco
# WORKERS
THREADS_LIMIT = 25

def threadify(target, args: tuple):
    # Guard close para evitar errores si solo se pasa un parámetro
    # if type(args) != tuple: args = tuple(args)

    # Se espera antes de ejecutar de más, de esta manera el limit funciona correctamente
    if (len(THREADS) >= THREADS_LIMIT):
        # TODO: buscar una manera en la que se evite hacer un wait_all
        wait_for_all_threads()

    thread = threading.Thread(target=target, args=args)
    thread.start()
    THREADS.append(thread)

MAX_PRODUCT_DOWNLOAD_ATTEMPTS = DOWNLOAD_ATTEMPTS
def product_to_thread(data):
    details, product = data

    # Se hace un bucle para que se intente la descarga n número de veces
    for attempt in range(0, MAX_PRODUCT_DOWNLOAD_ATTEMPTS):
        try:
            product_detail = get_product_details(product)
            details.append(product_detail)
            break
        except:
            log_error(f'No se ha podido descargar el producto {product}, {attempt + 1}º intento')

MAX_CATEGORY_PRODUCTS_DOWNLOAD_ATTEMPTS = DOWNLOAD_ATTEMPTS
# def category_products_to_thread(products: set, category_url: str, product_limit: int, limit: int):
def category_products_to_thread(data):
    products, category_url, product_limit, limit = data

    product_urls = []
    for attempt in range(0, MAX_CATEGORY_PRODUCTS_DOWNLOAD_ATTEMPTS):
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

def is_csv_corrupt(filename: str):
    # Si no es CSV, se fuerza
    if not str(filename).endswith('.csv'): filename = f'{filename}.csv'
    file_path = os.path.join(DATA_FOLDER, filename)

    with open(file_path, 'r+') as fr:
        content = fr.read()
        content_lines = content.split(FILE_NEWLINE)

    # TODO: optimizar para que no utilice set si no que compare línea a línea con las anteriores?
    # Se busca detectar todas aquellas líneas que estén repetidas en el CSV
    lines = set(content_lines)

    return len(content_lines) != len(lines)

ALL = -1
def scrape(
    category_limit: int = ALL,
    product_limit: int = ALL,
    limit: int = ALL,
    as_csv: bool = True,
    save_imgs: bool = True,
    display: bool = False,
    category_urls: list = None,
    products: list = None,
    detect_corruption: bool = True
):
    start_time = time.time()

    was_products_empty = is_null(products)

    # Si no se ha pasado por parámetro, se recuperan de la página
    if is_null(category_urls):
        category_urls = get_categories(URL) if was_products_empty else []
    # Se asegura que no habrá ninguna categoría repetida
    category_urls = list( set(category_urls) )

    details = []

    # Se usa un SET para los enlaces de productos porque pueden haber repetidos
    # (es una afirmación, no una duda)
    products = set() if was_products_empty else set(products)

    # No es un autoincremental porque se trabaja con un SET
    num_products = 0

    # El limit no funciona con un solo elemento
    if len(category_urls) > 1 & limit != ALL:
        category_urls = category_urls[:category_limit]

    # if DEBUG: print('\nRecuperando los productos de las categorías\n', len(category_urls), 'categoría(s)\n')
    print('\nRecuperando los productos de las categorías\n', len(category_urls), 'categoría(s)\n')

    # Primero se recuperan los enlaces de todos los productos, teniendo en cuenta cada uno de los límites
    # implementear el threadify aquí, y si luego una categoría tardase de más en paginar, pues ya es otro problema
    if was_products_empty:
        categories_args = [(products, category_url, product_limit, limit) for category_url in category_urls]

        if not USE_THREADS:
            # Versión sin hilos
            [category_products_to_thread(data) for data in categories_args]
        else:
            # Versión con hilos
            with ThreadPoolExecutor(THREADS_LIMIT) as executor:
                executor.map(category_products_to_thread, categories_args)
                executor.shutdown()

    # print('category_urls', products, categories_args)
    # if DEBUG: print('\nRecuperando los detalles de los productos\n', len(products), 'producto(s)\n')
    print('\nRecuperando los detalles de los productos\n', len(products), 'producto(s)\n')

    # Con los threads se puede llegar a sobrepasar el limit, así que se fuerza
    if len(products) > product_limit & product_limit != ALL: products = list(products)[:product_limit]

    products_args = [(details, p) for p in products]

    if not USE_THREADS:
        # Versión sin hilos
        [product_to_thread(data) for data in products_args]
    else:
        # Y luego ya se recorren cada uno de los productos recuperando su información
        with ThreadPoolExecutor(THREADS_LIMIT) as executor:
            # print('se está intentando', products_args)
            executor.map(product_to_thread, products_args)
            executor.shutdown()

    if display:
        print(details)
        # df = pd.DataFrame(details)
        # print(df)
        # df.to_csv(FILENAME, SEPARATOR, EMPTY_VALUE)

    if as_csv:
        # if DEBUG: print('\nSe guarda como CSV\n', len(details), 'línea(s)\n')
        print('\nSe guarda como CSV\n', len(details), 'línea(s)\n')
        save_csv(details)
    if save_imgs:
        if DEBUG: print('\nSe descargan las imágenes\n')
        save_images(details)
    if detect_corruption:
        is_corrupt = is_csv_corrupt(FILENAME)
        is_corrupt_str = '' if is_corrupt else 'no'
        print('El archivo', is_corrupt_str, 'es corrupto')

    end_time = time.time()
    total_time = end_time - start_time

    timedelta = format_time(total_time)
    print('Tiempo total de ejecución:', timedelta)

def format_time(seconds):
    return datetime.timedelta(seconds=seconds)

category_urls = [
    
]

categories_filepath = os.path.join(basedir, 'categories.txt')
with open(categories_filepath, 'r+') as f:
    category_urls = [ line.strip() for line in f.readlines() ]

# Introducir los productos que no se han podido descargar en la iteración
# TODO: implementar un append al CSV cuando se pasen productos como argumento
products = [
    'https://www.thewhiskyexchange.com/p/19887/high-west-campfire',
    'https://www.thewhiskyexchange.com/p/62334/kurayoshi-pure-malt',
]

# DEBUG = True
DEBUG = False
USE_THREADS = False
# 0:01:51.676625 con el debug a true   544 lineas
# 0:01:41.111771 con el debug a false  522 lineas
scrape(limit = ALL, save_imgs=False, display=False, category_urls=category_urls, detect_corruption=False)
# scrape(limit = 3, save_imgs=False, display=False, category_urls=category_urls[:2], detect_corruption=False)