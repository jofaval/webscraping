# Para las peticiones
import requests
# Para el parseo del HTML
from bs4 import BeautifulSoup
# Para la representación visual de los datos, usado anteriormente para guardar en CSV
import pandas as pd
# Para las operaciones de sistema, limpiar consola y crear directorios
import os
# Para validar la URL de las peticiones
import validators
# Para guardar ficheros en binario (imágenes)
import shutil
# Para el timestamp del log
from datetime import datetime

# source
# https://www.freecodecamp.org/news/scraping-ecommerce-website-with-python/

def cls():
    os.system('cls' if os.name=='nt' else 'clear')

# Se limpia la consola cada vez que se ejecuta
cls()

# |----------------------------------------|
# |               CONSTANTES               |
# |----------------------------------------|
BASE_DOMAIN = 'www.thewhiskyexchange.com'
BASE_URL = 'https://' + BASE_DOMAIN

IMG_DOMAIN = 'img.thewhiskyexchange.com'
BASE_IMG_URL = 'https://' + IMG_DOMAIN

# URL = 'https://tienda.consum.es/es/p/mistela-moscatel-do-valencia/72280'
# URL = 'https://www.thewhiskyexchange.com/p/43320/renegade-gin'
URL = BASE_URL

# SITEMAP = 'https://tienda.consum.es/sitemap.xml'
# HEADERS = { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36' }
PARSER = 'html.parser'

FILENAME = 'data.csv'
IMG_DIR = 'img'
if not os.path.exists(IMG_DIR): os.makedirs(IMG_DIR)

# |-----------------------------------------|
# |                 MÉTODOS                 |
# |-----------------------------------------|
def get_page(url, display = True):
    print('Se va a recuperar el contenido de la página', url)

    if not (validators.url(url)):
        print('La URL no es válida, no se ha podido descargar el contenido')
        return ''
    res = requests.get(url, timeout=(3.05, 27))

    if (display == True): print(res.status_code, res.text)
    html = res.text

    return html

EMPTY_VALUE = " "
def add_detail(dict, name, value = None):
    if value is None: value = EMPTY_VALUE
    dict[name] = value

CATEGORY_JOINER = ">"
def parse_category(element):
    categories = element[0].select('li')
    # El primer valor siempre será "Home", y el último el nombre del producto
    categories = categories[1:-2]

    return CATEGORY_JOINER.join([l.text for l in categories])

def parse_img(element):
    if (len(element) <= 0): return None

    img = element[0]
    src = img.get('src')
    if (src is None): src = img.get('data-original')

    return src

FIELDS = {
    'name'       : { 'query': '.product-main__name'                 , 'parser': None          , },
    'price'      : { 'query': '.product-action__price'              , 'parser': None          , },
    'description': { 'query': '.product-main__description'          , 'parser': None          , },
    'img'        : { 'query': '.product-main__image-container img'  , 'parser': parse_img     , },
    'category'   : { 'query': '.breadcrumb__list'                   , 'parser': parse_category, },
    'unit'       : { 'query': '.product-main__data'                 , 'parser': None          , },
    'rating'     : { 'query': '.review-overview__rating.star-rating', 'parser': None          , },
}

def get_detail(soup, dictionary, name):
    """
    Devuelve un campo de detalle

    Parameters
    ----------
        soup : BeautifulSoup
            El parser de HTML
        dictionary : dict
            El diccionario que representa a una fila con todos los valores
        name : str
            El nombre del campo a recuperar, y de la columna
    """
    # print('Se intenta recueprar el campo', name)

    field = FIELDS[name] if name in FIELDS else " "
    query = field['query']
    element = soup.select(query)

    # Si el campo tiene una función de parseo, se utiliza
    if (('parser' in field) & (not field['parser'] is None)):
        value = field['parser'](element)
    else:
        value = None
        if len(element) > 0:
            value = element[0].text
            if (str(value).replace('\n', '') == ''): value = element[0].get('src')

    value = str(value).strip()
    add_detail(dictionary, name, value)

    return value

def get_content(url = None, content = None):
    if not (url is None): content = get_page(url, False)
    if (content is None): return None

    return content

def get_page_details(url = None, content = None):
    content = get_content(url, content)
    if (content is None): return None
    print('Se recuperan los detalles de una página de producto')

    soup = BeautifulSoup(content, features=PARSER)

    row = {}
    for name in FIELDS:
        get_detail(soup, row, name)
    row['url'] = url

    print('Ya se han recuperado los detalles')

    return row

def is_category_url(url):
    return str(url).startswith('/c')

CATEGORY_LINK_QUERY = 'a.subnav__link, a.navbar__link'
def get_categories(url = None, content = None):
    content = get_content(url, content)
    if (content is None): return None
    print('Se recuperan las categorías')

    soup = BeautifulSoup(content, features=PARSER)
    category_tags = soup.select(CATEGORY_LINK_QUERY)

    categories = []
    for category_tag in category_tags:
        href = category_tag.get('href')
        if (is_category_url(href)): categories.append(href)

    categories = [ BASE_URL + c for c in categories ]
    print('Se han recuperado un total de:', len(categories), 'categoría(s)')
    return categories

def is_product_url(url):
    return str(url).startswith('/p')

CATEGORY_PRODUCT_LINK_QUERY = 'a.product-card'
def get_category_products(url = None, content = None):
    content = get_content(url, content)
    if (content is None): return None
    print('Se recuperan los productos de la categoría')

    soup = BeautifulSoup(content, features=PARSER)
    products = set()

    # NO FUNCIONA, FALTA QUE CARGUE LA PÁGINA POR COMPLETO, FUNCIONA CON JS
    pages = soup.select('#paging')
    num_pages = len(pages)
    # print(num_pages)
    # hacer un for para recuperar todas las páginas de una categoría dependiendo del select

    for page in range(1, num_pages + 1):
        temp_url = str(url) + '?pg=' + str(page) + '&psize=24&sort=rdesc'
        get_page(temp_url, False)
        
        soup = BeautifulSoup(content, features=PARSER)
        category_product_tags = soup.select(CATEGORY_PRODUCT_LINK_QUERY)

        for category_product_tag in category_product_tags:
            href = category_product_tag.get('href')
            if (is_product_url(href)): products.add(href)

    products = [ BASE_URL + p for p in list(products) ]
    print('Se han recuperado un total de:', len(products), 'producto(s)')
    return products

SEPARATOR = ";"
FILE_NEWLINE = "\n"
def save_csv(data):
    print('Se guarda el contenido en un fichero .csv', FILENAME)
    # keys = ['id'] + list(FIELDS.keys())
    keys = list(FIELDS.keys()) + ['url']
    # keys = list(FIELDS.keys())

    with open(FILENAME, 'w+', 2, 'utf-8') as file:
        file.write(SEPARATOR.join(['\"' + k + '\"' for k in keys]))
        file.write(FILE_NEWLINE)

        for row in data:
            values = list([x.replace(FILE_NEWLINE, EMPTY_VALUE) for x in row.values()])
            file.write(SEPARATOR.join(values))
            file.write(FILE_NEWLINE)

def get_image_filename(url):
    return str(url).replace(BASE_IMG_URL, '').replace('/', '_')

def download_img(url):
    filename = get_image_filename(url)

    img_content = requests.get(url, stream = True)
    img_content.raw.decode_content = True
    with open(os.path.join(IMG_DIR, filename), "wb") as img_file:
        shutil.copyfileobj(img_content.raw, img_file)

    print('Se ha descargado la imagen', filename)

def get_now():
    return datetime.now().strftime("%d/%m/%Y %H:%M:%S")

LOG_FILE = 'error.log'
def log_error(data):
    with open(LOG_FILE, 'a+') as log:
        log.write('[' + get_now() + ']' + ' ' + str(data))
        log.write('\n')

def save_images(data):
    print('Se empiezan a descargar todas las imágenes de los productos')

    no_img_download = []

    index = 0
    rows = len(data)
    percentage = 100
    for row in data:
        index += 1
        percentage = (index / rows) * 100

        img_url = row['img']
        print('Se va a intentar descargar la imagen', img_url)

        # Las imágenes de productos personalizables no se llegan a descargar, serían a mano
        try:
            if not (validators.url(img_url)): raise Exception('Imagen no válida')

            download_img(img_url)
        except:
            log_error('No se ha podido descargar la imagen del producto: ' + str(row['url']))
            print('NO se ha podido descargar la imagen', img_url, row['url'])
            no_img_download.append(row)
        finally:
            print('Porcentaje de descarga de imágenes (', str(index) + '/' + str(rows), ')', percentage, '%')
    
    print('Ya se han descargado las imágenes de todos los productos')
    not_downloaded = len(no_img_download)
    if (not_downloaded > 0): print('No se ha(n) podido descargar la(s) imágen(es) de', not_downloaded, 'producto(s)')

ALL = -1
def scrape(category_limit = ALL, product_limit = ALL, limit = 1000):
    category_urls = get_categories(URL)

    details = []

    # Se usa un SET para los enlaces de productos porque pueden haber repetidos
    # (es una afirmación, no una duda)
    products = set()

    # No es un autoincremental porque se trabaja con un SET
    num_products = 0

    # Primero se recuperan los enlaces de todos los productos,
    # teniendo en cuenta cada uno de los límites
    for category_url in category_urls[:category_limit]:
        product_urls = get_category_products(category_url)

        for product_url in product_urls[:product_limit]:
            products.add(product_url)
            num_products = len(products)
            if (num_products >= limit): break

        if (num_products >= limit): break
    
    # Y luego ya se recorren cada uno de los productos recuperando su información
    for product in products:
        try:
            product_detail = get_page_details(product)
            details.append(product_detail)
        except:
            log_error('No se ha podido descargar el producto ', product)
    
    # df = pd.DataFrame(details)
    # print(df)
    # df.to_csv(FILENAME, SEPARATOR, EMPTY_VALUE)
    save_csv(details)
    save_images(details)

scrape(limit = 1000)

# |-------------------------------|
# |            TESTING            |
# |-------------------------------|

# |-------------------------------|
# | RECUPERA TODAS LAS CATEGORÍAS |
# |-------------------------------|

# categories = get_categories(URL)
# print(categories[0])

# |-----------------------------------------------|
# | RECUPERA TODOS LOS PRODUCTOS DE UNA CATEGORÍA |
# |-----------------------------------------------|

# URL = 'https://www.thewhiskyexchange.com/c/828/sample-gift-sets'
# 
# URL = 'https://www.thewhiskyexchange.com/c/304/blended-scotch-whisky'
# product_links = get_category_products(URL)
# print(product_links)

# |--------------------------------------------|
# | RECUPERA TODOS LOS DETALLES DE UN PRODUCTO |
# |--------------------------------------------|

# URL = 'https://www.thewhiskyexchange.com/p/59957/20-whiskies-that-changed-the-world-tasting-set-20x3cl'
# details = get_page_details(URL)
# print(details)

# |--------------------------|
# | PRUEBA DE INTEGRACIÓN v1 |
# |--------------------------|

# URL = 'https://www.thewhiskyexchange.com/c/828/sample-gift-sets'
# 
# product_links = get_category_products(URL)
# 
# details = []
# for product_link in product_links[:2]:
    # product_detail = get_page_details(BASE_URL + product_link)
    # details.append(product_detail)
# 
# df = pd.DataFrame(details)
# print(df)
# df.to_csv(FILENAME, SEPARATOR, EMPTY_VALUE)
# save_csv(details)

# TODO: