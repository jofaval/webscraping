import os, datetime

basedir = os.path.dirname(__file__)

category_urls = [
    
]

categories_filepath = os.path.join(basedir, 'categories.txt')
with open(categories_filepath, 'r+') as f:
    category_urls = [ line.strip() for line in f.readlines() ]

# Enter the products that could not be downloaded in the iteration
# TODO: implement an append to the CSV when products are passed as an argument
products = [
    'https://www.thewhiskyexchange.com/p/19887/high-west-campfire',
    'https://www.thewhiskyexchange.com/p/62334/kurayoshi-pure-malt',
]

BASE_DOMAIN = 'www.thewhiskyexchange.com'
BASE_URL = 'https://' + BASE_DOMAIN

IMG_DOMAIN = 'img.thewhiskyexchange.com'
BASE_IMG_URL = 'https://' + IMG_DOMAIN

DOWNLOAD_ATTEMPTS = 3
ALL = -1

CONF = {
    'basedir': basedir,
    'DEBUG': False,
    'BASE_DOMAIN': BASE_DOMAIN,
    'BASE_URL': 'https://' + BASE_DOMAIN,
    'IMG_DOMAIN': IMG_DOMAIN,
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
    'save_imgs': False,
    'display': False,
    'category_urls': category_urls,
    'products': None,
    'detect_corruption': False,
}