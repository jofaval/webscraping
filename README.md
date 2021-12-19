# Web Scraping #
Web scraping is data scraping used for extracting data from websites

## Why?
This was part of a degree project, and I expanded on it much further than intially intended.
<br />
It started as such, but continued with the idea of having a complex system made simple for scraping a ton of sites easily. Initially focused on the E-Commerce [www.thewhiskyexchange.com](https://thewhiskyexchange.com)

### Advice
Always check the `domain.com/robots.txt` to check if the website allows and acknowledges the use of scrapers on their page

## Usage
### Requirements
- Python >= v3.6 Installed
- Decent Internet connection
- Modules (if not previously installed, they will be downloaded on execution):
  - `requests`, `bs4`, `validators`, `cchardet`, `lxml`,

### How to use? 
Move to your working directory
`cd /my/working/directory`

And execute the python script (use python3 on a linux OS)\
`python script.py` | `python3 script.py`\
or\
`/usr/bin/python3 /my/working/directory/script.py`

#### Tip
You can always modify de `THREADS_LIMIT` constant to update the number of *workers* (threads) that will be executed simultaneously. It can really speed up the process, but only if you're computer does allow the number you're inputing.

For example, I have 8 virtual cores, but 50 *workers* work perfectly fine, more slows it, less are not enough.

### How to download?
The basis of webscraping you'd have to replicate what a use would do to check prices and products.

The important parts to modify on this scraper, at the moment of writting the README 2021-19-12, are:
- `FIELDS`: all the fields to download
  - `name`: the dict `key`, the name of the field to download, the label.
  - `query`: the CSS query from which to get the element(s). NOT tested with multiple queries at once.
  - `parser`: the function to parse the data recieved, if used, this will override the default data retrieval
  - `default`: doesn't matter if it's using a parser or not, the default value if the given value is `None`
- `CATEGORY_LINK_QUERY`: the CSS query to get all the category links
- `is_category_url`: wether a url is a category url
- `is_product_url`: wether a url is a product url
- `CATEGORY_PRODUCT_LINK_QUERY`: the CSS to get all the product links in a category.
- `BASE_DOMAIN`: the base domain of the website, just the base path of the domain, i.e., `google.com/es` would also work
- `IMG_DOMAIN`: the base domain of the images. Some websites use different domains for their images
- `RETRY`: will it attempt to redownload a failed download? It will by default
- `DOWNLOAD_ATTEMPTS`: how many times do you want to retry? 3 by default
- `get_category_products`: if your page does allow for pagination without JS, it should be implemented here.

### How to deploy?
You'll need the use of cron (UNIX based OS, not tested on Windows OS).
For a cron to work you need to use absolute paths (the /usr/bin/... command) otherwise it won't work.

And you'd need to specify the time it will execute at, 1 AM, 4 AM (the on I used), 9 AM

#### Cron example
`0 4 * * * /usr/bin/python3 /my/working/directory/script.py`\
It will execute *everyday* at exactly 4:00 AM *forever*.

Now with a more real path:
`0 4 * * * /usr/bin/python3 /home/username/webscraping/script.py`

## Testing
Run them individually, modify the limit so it only downloads N number of categories or products, just to check if it works.\
Later on, you could just pick the categories you want to download (my example and use case), or simply, remove the category_urls param and use a `limit = ALL` to get everything on the website scraped.

There's no unit testing in webscraping as such, there's just manually checking if it works as required.