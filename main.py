from script import scrape, set_configuration
from websites.thewhiskyexchange.configuration import CONF as thewhiskyexchange

# All the scrapers to execute
scrapers = [
    thewhiskyexchange
]

def execute_scraper(scraper: dict) -> None:
    """
    Does all the pre-configuration to properly scrape the data, then scrapes it

    returns None
    """

    configuration = set_configuration(scraper)

    scrape(
        configuration['category_limit'],
        configuration['product_limit'],
        configuration['limit'],
        configuration['as_csv'],
        configuration['save_imgs'],
        configuration['display'],
        configuration['category_urls'],
        configuration['products'],
        configuration['detect_corruption'],
    )

def main() -> None:
    """
    Holds the main logic of the 'orquestrator'

    returns None
    """

    print('Ejecutando scripts desde el decorador main.py')

    # Execute all the scrapers    
    [ execute_scraper(scraper) for scraper in scrapers ]

# Executed only on user action, not on import
if __name__ == '__main__':
    main()