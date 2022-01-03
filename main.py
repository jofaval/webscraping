from script import scrape, set_configuration
from websites.thewhiskyexchange.configuration import CONF as thewhiskyexchange

# All the scrapers to execute
scrapers = [
    thewhiskyexchange
]

STOP_ON_FAILURE = False

def execute_scraper(scraper: dict) -> None:
    """
    Does all the pre-configuration to properly scrape the data, then scrapes it

    returns None
    """
    try:
        # Set the configuration values
        configuration = set_configuration(scraper)

        # Actually scrape the page
        # TODO: parse to a one conf value to get all this? It'd be cleaner for sure.
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
    except Exception as e:
        print(e)

        if STOP_ON_FAILURE: raise Exception('Stopping the execution because of a failure...')

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