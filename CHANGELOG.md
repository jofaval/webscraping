# CHANGELOG #
All the log of changes on the project/repository

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## 2022-01-03
### Added
- `PythonDoc` the whole `script.py` and other missing functions
- Add `get_url_id` to the `configuration` file
- Implement `has_required_fields` not to execute an incomplete `scraper`, with exception handling in the `orchestrator`
- Add CHANGELOG standards

### Modified
- Retyped, and add, missing data parameter types, or incorrectly put, `list` instead of `List[str]`

## 2022-01-02
### Added
- `main.py` folder as a sort of `decorator` or `orchestrator` for all the scraping
- Properly and fully implement `CONF` dict usage with a `main` `orchestrator` and separate sites configurations

## 2021-12-20
### Modified
- Define some of the `CONF` dict base structure
- Change all the constant calls, they now use the `CONF` dict

## 2021-12-19
### Added
- Created first wiki page
- Create CHANGELOG wiki page
- Add LICENSE.md

### Modified
- Greatly improve README.md

## 2021-12-15
### Added
- Webscrapping
  - Uploaded raw versions, cleaning and refactoring left to do
- All the previous log on the CHANGELOG (as accurate as possible)

### Modified
- Updated a little bit more the README.md, a lot left to explain
- Comments translated from Spanish to English using Google Translate
- Renamed script from "webscrapping.py" to "script.py"

## 2021-12-14
### Added
- Base README.md created
- CHANGELOG.md created
- Github repository created

## 2021-12-13
### Added
- Repository initialized

## 2021-12-05
### Added
- Price discount to the webscraper (price_prev)
- Implemented all the required elements for a cron configuration to work on a Linux OS (tested with Debian distribution)

## 2021-11-21
### Modified
- Implemented the Therading Pool executor for a much more efficient threading queue

## 2021-11-06 - 2021-11-21
### Modified
- Refactoring, cleaning and reestructuring the code

## 2021-11-06
### Added
- Threads system for threading with a "semi-manual queue"

### Modified
- Cleaning the code
- Refactoring the code and logic
- Extract functionality into separate function layers

## 2021-11-05
### Added
- Extra layer of functions

### Modified
- Cleaned and reestructured a little bit the code

## 2021-11-01
### Added
- FIELDS system implementation for a dict of fields to scrape

### Modified
- Cleaned the code

## 2021-10-30
### Added
- Project started
- CSV download
- E-Commerce webscraping