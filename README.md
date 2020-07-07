# Benzinga.com Scraping Tools

These are the scripts used for the getting the dataset shown here: https://www.kaggle.com/miguelaenlle/massive-stock-news-analysis-db-for-nlpbacktests

## Getting Started

### Prerequisites

All the prerequisite files are in requirements.txt
Also, install selenium webdriver firefox.

### Installing

1. Download this repository
2. Install the requirements via $ pip install -r requirements.txt

## Usage

scrape_benzinga.py is for getting scrape data for individual stocks.
scrape_benzinga_full.py is for scraping the entire Benzinga news database.

To use scrape_benzinga.py, 
- Put your script in this directory
- import scrape_benzinga

scrape_benzinga has two functions:
- get_benzinga_data
- get_benzinga_data_with_lookback

**get_benzinga_data_with_lookback** takes 1 argument: stock
It returns all analyst ratings, not partner headlines because analyst ratings are the only type of article that can be scraped for their exact dates.
I might add partner headline support in the future.
**get_benzinga_data** takes 2 arguments: stock, days_to_lookback
This gets the entire history of stock news for the designated stock whose release dates are within the days_to_lookback range: 
e.g. 7 day lookback returns all articles published earlier than 7 days ago.
This function returns both analyst_ratings and partner_headlines

To use scraper_benzinga_full.py, simply call the script via python scraper_benzinga_full.py and follow the prompts shown on the
command line.
