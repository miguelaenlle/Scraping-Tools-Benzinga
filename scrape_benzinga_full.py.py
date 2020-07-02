# https://www.benzinga.com/stock/nnvc/
import pandas as pd
nasdaq_listed = pd.read_csv('nasdaq-listed-symbols_csv.csv')['Symbol'].values
other_listed = pd.read_csv('other-listed_csv.csv')['ACT Symbol'].values
nyse_listed = pd.read_csv('nyse-listed_csv.csv')['ACT Symbol'].values

symbols = []
for stock in nasdaq_listed:
	if stock not in symbols:
		symbols.append(stock)
for stock in other_listed:
	if stock not in symbols:
		symbols.append(stock)
for stock in nyse_listed:
	if stock not in symbols:
		symbols.append(stock)
from joblib import Parallel, delayed
# print(symbols)
# print(len(symbols))
import time
from selenium import webdriver
if 'benzinga_scrape' not in os.listdir():
	print('Creating folder /benzinga_scrape...')
	os.mkdir('benzinga_scrape')
if 'analyst_ratings' not in os.listdir('benzinga_scrape'):
	print('Creating folder /benzinga_scrape/analyst_ratings...')
	os.mkdir('benzinga_scrape/analyst_ratings')
if 'partner_headlines' not in os.listdir('benzinga_scrape'):
	print('Creating folder /benzinga_scrape/partner_headlines')
	os.mkdir('benzinga_scrape/partner_headlines')

def get_benzinga_data(stock):
	ffox_options = webdriver.FirefoxOptions()
	ffox_options.set_headless()
	tol_amount = 5
	tols_curr = 0
	ff = webdriver.Firefox(options = ffox_options)
	try:
		ff.get('https://benzinga.com/stock/{}'.format(stock.lower()))
		time.sleep(5)
		while True:
			try:
				elem = ff.find_element_by_xpath('/html/body/div[6]/div/div[2]/div[2]/div[1]/div/div[7]/div/div/div[1]/a/span[1]')
				ff.execute_script("arguments[0].scrollIntoView();", elem)
				time.sleep(0.3)
				elem.click()
				time.sleep(2)

			except Exception as e:
				# ff.dismiss()
				try:
					ff.find_element_by_xpath('//*[@id="onesignal-popover-cancel-button"]').click()
				except:
					pass
					# print('FAILED')
				try:
					ff.find_element_by_xpath('/html/body/div[21]/div/div/div/div/div/div/div/div[1]/div/div/div/div/div/div[4]/div/div').click()
				except:
					pass
					# print('FAILED (2)')

				# print(e)
				time.sleep(1)
				tols_curr += 1
				
				if tol_amount < tols_curr:
					break


		analyst_ratings = []
		current_index = 1
		while True:
			try:
				header = '/html/body/div[6]/div/div[2]/div[2]/div[1]/div/div[7]/div/div/div[1]/ul/li[{}]/a'.format(current_index)
				headline = ff.find_element_by_xpath(header).text
				url = ff.find_element_by_xpath(header).get_attribute('href')
				publisher = ff.find_element_by_xpath('/html/body/div[6]/div/div[2]/div[2]/div[1]/div/div[7]/div/div/div[1]/ul/li[{}]/span[1]'.format(current_index)).text
				date = ff.find_element_by_xpath('/html/body/div[6]/div/div[2]/div[2]/div[1]/div/div[7]/div/div/div[1]/ul/li[{}]/span[2]'.format(current_index)).text
				analyst_ratings.append([headline, url, publisher, date])
				# print(analyst_ratings[-1])
				current_index += 1
			except Exception as e:
				# print(e)
				break
		analyst_ratings = pd.DataFrame(analyst_ratings, columns = ['headline', 'url', 'publisher', 'date'])
		analyst_ratings.to_csv('benzinga_scrape/analyst_ratings/{}.csv'.format(stock))

		while True:
			try:
				elem = ff.find_element_by_xpath('/html/body/div[6]/div/div[2]/div[2]/div[1]/div/div[9]/div/div/div/a/span[1]')
				ff.execute_script("arguments[0].scrollIntoView();", elem)
				time.sleep(0.3)
				elem.click()
				time.sleep(2)

			except Exception as e:
				# ff.dismiss()
				try:
					ff.find_element_by_xpath('//*[@id="onesignal-popover-cancel-button"]').click()
				except:
					pass
					# print('FAILED')
				try:
					ff.find_element_by_xpath('/html/body/div[21]/div/div/div/div/div/div/div/div[1]/div/div/div/div/div/div[4]/div/div').click()
				except:
					pass
					# print('FAILED (2)')
				print(e)
				time.sleep(1)
				tols_curr += 1
				
				if tol_amount < tols_curr:
					break


		partner_headlines = []
		current_index = 1
		while True:
			try:
				header = '/html/body/div[6]/div/div[2]/div[2]/div[1]/div/div[9]/div/div/div/ul/li[{}]/a'.format(current_index)
				headline = ff.find_element_by_xpath(header).text
				url = ff.find_element_by_xpath(header).get_attribute('href')
				publisher = ff.find_element_by_xpath('/html/body/div[6]/div/div[2]/div[2]/div[1]/div/div[9]/div/div/div/ul/li[{}]/span[1]'.format(current_index)).text
				date = ff.find_element_by_xpath('/html/body/div[6]/div/div[2]/div[2]/div[1]/div/div[9]/div/div/div/ul/li[{}]/span[2]'.format(current_index)).text
				partner_headlines.append([headline, url, publisher, date])
				# print(partner_headlines[-1])
				current_index += 1
			except Exception as e:
				# print(e)
				break
		partner_headlines = pd.DataFrame(partner_headlines, columns = ['headline', 'url', 'publisher', 'date'])
		partner_headlines.to_csv('benzinga_scrape/partner_headlines/{}.csv'.format(stock))
		ff.close()	
	except Exception as e:
		print(e)
		ff.close()
# get_benzinga_data('NNVC')
from joblib import Parallel, delayed
core_count = int(input('How many cores to use?: '))

print('Starting data mine...')
Parallel(core_count, 'loky', verbose = 10)(delayed(get_benzinga_data)(stock) for stock in symbols)

import os
import time
import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
analyst_ratings = pd.read_csv('raw_analyst_ratings.csv', index_col = 0)
partner_headlines = pd.read_csv('raw_partner_headlines.csv', index_col = 0)
from lxml import html
def get_basic_data_for_url(url):
    try:
        page = requests.get(url)
        text = page.text
        soup = BeautifulSoup(text, 'html.parser')
        date = soup.findAll('span', {'class': 'date'})[0].text
        tree = html.fromstring(page.content)
        title = tree.xpath('//*[@id="title"]')[0].text
        return title, date
    except Exception as e:
        print(e)
        return np.nan, np.nan
from joblib import Parallel, delayed
from datetime import datetime
def wait(timestamp):
	
	print('Wait to {}'.format(timestamp))
	while True:
		current_timestamp = datetime.now()
		current_timestamp = pd.Timestamp(current_timestamp, tz = 'America/Chicago')
		if current_timestamp > timestamp:
			break
		else:
			time.sleep(5)

to_scrape = []
if 'benzinga_processed' not in os.listdir():
	os.mkdir('benzinga_processed')
if 'analyst_ratings_processed' not in 'benzinga_processed':
	os.mkdir('benzinga_processed/analyst_ratings_processed')
for rating in os.listdir('benzinga_scrape/analyst_ratings'):
	if rating + '.csv' not in os.listdir('benzinga_processed/analyst_ratings_processed'):
		to_scrape.append(rating)
# print(len(to_scrape))
# wait(pd.Timestamp('2020-06-13 16:00', tz = 'America/Chicago'))
for analyst_rating in tqdm(to_scrape):
	path = 'benzinga_scrape/analyst_ratings/{}'.format(analyst_rating)
	data = pd.read_csv(path, index_col = 0)
	data['stock'] = analyst_rating[:-4]
	if len(data) > 0:
		if analyst_rating not in os.listdir('benzinga_processed/analyst_ratings_processed'):
			# print(len(data))
			datas = Parallel(8, 'loky', verbose = 10)(delayed(get_basic_data_for_url)(url) for url in data['url'])
			datas = pd.DataFrame(datas, columns = ['title', 'date'])
			datas.to_csv('benzinga_processed/analyst_ratings_processed/{}.csv'.format(analyst_rating))
# /html/body/div[6]/div/div[2]/div[2]/div[1]/div/div[7]/div/div/div[1]/ul/li[{}]/a - analyst ratings
# /html/body/div[6]/div/div[2]/div[2]/div[1]/div/div[7]/div/div/div[1]/ul/li[{}]/span[1] - publisher
# /html/body/div[6]/div/div[2]/div[2]/div[1]/div/div[7]/div/div/div[1]/ul/li[{}]/span[2] - date

# /html/body/div[6]/div/div[2]/div[2]/div[1]/div/div[9]/div/div/div/ul/li[{}]/a - partner headlines
# /html/body/div[6]/div/div[2]/div[2]/div[1]/div/div[9]/div/div/div/ul/li[{}]/span[1] - publisher
# /html/body/div[6]/div/div[2]/div[2]/div[1]/div/div[9]/div/div/div/ul/li[{}]/span[2] - date
