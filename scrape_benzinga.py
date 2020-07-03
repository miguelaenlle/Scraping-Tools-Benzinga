import time
import copy
from lxml import html
import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from selenium import webdriver

def get_benzinga_data(stock, days_to_look_back):
	ffox_options = webdriver.FirefoxOptions()
	minimum_date = pd.Timestamp(datetime.utcnow(), tz = 'UTC') - pd.Timedelta('{} days'.format(days_to_look_back))
	ffox_options.set_headless()
	tol_amount = 5
	tols_curr = 0
	bp = False
	ff = webdriver.Firefox(options = ffox_options)
	try:
		ff.get('https://benzinga.com/stock/{}'.format(stock.lower()))
		time.sleep(5)
		analyst_ratings = []
		current_index = 1
		while True:
			try:
				elem = ff.find_element_by_xpath('/html/body/div[6]/div/div[2]/div[2]/div[1]/div/div[7]/div/div/div[1]/a/span[1]')
				ff.execute_script("arguments[0].scrollIntoView();", elem)
				time.sleep(0.3)
				elem.click()
				time.sleep(2)

				while True:
					try:
						header = '/html/body/div[6]/div/div[2]/div[2]/div[1]/div/div[7]/div/div/div[1]/ul/li[{}]/a'.format(current_index)
						headline = ff.find_element_by_xpath(header).text
						url = ff.find_element_by_xpath(header).get_attribute('href')
						publisher = ff.find_element_by_xpath('/html/body/div[6]/div/div[2]/div[2]/div[1]/div/div[7]/div/div/div[1]/ul/li[{}]/span[1]'.format(current_index)).text
						date = ff.find_element_by_xpath('/html/body/div[6]/div/div[2]/div[2]/div[1]/div/div[7]/div/div/div[1]/ul/li[{}]/span[2]'.format(current_index)).text
						# print(date)
						# print([headline, date])
						if ('-0400' not in date) & ('-0500' not in date):
							# # print(date)
							if 'ago' in date:
								if date == 'a day ago':
									pass
								else:

									# # print(date)
									timeperiod = date[::-1]
									timeperiod = timeperiod[timeperiod.find(' ') + 1:][::-1]
									timeperiod = pd.Timedelta(timeperiod)
									# print('FINDME!!!!')
									# print(days_to_look_back)
									# print(timeperiod)
									# print(pd.Timedelta('{} days'.format(days_to_look_back)) > timeperiod)
									if timeperiod > pd.Timedelta('{} days'.format(days_to_look_back)):
										bp = True
										break
							else:
								# # print(date)
								date = pd.Timestamp(date, tz = 'America/Detroit')
								if date < minimum_date - pd.Timedelta('1 day'):
									bp = True
									break
						analyst_ratings.append([headline, url, publisher, date])
						
						current_index += 1
						# print(current_index)
					except Exception as e:
						# print(e)
						break
				if bp:
					break

			except Exception as e:
				# print(e)
				# print(e)
				# ff.dismiss()
				try:
					ff.find_element_by_xpath('//*[@id="onesignal-popover-cancel-button"]').click()
				except:
					pass
					# print('FAILED')
				try:
					ff.find_element_by_xpath('/html/body/div[22]/div/div/button').click()
				except:
					pass
					# print('FAILED (1)')
				try:
					ff.find_element_by_xpath('//*[@id="shreveport-ButtonElement--zs4zLUkKVVfSEq8qDkow"]').click()
				except:
					pass
					# print('FAILED (2)')

				# print(e)
				time.sleep(1)
				tols_curr += 1
				
				if tol_amount < tols_curr:
					break
		ff.close()
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
				# print(e)
				return np.nan, np.nan
		analyst_ratings = pd.DataFrame(analyst_ratings, columns = ['headline', 'url', 'publisher', 'date'])
		datas = []
		for i in analyst_ratings.index:
			url = analyst_ratings.loc[i, 'url']
			datas.append(get_basic_data_for_url(url))
		datas = pd.DataFrame(datas, columns = ['headline', 'date'])
		analyst_ratings[datas.columns] = datas
		return analyst_ratings	
	except Exception as e:
		# print(e)
		ff.close()
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