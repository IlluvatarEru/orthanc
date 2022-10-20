import datetime
import logging

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

from src.utils import root_folder
from src.utils.dates import get_last_tuesday_of_last_month, get_tuesday_of_last_week
from src.utils.formatting import format_price_to_million_tenge

root_folder.determine_root_folder()
from src.utils.constants import PATH_TO_DATA
from src.utils.logger import scrapper_logger, logger_init

SCRAPING_TIMEOUT = 30
logging.getLogger('WDM').setLevel(logging.NOTSET)

logger = scrapper_logger('Orthanc')


def get_selenium_scraping_options():
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('start-maximized')
    options.add_experimental_option('excludeSwitches', ['enable-automation'])
    options.add_experimental_option('useAutomationExtension', False)
    return options


def get_user_agent():
    return 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.50 ' \
           'Safari/537.36 '


class OrthancScrapper:
    """
    Base class used to scrap different RE websites
    """

    def __init__(self, main_url, base_flat_url, country, file_name, flat_characteristics_df):
        """

        :param main_url: str, url from which we want to scrap all the individual flats
        :param base_flat_url: str, base url for each flat usually there is just an id to add
        :param country: str
        :param file_name: str, name of the file
        :param flat_characteristics_df: pd.Df, empty df representing the format of data we need
        """
        logger_init(logger)
        self.driver = None
        self.flat_urls = []
        self.country = country
        self.data_path = PATH_TO_DATA + country + '/'
        self.flats_characteristics = flat_characteristics_df
        self.file_name = file_name
        self.main_url = main_url
        self.base_flat_url = base_flat_url
        self.init_webdriver()
        self.last_week_flats = self.read_last_week()

    def init_webdriver(self, trials=5):
        if trials > 0:
            logger.info('Initializing ' + logger.name + "'s driver")
            try:
                driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()),
                                          options=get_selenium_scraping_options())
                user_agent = get_user_agent()
                driver.execute_cdp_cmd('Network.setUserAgentOverride', {'userAgent': user_agent})
                self.driver = driver
            except:
                logger.error('Failed to init driver. Trying again.')
                self.init_webdriver(trials - 1)
        else:
            logger.error('Failed to init driver despite multiple trials.')

    def get_element_by_path(self, element_to_look_for):
        """
        Given a htnl element to look for (class etc) try to find it
        :param element_to_look_for: str, eg: //div[contains(text(),'Подъезд')]//following::div[1]
        :return:
        """

        logger.info('Looking for:' + element_to_look_for)
        try:
            result = WebDriverWait(self.driver, SCRAPING_TIMEOUT).until(
                EC.presence_of_element_located(
                    (By.XPATH, element_to_look_for)
                )
            )
            return result
        except Exception as e:
            logger.error('Failed to find element at url: ' + self.driver.current_url + '\nError is:' + str(e))

    def get_elements_by_path(self, elements_to_look_for):
        """
        Given a htnl element to look for (class etc) try to find it
        :param elements_to_look_for: list of str
        :return:
        """

        logger.info('Looking for:' + elements_to_look_for)
        try:
            result = WebDriverWait(self.driver, SCRAPING_TIMEOUT).until(
                EC.presence_of_all_elements_located(
                    (By.XPATH, elements_to_look_for)
                )
            )
            return result
        except Exception as e:
            logger.error('Failed to find element at url: ' + self.driver.current_url + '\nError is:' + str(e))

    def click_button(self, button_class_to_find):
        """
        Given the html code of a button, finds it and clicks on it
        :param button_class_to_find:
        :return:
        """
        logger.info('Clicking the following button:' + button_class_to_find)
        button = self.get_element_by_path(button_class_to_find)
        self.driver.execute_script("arguments[0].click();", button)

    def find_all_flats_urls_on_main_page(self):
        """
        To implement by the child class
        :return:
        """
        raise Exception('find_all_flats_urls_on_main_page not implemented')

    def find_flat_characteristics(self, flat_url):
        """
        To implement by the child class

        :param flat_url:
        :return:
        """
        raise Exception('find_flat_characteristics not implemented')

    def find_flats_characteristics(self):
        """
        Given the saved urls of all flats, find all the required characteristics and save them to the class + on disk
        :return:
        """
        logger.info('Starting to find flats characteristics')
        flats_characteristics = self.flats_characteristics
        for url in self.flat_urls:
            flat_characteristics = self.find_flat_characteristics(url)
            flats_characteristics = pd.concat([flats_characteristics, flat_characteristics])
        flats_characteristics = flats_characteristics.sort_values(by=['Entrance', 'Number Of Floors'])
        self.flats_characteristics = flats_characteristics.reset_index(drop=True)
        self.save_flats_to_file()
        return flats_characteristics

    def package_flat_characteristics(self, flat_id, entrance, max_floor, floor, surface, price, flat_url):
        last_week_flats = self.last_week_flats.copy()
        similar_flats_last_week = last_week_flats.loc[
            (last_week_flats['Surface'] == surface) &
            (last_week_flats['Floor'] == floor) &
            (last_week_flats['Number Of Floors'] == max_floor)
            ]
        # check if flat was already here last week but the add was removed and put back
        # so it has a different flat_id but all the same characteristics
        if len(similar_flats_last_week) > 0:
            flat_id = similar_flats_last_week['Id'].values[0]
            # if more than one similar flat, filter on price
            if len(similar_flats_last_week) > 1:
                similar_flats_last_week = similar_flats_last_week.loc[(last_week_flats['Price'] == price)]
                if len(similar_flats_last_week) > 0:
                    flat_id = similar_flats_last_week['Id'].values[0]
        return pd.DataFrame([[flat_id, entrance, max_floor, floor, surface, price, flat_url]],
                            columns=['Id', 'Entrance', 'Number Of Floors', 'Floor', 'Surface', 'Price', 'Link'])

    def save_flats_to_file(self):
        logger.info('Saving flats characteristics')
        today = datetime.datetime.today().strftime('%Y-%m-%d')
        flats = self.flats_characteristics.copy()
        flats.to_csv(self.data_path + today + '_' + self.file_name + '.csv', index=False)

    def count_by_building(self):
        logger.info('Counting flats by buildings/entrances')
        return self.flats_characteristics.groupby(['Entrance']).size().reset_index(name='counts')

    def get_flats_between_floors(self, floor_from, floor_to):
        logger.info('Filtering flats between min floor=' + str(floor_from) + 'and max floor=' + str(floor_to))
        filtered_flats = self.flats_characteristics.copy()
        filtered_flats = filtered_flats.loc[(filtered_flats['Floor'] >= floor_from) &
                                            (filtered_flats['Floor'] <= floor_to)]
        return filtered_flats

    def weekly_comparison(self):
        last_week_flats = self.last_week_flats.copy()
        this_week_flats = self.flats_characteristics.copy()
        ids_last_week = last_week_flats['Id'].unique().tolist()
        ids_this_week = this_week_flats['Id'].unique().tolist()
        all_ids = set(ids_this_week + ids_last_week)
        # get status of flats (New, Sold, NA)
        status = {}
        for flat_id in all_ids:
            flat_status = 'NA'
            last_week_flats.loc[last_week_flats['Id'] == flat_id, 'Status'] = 'NA'
            if (flat_id in ids_last_week) & (flat_id not in ids_this_week):
                flat_status = 'sold'
                last_week_flats.loc[last_week_flats['Id'] == flat_id, 'Status'] = 'Sold'
            if (flat_id in ids_this_week) & (flat_id not in ids_last_week):
                flat_status = 'new'
                this_week_flats.loc[this_week_flats['Id'] == flat_id, 'Status'] = 'New'
            status[flat_id] = flat_status

        # Add sold flats to current flats
        this_week_flats = pd.concat([this_week_flats, last_week_flats.loc[last_week_flats['Status'] == 'Sold']])

        for flat_id in all_ids:
            if status[flat_id] == 'NA':
                price_last_week = last_week_flats.loc[last_week_flats['Id'] == flat_id, 'Price'].values[0]
                price_this_week = this_week_flats.loc[this_week_flats['Id'] == flat_id, 'Price'].values[0]
                delta = price_this_week - price_last_week
                change = delta / price_last_week
                this_week_flats.loc[this_week_flats['Id'] == flat_id, 'Price Delta'] = \
                    '-' if delta == 0 else format_price_to_million_tenge(delta)
                this_week_flats.loc[this_week_flats['Id'] == flat_id, 'Price Change'] = \
                    '-' if change == 0 else str(round(change * 100, 2)) + '%'
        this_week_flats = this_week_flats.sort_values('Status', ascending=False, na_position='last')
        this_week_flats = this_week_flats.fillna('-')
        this_week_flats = this_week_flats.reset_index(drop=True)
        return this_week_flats

    def read_last_month(self):
        last_month = get_last_tuesday_of_last_month().strftime('%Y-%m-%d')
        return pd.read_csv(self.data_path + last_month + '_' + self.file_name + '.csv')

    def read_last_week(self):
        last_week = get_tuesday_of_last_week().strftime('%Y-%m-%d')
        last_week_flats = pd.read_csv(self.data_path + last_week + '_' + self.file_name + '.csv')
        last_week_flats['Id'] = last_week_flats['Id'].astype(str)
        last_week_flats['Number Of Floors'] = last_week_flats['Number Of Floors'].astype(int)
        last_week_flats['Floor'] = last_week_flats['Floor'].astype(int)
        return last_week_flats
