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

SCRAPING_TIMEOUT = 30
logging.getLogger('WDM').setLevel(logging.NOTSET)


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
        self.driver = None
        self.flat_urls = []
        self.country = country
        self.data_path = 'C:/dev/data/re/' + country + '/'
        self.flats_characteristics = flat_characteristics_df
        self.main_url = main_url
        self.base_flat_url = base_flat_url
        self.init_webdriver()
        self.file_name = file_name

    def init_webdriver(self):
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()),
                                  options=get_selenium_scraping_options())
        user_agent = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.50 ' \
                     'Safari/537.36 '
        driver.execute_cdp_cmd('Network.setUserAgentOverride', {'userAgent': user_agent})

        self.driver = driver

    def get_by_path(self, element_to_look_for):
        """
        Given a htnl element to look for (class etc) try to find it
        :param element_to_look_for: str, eg: //div[contains(text(),'Подъезд')]//following::div[1]
        :return:
        """
        return WebDriverWait(self.driver, SCRAPING_TIMEOUT).until(
            EC.presence_of_element_located(
                (By.XPATH, element_to_look_for)
            )
        )

    def click_button(self, button_class_to_find):
        """
        Given the html code of a button, finds it and clicks on it
        :param button_class_to_find:
        :return:
        """
        button = self.get_by_path(button_class_to_find)
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
        flats_characteristics = self.flats_characteristics
        for url in self.flat_urls:
            flat_characteristics = self.find_flat_characteristics(url)
            flats_characteristics = pd.concat([flats_characteristics, flat_characteristics])
        self.flats_characteristics = flats_characteristics.reset_index(drop=True)
        self.save_flats_to_file()
        return flats_characteristics

    def save_flats_to_file(self):
        today = datetime.datetime.today().strftime('%Y-%m-%d')
        self.flats_characteristics.to_csv(self.data_path + today + '_' + self.file_name + '.csv')

    def count_by_building(self):
        return self.flats_characteristics.groupby(['Entrance']).size().reset_index(name='counts')

    def get_flats_between_floors(self, floor_from, floor_to):
        filtered_flats = self.flats_characteristics.copy()
        filtered_flats = filtered_flats.loc[(filtered_flats['Floor'] >= floor_from) &
                                            (filtered_flats['Floor'] <= floor_to)]
        return filtered_flats