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
URL_FILE_PATH = 'C:/dev/re_kz/bi_urls.txt'

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


class SeleniumScrapper:
    """
    Very often the pattern when scrapping a RE website is:
    - we have a main url
    - from which we can get several urls for different flats
    - for each flat (ie url) we want to get some info
    """

    def __init__(self, main_url, base_flat_url, file_name, flat_characteristics_df):
        self.driver = None
        self.flat_urls = []
        self.data_path = 'C:/dev/data/re/'
        self.flats_characteristics = flat_characteristics_df
        self.main_url = main_url
        self.base_flat_url = base_flat_url
        self.init_webdriver()
        self.file_name = file_name

    def init_webdriver(self):
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()),
                                  options=get_selenium_scraping_options())
        user_agent = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.50 Safari/537.36'
        driver.execute_cdp_cmd('Network.setUserAgentOverride', {'userAgent': user_agent})

        self.driver = driver

    def get_by_path(self, element_to_look_for):
        return WebDriverWait(self.driver, SCRAPING_TIMEOUT).until(
            EC.presence_of_element_located(
                (By.XPATH, element_to_look_for)
            )
        )

    def click_button(self, button_class_to_find):
        # button = driver.find_element_by_xpath(button_class_to_find)
        button = self.get_by_path(button_class_to_find)
        self.driver.execute_script("arguments[0].click();", button)

    def find_all_flats_urls_on_main_page(self):
        raise Exception('find_all_flats_urls_on_main_page not implemented')

    def find_flat_characteristics(self, flat_url):
        raise Exception('find_flat_characteristics not implemented')

    def find_flats_characteristics(self):
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
