import pandas as pd

from src.kz.read import read_bi_jk_ids
from src.orthanc_scrapper import OrthancScrapper
from src.utils.constants import STANDARD_FLAT_CHARACTERISTICS
from src.utils.emails import send_dataframe_by_email, get_email_text, get_email_object, build_platform_jk_file_name
from src.utils.formatting import format_prices_to_million_tenge

BI_BASE_FLAT_URL = 'https://bi.group/ru/flats?placementUUID='
BI_BASE_URL = 'https://bi.group/ru/filter?'
PLATFORM = 'BI'
from src.utils.logger import scrapper_logger, logger_init

logger = scrapper_logger('BI_Group')


def get_jk_id_bi(jk_name):
    jk_ids = read_bi_jk_ids()
    jk_id = jk_ids[jk_name]
    return '["' + str(jk_id) + '"]'


def build_main_url_bi(city, jk_name='Nexpo', number_of_rooms=0):
    main_url = BI_BASE_URL
    main_url += 'realEstateUUIDs=' + get_jk_id_bi(jk_name)
    if number_of_rooms > 0:
        main_url += '&roomCounts=[' + str(number_of_rooms) + ']'
    return main_url


def scrap_bi(city='astana', jk_name='Aqua', number_of_rooms=1):
    bi = KzBIGroup(city, jk_name, number_of_rooms)
    bi.find_all_flats_urls_on_main_page()
    bi.find_flats_characteristics()
    return bi.weekly_comparison()


def send_email_bi(city='astana', jk_name='Aqua', number_of_rooms=1):
    bi_flats = scrap_bi(city=city, jk_name=jk_name, number_of_rooms=number_of_rooms)
    email_object = get_email_object(PLATFORM, city, jk_name)
    text = get_email_text(PLATFORM, city, jk_name, number_of_rooms)
    bi_flats['Price'] = format_prices_to_million_tenge(bi_flats['Price'])
    send_dataframe_by_email(bi_flats, ['arthurimbagourdov@gmail.com'], email_object, text)


class KzBIGroup(OrthancScrapper):
    """
    BI Group is the leader of the RE market in Astana, we want to scrap new projects
    """

    def __init__(self, city, jk_name, number_of_rooms, flat_characteristics_df=STANDARD_FLAT_CHARACTERISTICS.copy()):
        logger_init(logger)
        main_url = build_main_url_bi(city, jk_name, number_of_rooms)
        file_name = build_platform_jk_file_name(PLATFORM, jk_name)
        OrthancScrapper.__init__(self, main_url, BI_BASE_FLAT_URL, 'kz', file_name, flat_characteristics_df)

    def find_all_flats_urls_on_main_page(self):
        logger.info('Starting to find all flats urls')
        driver = self.driver
        driver.get(self.main_url)

        n_ids_prev = 0
        driver.implicitly_wait(5)
        # search once
        self.find_flat_ids_from_img_urls()
        n_ids = len(self.flat_urls)

        # try to load more and to re-search
        while n_ids > n_ids_prev:
            try:
                self.load_more()
            except Exception as e:
                logger.error('Failed to load more.\nError:' + str(e))
            self.find_flat_ids_from_img_urls()
            n_ids_prev = n_ids
            n_ids = len(self.flat_urls)
        return self.flat_urls

    def find_flat_ids_from_img_urls(self):
        logger.info('Starting to find all flats ids from urls')
        element_urls = self.get_elements_by_path("//img[starts-with(@class,'MRE-jss')]")
        for element_url in element_urls:
            uid = element_url.get_attribute("src").split("/")[-2]
            self.flat_urls.append(self.base_flat_url + uid)
        self.flat_urls = list(set(self.flat_urls))

    def find_flat_characteristics(self, flat_url):
        logger.info('Starting to find all flats characteristics')
        self.init_webdriver()
        driver = self.driver
        driver.get(flat_url)
        try:
            flat_id = flat_url.split('=')[-1]
            element_price = self.get_element_by_path("//div[contains(text(),'Стоимость')]//following::div[1]")
            price = float(element_price.text.replace(' ₸', '').replace(",", ""))

            element_floor = self.get_element_by_path("//div[contains(text(),'Этаж')]//following::div[1]")
            floor = element_floor.text
            floor, max_floor = floor.split(' из ')
            floor = int(floor)
            max_floor = int(max_floor)

            element_surface = self.get_element_by_path("//div[contains(text(),'Площадь')]//following::div[1]")
            surface = element_surface.text.split("м²")[0]
            surface = float(surface.replace('м²', '').replace(' ', ''))

            element_entrance = self.get_element_by_path("//div[contains(text(),'Подъезд')]//following::div[1]")
            entrance = element_entrance.text

            return self.package_flat_characteristics(flat_id, entrance, max_floor, floor, surface, price, flat_url)

        except Exception as e:
            logger.error('Failed to find flats characteristics for url:' + flat_url +
                         '\nReceived the following error' + str(e))
            return STANDARD_FLAT_CHARACTERISTICS.copy()

    def load_more(self):
        """
        Clicks on load more button
        :return:
        """
        logger.info('Loading more flats on the page...')
        button_class_to_find = "//button[starts-with(@class, 'MRE-MuiButtonBase-root MRE-MuiButton-root " \
                               "MRE-MuiButton-contained MRE-jss')]//span[text()='Показать еще'] "
        self.click_button(button_class_to_find)
