import pandas as pd

from src.kz.read import read_jk_ids_krisha
from src.orthanc_scrapper import OrthancScrapper
from src.utils.constants import STANDARD_DF
from src.utils.emails import send_dataframe_by_email, get_email_text, get_email_object, build_platform_jk_file_name
from src.utils.formatting import format_price_to_million_tenge, format_prices_to_million_tenge

PLATFORM = 'Krisha'
KRISHA_BASE_URL = 'https://krisha.kz/prodazha/kvartiry/'
KRISHA_BASE_FLAT_URL = 'https://krisha.kz/a/show/'
from src.utils.logger import scrapper_logger, logger_init

logger = scrapper_logger(PLATFORM)


def scrap_krisha(city='astana', jk_name='Nexpo', number_of_rooms=1):
    krisha_scrapper = KrishaScrapper(city, jk_name, number_of_rooms)
    krisha_scrapper.find_all_flats_urls_on_main_page()
    return krisha_scrapper.find_flats_characteristics()


def send_email_krisha(city='astana', jk_name='Nexpo', number_of_rooms=1):
    krisha_flats = scrap_krisha(city=city, jk_name=jk_name, number_of_rooms=number_of_rooms)
    email_object = get_email_object(PLATFORM, city, jk_name)
    text = get_email_text(PLATFORM, city, jk_name, number_of_rooms)
    krisha_flats['Price'] = format_prices_to_million_tenge(krisha_flats['Price'])
    send_dataframe_by_email(krisha_flats, ['arthurimbagourdov@gmail.com'], email_object, text)


def build_main_url_krisha(city, number_of_rooms=0, jk_id=0):
    main_url = KRISHA_BASE_URL + city + '/'
    if number_of_rooms > 0:
        main_url += '?das[live.rooms]=' + str(number_of_rooms)
    if jk_id > 0:
        main_url += '&das[map.complex]=' + str(jk_id)
    return main_url


def get_jk_id_krisha(jk_name):
    jk_ids = read_jk_ids_krisha()
    return jk_ids[jk_name.title()]


class KrishaScrapper(OrthancScrapper):
    """
    Krisha is the main website in Kz to find flats to rent and to buy
    """

    def __init__(self,
                 city,
                 jk_name,
                 number_of_rooms=1,
                 flat_characteristics_df=STANDARD_DF.copy()
                 ):
        logger_init(logger)
        main_url = build_main_url_krisha(city, number_of_rooms, get_jk_id_krisha(jk_name))
        file_name = build_platform_jk_file_name(PLATFORM, jk_name)
        OrthancScrapper.__init__(self, main_url, KRISHA_BASE_FLAT_URL, 'kz', file_name, flat_characteristics_df)

    def find_all_flats_urls_on_main_page(self):
        logger.info('Starting to find all flats urls')
        driver = self.driver
        driver.get(self.main_url)
        elements = self.get_elements_by_path(
            "//div[starts-with(@class,'a-card a-storage-live ddl_product ddl_product_link not-colored is-visibl')]")
        for element in elements:
            uid = element.get_attribute("data-id")
            self.flat_urls.append(self.base_flat_url + uid)
        self.flat_urls = list(set(self.flat_urls))
        return self.flat_urls

    def find_flat_characteristics(self, flat_url):
        logger.info('Starting to find all flats characteristics')
        self.init_webdriver()
        driver = self.driver
        driver.get(flat_url)
        try:
            flat_id = flat_url.split('/')[-1]
            element_price = self.get_element_by_path("//div[starts-with(@class,'offer__price')]")
            price = float(element_price.text.replace(' \n〒', '').replace(",", "").replace(" ", ""))

            element_floor = self.get_element_by_path("//div[starts-with(@data-name,'flat.floor')]//following::div[3]")
            floor = element_floor.text
            if 'из' in floor:
                floor, max_floor = floor.split('из')
                max_floor = int(max_floor)
            else:
                floor = floor
                max_floor = 'na'
            floor = int(floor)

            element_surface = self.get_element_by_path(
                "//div[starts-with(@data-name,'live.square')]//following::div[3]")
            surface = element_surface.text.split("м²")[0]
            surface = float(surface.replace('м²', '').replace(' ', ''))

            entrance = "na"

            return pd.DataFrame([[flat_id, entrance, max_floor, floor, surface, price, flat_url]],
                                columns=['Id','Entrance', 'Number Of Floors', 'Floor', 'Surface', 'Price', 'Link'])
        except Exception as e:
            logger.error('Failed to find flats characteristics for url:' + flat_url +
                         '\nReceived the following error' + str(e))
            return STANDARD_DF.copy()