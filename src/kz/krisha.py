import pandas as pd

from src.orthanc_scrapper import OrthancScrapper

KRISHA_BASE_URL = 'https://krisha.kz/prodazha/kvartiry/'
KRISHA_BASE_FLAT_URL = 'https://krisha.kz/a/show/'

from src.utils.logger import scrapper_logger, logger_init

logger = scrapper_logger('Krisha')


def scrap_krisha(city='astana', jk_name='Nexpo', number_of_rooms=1):
    krisha_scrapper = KrishaScrapper(city, jk_name, number_of_rooms)
    krisha_scrapper.find_all_flats_urls_on_main_page()
    return krisha_scrapper.find_flats_characteristics()


def build_main_url_krisha(city, number_of_rooms=0, jk_id=0):
    main_url = KRISHA_BASE_URL + city + '/'
    if number_of_rooms > 0:
        main_url += '?das[live.rooms]=' + str(number_of_rooms)
    if jk_id > 0:
        main_url += '&das[map.complex]=' + str(jk_id)
    return main_url


def read_jk_ids_krisha():
    jk_ids = pd.read_csv('c:/dev/orthanc/src/kz/resources//krisha_jk_ids.csv')
    return dict(jk_ids.values)


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
                 flat_characteristics_df=pd.DataFrame(
                     columns=['Floor', 'Number Of Floors', 'Surface', 'Price', 'Entrance', 'Link'])
                 ):
        logger_init(logger)
        main_url = build_main_url_krisha(city, number_of_rooms, get_jk_id_krisha(jk_name))
        file_name = 'krisha_' + jk_name
        OrthancScrapper.__init__(self, main_url, KRISHA_BASE_FLAT_URL, 'kz', file_name, flat_characteristics_df)

    def find_all_flats_urls_on_main_page(self):
        logger.info('Starting to find all flats urls')
        driver = self.driver
        driver.get(self.main_url)
        elements = driver.find_elements_by_xpath(
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
            element_price = self.get_by_path("//div[starts-with(@class,'offer__price')]")
            price = int(element_price.text.replace(' \n〒', '').replace(",", "").replace(" ", ""))

            element_floor = self.get_by_path("//div[starts-with(@data-name,'flat.floor')]//following::div[3]")
            floor = element_floor.text
            if 'из' in floor:
                floor, max_floor = floor.split('из')
                max_floor = int(max_floor)
            else:
                floor = floor
                max_floor = 'na'
            floor = int(floor)

            element_surface = self.get_by_path("//div[starts-with(@data-name,'live.square')]//following::div[3]")
            surface = element_surface.text.split("м²")[0]
            surface = float(surface.replace('м²', '').replace(' ', ''))

            entrance = "na"

            return pd.DataFrame([[floor, max_floor, surface, price, entrance, flat_url]],
                                columns=['Floor', 'Number Of Floors', 'Surface', 'Price', 'Entrance', 'Link'])
        except Exception as e:
            logger.error('Failed to find flats characteristics for url:' + flat_url +
                         '\nReceived the following error' + str(e))
            return pd.DataFrame(columns=['Floor', 'Number Of Floors', 'Surface', 'Price', 'Entrance', 'Link'])
