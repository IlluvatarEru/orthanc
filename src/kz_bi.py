import pandas as pd

from src.orthanc_scrapper import OrthancScrapper


def get_bi_flats():
    base_flat_url = 'https://bi.group/ru/flats?placementUUID='
    main_url = 'https://bi.group/ru/filter?propertyTypes=%5B%225990a172-812a-4fee-b4f5-c860cca824d7%22%2C%22b6e20785' \
               '-9b33-46a9-86b5-707fdbffe314%22%2C%22a6deff39-99d2-4a4c-982c-b245b7e43037%22%2C%22b3be088f-52d2-47af' \
               '-93d5-0667312547c9%22%2C%22eb845125-c2b7-4d8a-93d7-015080355f78%22%2C%22c2c7b7b0-6b3e-4b9a-b729' \
               '-64a750fe271d%22%2C%2264d2ff0a-22d9-4fa3-92aa-6391faf460f9%22%2C%22e211be72-2986-4ea1-8991' \
               '-bfe7233ac4c2%22%2C%22253c179e-7a74-4096-a0e9-a63df0e24bb5%22%2C%228f72b6b1-0453-4938-9775' \
               '-0f2f3a836ccd%22%5D&realEstatePage=%22PLACEMENTS%22&floorMax=10&realEstateUUIDs=%5B%22c68f11e7-48d1' \
               '-11eb-a83d-00155d106579%22%5D&cityUUID=%224c0fe725-4b6f-11e8-80cf-bb580b2abfef%22&floorMin=3 '
    country = 'kz'
    file_name = 'bi_aqua'
    flats_characteristics = pd.DataFrame(columns=['Floor', 'Surface', 'Price', 'Entrance', 'Link'])
    bi = KzBIGroup(main_url, base_flat_url, country, file_name, flats_characteristics)
    bi.find_all_flats_urls_on_main_page()
    bi.find_flats_characteristics()


class KzBIGroup(OrthancScrapper):
    """
    BI Group is the leader of the RE market in Astana, we want to scrap new projects
    """

    def __int__(self, main_url, base_flat_url, country, file_name, flat_characteristics_df):
        OrthancScrapper.__init__(self, main_url, base_flat_url, country, file_name, flat_characteristics_df)

    def find_all_flats_urls_on_main_page(self):
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
                print("Exception in While Loop")
                print(e)
            self.find_flat_ids_from_img_urls()
            n_ids_prev = n_ids
            n_ids = len(self.flat_urls)
        return self.flat_urls

    def find_flat_ids_from_img_urls(self):
        element_urls = self.driver.find_elements_by_xpath("//img[starts-with(@class,'MRE-jss')]")
        print('element_urls', element_urls)
        for element_url in element_urls:
            uid = element_url.get_attribute("src").split("/")[-2]
            self.flat_urls.append(self.base_flat_url + uid)
        self.flat_urls = list(set(self.flat_urls))

    def find_flat_characteristics(self, flat_url):
        driver = self.driver
        driver.get(flat_url)

        element_price = self.get_by_path("//div[contains(text(),'Стоимость')]//following::div[1]")
        price = int(element_price.text.replace(' ₸', '').replace(",", ""))

        element_floor = self.get_by_path("//div[contains(text(),'Этаж')]//following::div[1]")
        floor = element_floor.text

        element_surface = self.get_by_path("//div[contains(text(),'Площадь')]//following::div[1]")
        surface = element_surface.text.replace(' м²', '')

        element_entrance = self.get_by_path("//div[contains(text(),'Подъезд')]//following::div[1]")
        entrance = element_entrance.text
        return pd.DataFrame([[floor, surface, price, entrance, flat_url]],
                            columns=['Floor', 'Surface', 'Price', 'Entrance', 'Link'])

    def load_more(self):
        """
        Clicks on load more button
        :return:
        """
        button_class_to_find = "//button[starts-with(@class, 'MRE-MuiButtonBase-root MRE-MuiButton-root " \
                               "MRE-MuiButton-contained MRE-jss')]//span[text()='Показать еще'] "
        self.click_button(button_class_to_find)
