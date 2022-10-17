import pandas as pd


def read_jk_ids_krisha():
    jk_ids = pd.read_csv('src/kz/resources/krisha_jk_ids.csv')
    #jk_ids = pd.read_csv('../src/kz/resources/krisha_jk_ids.csv')
    return dict(jk_ids.values)


def read_bi_jk_ids():
    jk_ids = pd.read_csv('src/kz/resources/bi_jk_ids.csv')
    #jk_ids = pd.read_csv('../src/kz/resources/bi_jk_ids.csv')
    return dict(jk_ids.values)
