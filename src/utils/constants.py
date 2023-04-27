# from src.utils.root_folder import ROOT_FOLDER
import pandas as pd

from src.utils import root_folder

PATH_TO_DATA = root_folder.ROOT_FOLDER + 'data/re/'
LOGGING_FORMAT = '%(asctime)s:%(levelname)s:%(funcName)s:%(lineno)d:%(message)s'
LOGGING_PATH = root_folder.ROOT_FOLDER + 'orthanc/logs/'
PATH_TO_PASSWORDS = root_folder.ROOT_FOLDER + 'keys/'

STANDARD_FLAT_CHARACTERISTICS = pd.DataFrame(
    columns=['Id', 'Entrance', 'Number Of Floors', 'Floor', 'Surface', 'Price', 'Link'])
