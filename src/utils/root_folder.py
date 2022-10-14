import os

global ROOT_FOLDER


def determine_root_folder():
    if os.name == 'nt':
        ROOT_FOLDER = 'C:/dev/'
    else:
        ROOT_FOLDER = '/home/mev/'
