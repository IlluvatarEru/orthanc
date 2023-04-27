import os

global ROOT_FOLDER


def determine_root_folder():
    global ROOT_FOLDER
    if os.name == 'nt':
        ROOT_FOLDER = 'C:/dev/'
    else:
        ROOT_FOLDER = '~/dev/'
