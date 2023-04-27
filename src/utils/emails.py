import io
import os
import sys
from email.encoders import encode_base64
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from smtplib import SMTP_SSL as SMTP

import pandas as pd
from pretty_html_table import build_table


def send_email(sender, sender_name, receivers, user, password, content, subject, content_format='txt',
               plot_to_send=None):
    """

    :param sender: a string, the email address to send from
    :param sender_name: a string, the sender name to be displayed in the email
    :param receivers: a list of string, containing the email addresses to send to
    :param user: a string, the username to log in the smtp server
    :param password: a string, the password to log in the smtp server
    :param content: a string, the content of the email, can be html formatted
    :param subject: a string, the subject of the email
    :param content_format: a string, the format to use (like "html")
    :param plot_to_send: a plot object, a plot to attach to the email
    :return:
    """
    try:
        msg = MIMEMultipart()
        if content_format == 'html':
            msg.attach(MIMEText(content, 'html'))
        else:
            msg.attach(MIMEText(content))
        msg['Subject'] = subject
        msg['From'] = sender_name
        msg['to'] = ','.join(receivers)
        if plot_to_send is None:
            pass
        else:
            buf = io.BytesIO()
            plot_to_send.figure.savefig(buf, format='png')
            buf.seek(0)
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(buf.read())
            encode_base64(part)
            part.add_header('Content-Disposition',
                            'attachment; filename="%s"' % plot_to_send.figure.axes[0].get_title() + '.png')
            msg.attach(part)
        conn = SMTP('boite.o2switch.net')
        conn.set_debuglevel(True)
        conn.login(user, password)
        try:
            conn.sendmail(sender, receivers, msg.as_string())
            print('Email is Sent')
        except Exception as e:
            print(e)
        finally:
            conn.quit()
    except Exception as e:
        sys.exit(f'mail failed; with error:\n{e}')


def send_email_from_ops(receivers, content, subject, content_format='txt', plot_to_send=None):
    """
    :param receivers: a list of string, containing the email addresses to send to
    :param content: a string, the content of the email, can be html formatted
    :param subject: a string, the subject of the email
    :param content_format: a string, the format to use (like "html")
    :param plot_to_send: a plot object, a plot to attach to the email
    :return:
    """
    sender = 'ops@orthanc.capital.bagourd.com'
    user = sender
    password = os.environ.get('EMAIL_OPS_ORTHANC')
    print(f'password={password}')
    send_email(sender, sender, receivers, user, password, content, subject, content_format, plot_to_send)


def send_dataframe_by_email(df, receivers, subject, text, plot_to_send=None, email_in_ru=False):
    """

    :param df: a pd.DataFrame, to be formatted as a table and sent in the email
    :param receivers: a list of string, containing the email addresses to send to
    :param subject: a string, the subject of the email
    :param text: a string, the content of the email, can be html formatted
    :param plot_to_send: a plot object, a plot to attach to the email
    :return:
    """
    # for testing purpose
    # df = pd.read_csv("C:/dev/data/test_orthanc.csv")
    if len(df) > 0:
        if 'Unnamed: 0' in df.columns:
            df = df.drop('Unnamed: 0', axis=1)
        if 'Id' in df.columns:
            df = df.drop('Id', axis=1)
    if email_in_ru:
        text, df, subject = translate_email_in_ru(text, df, subject)
        receivers = receivers.append("shatrova.lyudmila@gmail.com")
    html = ""
    html += text
    if len(df) > 0:
        html += build_table(df, 'blue_light')
    html = html.replace('Retired', '<p style="color:red">Retired</p>')
    html = html.replace('Open', '<p style="color:green">Open</p>')
    send_email_from_ops(receivers, html, subject, content_format='html',
                        plot_to_send=plot_to_send)


def translate_email_in_ru(text, df, subject):
    # Just for jazz for Lyudmila
    df = df.loc[df['Surface'] >= 42]
    df = df.loc[df['Surface'] <= 55]

    translation_dict = {'Id': 'Идентификатор',
                        'Entrance': 'Подъезд',
                        'Number Of Floors': 'Количество этажей',
                        'Floor': 'Этаж',
                        'Surface': 'Площадь',
                        'Price': 'Цена',
                        'Link': 'Ссылка',
                        'JK': 'ЖК',
                        'Number of rooms': 'комн.',
                        'City': 'Город'}

    # rename the columns using the translation dictionary
    df = df.rename(columns=translation_dict)

    for en_word, ru_word in translation_dict.items():
        text = text.replace(en_word, ru_word)

    return text, df, subject


def build_platform_jk_file_name(platform, jk_name):
    return platform.lower() + '_' + jk_name.lower()


def get_email_text(platform, city, jk_name, number_of_rooms):
    text = 'Weekly ' + platform + ' summary for :<br>' + \
           '    - City: ' + city.title() + '<br>' + \
           '    - JK: ' + jk_name.title() + '<br>' + \
           '    - Number of rooms: ' + str(number_of_rooms) + '<br>'
    return text


def get_email_object(platform, city, jk_name, environment='PROD'):
    obj = f'Weekly {platform} Summary - {city.title()}  - {jk_name.title()}'
    if environment == 'DEV':
        obj = 'DEV - ' + obj
    return obj
