from __future__ import print_function
from googleapiclient.discovery import build
import requests
import json
from pprint import pprint
import urllib
import time
from datetime import date as dt
from datetime import datetime, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import smtplib
from email.mime.text import MIMEText
from email.header    import Header
import config

filename = "token_status.json"
utc_now_delta = (str(datetime.utcnow() + timedelta(hours = 3)).split(' ')[0]).split('-')
today = dt(int(utc_now_delta[0]), int(utc_now_delta[1]), int(utc_now_delta[2]))
yesterday = today - timedelta(days=1)
print(today)
print(yesterday)
print(str(yesterday))

def start():

    def update_token(refresh_token):
        token_url = 'http://api.hybrid.ru/token'
        client_id = 'XXXXXXXX'
        client_secret = 'XXXX/XXXXX/XXXXXX='

        data = {
            'grant_type': 'refresh_token',
            'refresh_token': refresh_token,
            'client_id': client_id,
            'client_secret': client_secret
        }

        res = requests.post(token_url, data=data)
        return res.json()

    with open(filename, "r") as f:
        token_status = json.load(f)
        token = token_status['access_token']
        token_refresh = token_status['refresh_token']
        token_expires = token_status['expires']

    current_datetime = int(datetime.utcnow().timestamp())

    if token_expires < current_datetime:
        updated_token_response = update_token(token_refresh)
        token = updated_token_response['access_token']
        new_token_will_expire = current_datetime + updated_token_response['expires_in']
        updated_token_response['expires'] = new_token_will_expire

        with open(filename, "w") as f:
            json.dump(updated_token_response, f)

    hed = {'Authorization': 'Bearer ' + token}

    def get_campaigns_stats(campaign_id, dates, camp_type, hedear=hed):
        pasing_value = ''

        camp_type = 'advertiser' if camp_type == 'advert' else 'campaign'
        if dates['end_date'] < yesterday:
            token_adv = f'http://api.hybrid.ru/v2.0/{camp_type}/day?from={dates["start_date_str"]}&to={dates["end_date_str"]}&id={campaign_id}'
        else:
            token_adv = f'http://api.hybrid.ru/v2.0/{camp_type}/day?from={dates["start_date_str"]}&to={str(yesterday)}&id={campaign_id}'
        campaign_stats_by_day = requests.get(token_adv, headers=hed).json()
        return campaign_stats_by_day


    def get_campaign_dates(row):
        start_date_row = row['Дата начала '].split('.')
        end_date_row = row['Дата конца '].split('.')
        start_date_str = f'{start_date_row[2]}-{start_date_row[1]}-{start_date_row[0]}'
        end_date_str = f'{end_date_row[2]}-{end_date_row[1]}-{end_date_row[0]}'

        start_date = dt(int(start_date_row[2]), int(start_date_row[1]), int(start_date_row[0]))
        end_date = dt(int(end_date_row[2]), int(end_date_row[1]), int(end_date_row[0]))
        return {
            'start_date': start_date,
            'end_date': end_date,
            'start_date_row': start_date_row,
            'end_date_row': end_date_row,
            'start_date_str': start_date_str,
            'end_date_str': end_date_str,
        }


    def get_count_left_imps(end_date_row):
        days_left = (end_date_row - today).days
        return days_left


    def get_buying_table():
        credent = ServiceAccountCredentials.from_json_keyfile_name(
            'credentials_google.json',
            ['https://spreadsheets.google.com/feeds'])
        client = gspread.authorize(credent)

        sheet = client.open_by_url('https://docs.google.com/spreadsheets/d/fillItHere')

        worksheet = sheet.get_worksheet(0)
        buying_table = worksheet.get_all_records()
        return buying_table



    statistics = [dict()]


    cou = 0
    for row in get_buying_table():
        if (row['type']) != '':
                campaign_dates = get_campaign_dates(row)
                start_date_row = campaign_dates['start_date_row']
                end_date_row = campaign_dates['end_date_row']
                start_date = campaign_dates['start_date']

                days_left = get_count_left_imps(campaign_dates['end_date'])
                print(f'days_left: {days_left}')
                if (start_date + timedelta(days=3)) < today:
                    print(f'start date: {start_date}, +3 days: {start_date+timedelta(days=3)}')
                    total_imps = 0
                    ids = row['id'].split(',')
                    avg_imps_threeDays = [0, 0, 0]
                    imp_done = 0
                    imp_ordered = row['Заказано кликов / показов']
                    clicks_done = 0

                    for Id in ids:
                        advertiser_stats = get_campaigns_stats(Id, campaign_dates, row['type'])
                        if 'Total' in advertiser_stats:
                            imp_done += advertiser_stats['Total']['ImpressionCount']
                            clicks_done += advertiser_stats['Total']['ClickCount']
                        else:
                            continue

                        stats = advertiser_stats['Statisitic']
                        len_stats = len(stats)

                        if (len_stats > 2):
                            avg_imps_threeDays[2] += stats[len_stats - 4]['ImpressionCount']
                        elif (len_stats > 1):
                            avg_imps_threeDays[1] += stats[len_stats - 3]['ImpressionCount']
                        elif (len_stats > 0):
                            avg_imps_threeDays[0] += stats[len_stats - 2]['ImpressionCount']

                    avg_imp = sum(avg_imps_threeDays) / 3
                    forecast_imp_value = (imp_done + avg_imp * days_left) / imp_ordered

                    statistics.append({'Кампания': row["Название РК"], 'Окончание РК': campaign_dates["end_date_str"], 'Показы': imp_done, 'Клики': clicks_done, 'Прогноз показов': f'{int(round(forecast_imp_value*100))}%'})

    st_df = pd.DataFrame(data = statistics, columns = ['Кампания', 'Окончание РК', 'Показов']).sort_values('Окончание РК').to_html()
    with open('daily_stats.json', 'w', encoding='utf8') as f:
        json.dump(statistics, f, ensure_ascii=False)


    def send_email(msg, today):
        smtp_host = 'smtp.gmail.com'       # google
        recipients_emails = [config.EMAIL_SEND_TO]

        msg = MIMEText(msg, 'html') # 'plain', 'utf-8'
        msg['Subject'] = Header(f'Темпы открутки на {today}', 'utf-8')
        msg['From'] = config.EMAIL_ADDRESS
        msg['To'] = ", ".join(recipients_emails)

        s = smtplib.SMTP(smtp_host, 587, timeout=10)
        s.set_debuglevel(1)
        try:
            s.starttls()
            s.login(config.EMAIL_ADDRESS, config.PASSWORD)
            s.sendmail(msg['From'], recipients_emails, msg.as_string())
        finally:
            s.quit()

    # send_email(st_df, today)
