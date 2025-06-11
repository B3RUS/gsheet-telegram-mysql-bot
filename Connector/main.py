import csv
from dotenv import load_dotenv
import os
import gspread
from google.oauth2.service_account import Credentials
import time
import mysql.connector
import asyncio
from telegram import Bot
from randfacts import get_fact
import pandas as pd

class MySQLDownloader:
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database

    def download_to_csv(self, query_path, output_file):
        try:
            with open(query_path, "r", encoding="utf-8") as file:
                query = file.read()
            connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database
            )
            cursor = connection.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            column_names = [i[0] for i in cursor.description]
            with open(output_file, mode='w', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                writer.writerow(column_names)
                writer.writerows(rows)
            print(f"Data successfully downloaded to {output_file}")
        except mysql.connector.Error as err:
            print(f"Error: {err}")
        finally:
            if 'cursor' in locals():
                cursor.close()
            if 'connection' in locals():
                connection.close()

class GoogleSheetManager:
    def __init__(self, spreadsheet_id, creds_json_path):
        self.spreadsheet_id = spreadsheet_id
        self.creds_json_path = creds_json_path
        self.scope = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        self.creds = Credentials.from_service_account_file(self.creds_json_path, scopes=self.scope)
        self.client = gspread.authorize(self.creds)

    def clear_and_append_csv(self, csv_path, sheet_name):
        sheet = self.client.open_by_key(self.spreadsheet_id).worksheet(sheet_name)
        sheet.clear()
        with open(csv_path, newline='', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            rows = list(reader)
            if rows:
                sheet.append_rows(rows, value_input_option='RAW')
        print(f"Data successfully appended to Google Sheets from {csv_path}")

    def append_csv(self, csv_path, sheet_name):
        df = pd.read_csv(csv_path)
        sheet = self.client.open_by_key(self.spreadsheet_id).worksheet(sheet_name)
        # Replace NaN values with empty strings to avoid JSON serialization errors
        df = df.fillna("")
        # Only append data rows (skip header)
        data_rows = df.values.tolist()
        if data_rows:
            sheet.append_rows(data_rows, value_input_option='RAW')
        print(f"Data (without header) successfully appended to Google Sheets from {csv_path}")


    def write_cell(self, sheet_name, cell, value):
        sheet = self.client.open_by_key(self.spreadsheet_id).worksheet(sheet_name)
        sheet.update(range_name=cell, values=[[value]], value_input_option='RAW')


    def get_unique_shops(self, sheet_name, shop_column="shop"):
        sheet = self.client.open_by_key(self.spreadsheet_id).worksheet(sheet_name)
        data = sheet.get_all_values()
        if not data or shop_column not in data[0]:
            return []
        col_idx = data[0].index(shop_column)
        shops = [row[col_idx] for row in data[1:] if len(row) > col_idx and row[col_idx]]
        return list(set(shops))

class TelegramNotifier:
    def __init__(self, bot_token, chat_id):
        self.bot_token = bot_token
        self.chat_id = chat_id

    async def send_message(self, message):
        bot = Bot(token=self.bot_token)
        await bot.send_message(chat_id=self.chat_id, text=message)

class ShopListManager:
    @staticmethod
    def get_shop_names(filename):
        shop_names = set()
        with open(filename, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                shop_names.add(row['shop'])
        return list(shop_names)


    @staticmethod
    def compare_shop_lists(today_list, yesterday_list):
        new_shops = list(set(today_list) - set(yesterday_list))
        print(f"New shops since yesterday: {new_shops}")
        return new_shops


def clean_files(file_list):
    for file_path in file_list:
        if os.path.exists(file_path):
            os.remove(file_path)
            print(f"{file_path} deleted.")
        else:
            print(f"{file_path} does not exist.")

def main():
    load_dotenv()

    
    clean_files(["kolejki.csv", "nowe_sklepy.csv", "atrybuty.csv", "mote_bez_typu.csv"])

    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    host = os.getenv("DB_HOST")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    database = os.getenv("DB_NAME")
    spreadsheet_id = os.getenv("SPREADSHEET_ID")
    creds_json_path = 'Connector/google-sheets-bot.json'

    mysql_downloader = MySQLDownloader(host, user, password, database)
    sheet_manager = GoogleSheetManager(spreadsheet_id, creds_json_path)
    notifier = TelegramNotifier(bot_token, chat_id)

    # Kolejki
    print("Pobieranie kolejek")
    mysql_downloader.download_to_csv("Connector/sql/kolejki.sql", "kolejki.csv")
    while not os.path.exists("kolejki.csv"):
        print("Waiting for kolejki.csv to be created...")
        time.sleep(1)
    sheet_manager.append_csv("kolejki.csv", "Kolejki")

    # Nowe sklepy
    print("Pobieranie nowych sklepow")

    shop_list_from_yesterday = sheet_manager.get_unique_shops("Nowe sklepy", "shop")
    mysql_downloader.download_to_csv("Connector/sql/nowe_sklepy.sql", "nowe_sklepy.csv")
    while not os.path.exists("nowe_sklepy.csv"):
        print("Waiting for nowe_sklepy.csv to be created...")
        time.sleep(1)
    sheet_manager.clear_and_append_csv("nowe_sklepy.csv", "Nowe sklepy")
    shop_list_from_today = ShopListManager.get_shop_names("nowe_sklepy.csv")

    # Uzupełnienie atrybutów
    print("Pobieranie uzupełnienia atrybutów")
    mysql_downloader.download_to_csv("Connector/sql/uzupelnienie_attr.sql", "atrybuty.csv")
    while not os.path.exists("atrybuty.csv"):
        print("Waiting for atrybuty.csv to be created...")
        time.sleep(1)
    sheet_manager.clear_and_append_csv("atrybuty.csv", "Atrybuty")

    new_shops = ShopListManager.compare_shop_lists(shop_list_from_today, shop_list_from_yesterday)

    # Update timestamp
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    sheet_manager.write_cell("Ogólne", "A1", f"Updated at {timestamp}")
    print("Pobieranie zakonczone")

    spreadsheet_id = os.getenv("MOTE_SPREADSHEET_ID")
    sheet_manager = GoogleSheetManager(spreadsheet_id, creds_json_path)
    # Uzupełnienie mote bez typu
    print("Pobieranie uzupełnienia mote bez typu...")
    mysql_downloader.download_to_csv("Connector/sql/mote_bez_typu.sql", "mote_bez_typu.csv")
    while not os.path.exists("mote_bez_typu.csv"):
        print("Waiting for mote_bez_typu.csv to be created...")
        time.sleep(1)
    sheet_manager.clear_and_append_csv("mote_bez_typu.csv", "RAW")

    # Telegram notification
    info = f"🔄️Dane odświeżone o {timestamp}\n\nNowe sklepy w porównaniu do poprzedniego odświeżenia danych: {new_shops}\n\nRandomowy fakt: {get_fact(filter_enabled=True)}"
    asyncio.run(notifier.send_message(info))

if __name__ == "__main__":
    main()