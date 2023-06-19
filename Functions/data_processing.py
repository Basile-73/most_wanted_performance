import pytz
import datetime
import pandas as pd
from Functions.email_manager import retrieve_attachments, move_messages_to_used_folder
from datetime import timedelta, datetime, timezone
from Functions.locations import Locations
import psycopg2
from psycopg2 import sql
import streamlit
from sqlalchemy import create_engine
from fastapi import FastAPI
from typing import Union

app = FastAPI()

def convert_to_datetime(date_string):
    # Converts a string of type '%Y-%m-%dT%H:%M:%S%z' to pd.datetime type
    try:
        dt = datetime.strptime(date_string, '%Y-%m-%dT%H:%M:%S%z')
    except ValueError:
        dt = datetime.strptime(date_string[:-3], '%Y-%m-%dT%H:%M:%S')
        dt = pytz.timezone('America/Denver').localize(dt) # Assuming 'MDT' refers to Mountain Daylight Time
    return dt

def get_clean_df(df_dirty):
    # Removes empty rows at the top & renames columns
    df_dirty.columns = df_dirty.iloc[3]
    df_cut = df_dirty[4:]
    transactions = df_cut[['Asset Id', 'Asset Description', 'Asset SKU', 'User Name', 'Action Type', 'Device Name', 'Action Date']].copy()
    transactions['Action Date'] = transactions['Action Date'].apply(convert_to_datetime).astype(str)
    #transactions['Action Date'] = transactions['Action Date'].apply(convert_to_datetime).apply(lambda dt: dt.astimezone(timezone.utc))
    #transactions['Action Date'] = transactions['Action Date'].astype(str)
    return transactions

def save_to_db(df, table_name):
    engine = create_engine(f'postgresql://{streamlit.secrets["postgres"]["user"]}:{streamlit.secrets["postgres"]["password"]}@{streamlit.secrets["postgres"]["host"]}:{streamlit.secrets["postgres"]["port"]}/{streamlit.secrets["postgres"]["dbname"]}')

    df.reset_index(drop=True, inplace=True)
    if 'index' in df.columns:
        df = df.drop('index', axis=1)
    if 'level_0' in df.columns:
        df = df.drop('level_0', axis=1)

    df.to_sql(table_name, engine, if_exists='replace', index=False)

import os

class AssetManager:
# Creating an Instance of this class and applying get_all will Update the list of overdue tools. The List is available within the Class.
    def __init__(self, lending_period, location):
        self.Transactions_records = []
        self.Lending_period = lending_period
        self.Location = location
        self.Tool_status = None
        self.Overdue = None
        self.table_name = 'tool_status'

    def update_overdue(self):
    # Computes the list of Overdue Tools and stores it in self.Overdue
        self.load_transactions()
        self.update_tool_status()
        self.get_overdue()
        move_messages_to_used_folder()
        save_to_db(self.Tool_status, 'tool_status')


    def load_transactions(self):
        # db connection
        connection = psycopg2.connect(
        host=streamlit.secrets["postgres"]["host"],
        port=streamlit.secrets["postgres"]["port"],
        dbname=streamlit.secrets["postgres"]["dbname"],
        user=streamlit.secrets["postgres"]["user"],
        password=streamlit.secrets["postgres"]["password"]
    )

        cursor = connection.cursor()
        cursor.execute("SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name=%s)", ('tool_status',))
        # Consider 2 Sources
        # 1: Database with inital transactions or tool_status
        if cursor.fetchone()[0]:
            # Table exists: Load data from PostgreSQL table
            select_query = sql.SQL("SELECT * FROM {}").format(sql.Identifier(self.table_name))
            cursor.execute(select_query)

            initial_transactions = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
            self.Transactions_records.append(initial_transactions)
        else:
            print("Could not reach database")
        # 2: Excel files from Attachment
        attachments = retrieve_attachments()
        for attachment in attachments:
            self.Transactions_records.append(get_clean_df(attachment))

    def update_tool_status(self):
        # concatenate all dataframes in the list into one
        combined_df = pd.concat(self.Transactions_records)

        # ensure 'Action Date' column is datetime
        combined_df['Action Date'] = pd.to_datetime(combined_df['Action Date'])

        # sort by 'Action Date' then drop duplicates, keeping only the last (most recent)
        combined_df = combined_df.sort_values('Action Date').drop_duplicates('Asset Id', keep='last')
        combined_df['Action Date'] = combined_df['Action Date'].astype(str)
        self.Tool_status = combined_df

    def get_overdue(self):
    # Filters and displays overdue Tools from the Tool_Status List
        self.Overdue = self.Tool_status.copy()
        self.Overdue = self.Overdue[self.Overdue['Action Type']=="CHECK_OUT"]
        self.Overdue = self.Overdue[self.Overdue['Device Name'].isin(Locations[self.Location])]
        self.Overdue['Action Date'] = self.Overdue['Action Date'].apply(lambda x: pd.to_datetime(x))
        now = datetime.now(pytz.utc)
        self.Overdue.loc[:, 'Out since (h)'] = self.Overdue['Action Date'].apply(lambda x: (now - x).total_seconds() / 3600).round().astype(int)
        #self.Overdue.loc[:, 'Out since (h)'] = self.Overdue['Action Date'].apply(lambda x: int((now - x) / timedelta(hours=1)))
        self.Overdue = self.Overdue[self.Overdue['Out since (h)'] > self.Lending_period]
        self.Overdue = self.Overdue[['Asset Id', 'Asset Description', 'Asset SKU', 'User Name', 'Out since (h)']]
        self.Overdue = self.Overdue.rename(columns={'Asset Id': 'Location', 'Asset SKU': 'Asset'})
        # drop column Asset
        self.Overdue = self.Overdue.drop('Asset', axis=1)
        self.Overdue = self.Overdue.sort_values(by='Out since (h)', ascending=False)

def get_update(hours, selected_location):
    update = AssetManager(hours, selected_location)
    update.update_overdue()
    return update.Overdue
