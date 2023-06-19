import pytz
import datetime
import pandas as pd
from datetime import timedelta, datetime, timezone
from Functions.locations import Locations
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
    transactions = df_dirty[['Asset Id', 'Asset Description', 'Asset SKU', 'User Name', 'Action Type', 'Device Name', 'Action Date']].copy()
    transactions['Action Date'] = transactions['Action Date'].apply(convert_to_datetime).astype(str)
    #transactions['Action Date'] = transactions['Action Date'].apply(convert_to_datetime).apply(lambda dt: dt.astimezone(timezone.utc))
    #transactions['Action Date'] = transactions['Action Date'].astype(str)
    return transactions

import os

class AssetManager:
# Creating an Instance of this class and applying get_all will Update the list of overdue tools. The List is available within the Class.
    def __init__(self, lending_period, location):
        self.Transactions_records = []
        self.Lending_period = lending_period
        self.Location = location
        self.Tool_status = None
        self.Overdue = None

    def update_tool_status(self):
        # concatenate all dataframes in the list into one
        combined_df = pd.concat(self.Transactions_records)

        # ensure 'Action Date' column is datetime
        combined_df['Action Date'] = pd.to_datetime(combined_df['Action Date'])

        # sort by 'Action Date' then drop duplicates, keeping only the last (most recent)
        combined_df = combined_df.sort_values('Action Date').drop_duplicates('Asset Id', keep='last')
        combined_df['Action Date'] = combined_df['Action Date'].astype(str)
        self.Tool_status = combined_df

    def get_overdue(self, now):
    # Filters and displays overdue Tools from the Tool_Status List
        self.Overdue = self.Tool_status.copy()
        self.Overdue = self.Overdue[self.Overdue['Action Type']=="CHECK_OUT"]
        self.Overdue = self.Overdue[self.Overdue['Device Name'].isin(Locations[self.Location])]
        self.Overdue['Action Date'] = self.Overdue['Action Date'].apply(lambda x: pd.to_datetime(x))
        # now comes in as a date: e.g. 14.07.2021. Add a time (6 p.m.) to it: e.g. 14.07.2021 18:00:00
        now = datetime.strptime(now, '%d-%m-%Y')
        now = now.replace(hour=18, minute=0, second=0)
        now = now.replace(tzinfo=pytz.UTC)  # This makes it UTC timezone aware
        # Convert to Salt Lake City time ('America/Denver' in pytz)
        now = now.astimezone(pytz.timezone('America/Denver'))
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
