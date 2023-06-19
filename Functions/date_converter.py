import pandas as pd

# Define your mapping of timezone abbreviations to UTC offsets
timezone_mapping = {
    'EST': '-05:00',  # Eastern Standard Time
    'EDT': '-04:00',  # Eastern Daylight Time
    'CST': '-06:00',  # Central Standard Time
    'CDT': '-05:00',  # Central Daylight Time
    'MST': '-07:00',  # Mountain Standard Time
    'MDT': '-06:00',  # Mountain Daylight Time
    'PST': '-08:00',  # Pacific Standard Time
    'PDT': '-07:00'   # Pacific Daylight Time
    # add more as needed
}

def convert_to_datetime(timestr):
    # Identify the timezone from the string
    tz = timestr[-3:]

    if tz in timezone_mapping:
        # Replace timezone abbreviation with UTC offset
        timestr = timestr.replace(tz, timezone_mapping[tz])
    else:
        print('Timezone ' + tz + ' not found in mapping.')
    return pd.to_datetime(timestr)
