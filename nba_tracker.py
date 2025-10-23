# nba_tracker.py
import pandas as pd
import time
import re
from nba_api.stats.endpoints import leaguegamefinder, playbyplay
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials

# === CONFIG ===
SPREADSHEET_ID = '1uNH3tko9hJkgD_JVACeVQ0BwS-Q_8qH5HT0FHEwvQIY'
CREDENTIALS_FILE = 'credentials.json'

# === FETCH GAMES (LAST 24 HOURS) ===
print("Fetching games from last 24 hours...")
gamefinder = leaguegamefinder.LeagueGameFinder(
    season_nullable='2024-25',
    season_type_nullable='Regular Season'
)
games = gamefinder.get_data_frames()[0]
games['GAME_DATE'] = pd.to_datetime(games['GAME_DATE']).dt.strftime('%Y-%m-%d')
yesterday = (pd.Timestamp.now() - pd.Timedelta(days=1)).strftime('%Y-%m-%d')
target_game_ids = games[games['GAME_DATE'] >= yesterday]['GAME_ID'].unique()

if len(target_game_ids) == 0:
    print("No new games today.")
    exit()

# === PROCESS GAMES ===
tracker_data = []
for game_id in target_game_ids:
    try:
        pbp = playbyplay.PlayByPlay(game_id).get_data_frames()[0]
        period1 = pbp[pbp['PERIOD'] == 1]
        if len(period1) == 0: continue

        # Tip
        jump = period1[period1['EVENTMSGTYPE'] == 10].head(1)
        tip_winner = tip_loser = 'No Tip'
        if not jump.empty:
            desc = jump.iloc[0].get('HOMEDESCRIPTION', '') or jump.iloc[0].get('VISITORDESCRIPTION', '')
            m = re.search(r'Jump Ball (\w+\.?\s*\w*) vs\. (\w+\.?\s*\w*)', desc)
            if m: tip_winner, tip_loser = m.groups()

        # First Shot
        fg = period1[period1['EVENTMSGTYPE'].isin([1,3])]
        first_shooter = first_type = 'No Shot'
        first_made = False
        if len(fg) > 0:
            desc = fg.iloc[0].get('HOMEDESCRIPTION', '') or fg.iloc[0].get('VISITORDESCRIPTION', '')
            m = re.search(r'^(\w+\.?\s*\w*?)(?=\s)', desc)
            first_shooter = m.group(1).strip() if m else 'Unknown'
            first_made = fg.iloc[0]['EVENTMSGTYPE'] == 1
            typ = 'Dunk' if 'dunk' in desc.lower() else 'Layup' if 'layup' in desc.lower() else '3pt' if '3pt' in desc.lower() else 'Other'
            first_type = typ

        tracker_data.append({
            'Game_ID': game_id,
            'Date': games[games['GAME_ID']==game_id]['GAME_DATE'].iloc[0],
            'Tip_Winner': tip_winner,
            'First_Shot_Shooter': first_shooter,
            'First_Shot_Made': first_made,
            'First_Shot_Type': first_type
        })
        print(f"Processed {game_id}")
    except: pass
    time.sleep(1.5)

# === EXPORT TO GOOGLE SHEETS ===
if tracker_data:
    creds = Credentials.from_service_account_file(CREDENTIALS_FILE,
                                                  scopes=['https://www.googleapis.com/auth/spreadsheets'])
    service = build('sheets', 'v4', credentials=creds)
    df = pd.DataFrame(tracker_data)
    values = [df.columns.tolist()] + df.values.tolist()
    body = {'values': values}
    service.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range='Sheet1!A1',
        valueInputOption='RAW',
        insertDataOption='INSERT_ROWS',
        body=body
    ).execute()
    print(f"Added {len(tracker_data)} new games!")
else:
    print("No new data.")
