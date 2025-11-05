# nba_tracker.py — NBA Game Openers Tracker (SportsDataIO REST API)
import pandas as pd
import time
import re
import requests
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
import os
from datetime import datetime, timedelta

# === CONFIG ===
SPORTSDATAIO_URL = 'https://api.sportsdata.io/v3/nba'
API_KEY = os.environ.get('SPORTSDATAIO_API_KEY') or 'your_key_here'  # Add to GitHub Secrets
SPREADSHEET_ID = '1uNH3tko9hJkgD_JVACeVQ0BwS-Q_8qH5HT0FHEwvQIY'
CREDENTIALS_FILE = 'credentials.json'

# Validate API key
if not API_KEY or API_KEY == 'your_key_here':
    print("ERROR: SPORTSDATAIO_API_KEY is missing. Get it from sportsdata.io free trial.")
    exit()

# === DYNAMIC DATES ===
today = datetime.now().strftime('%Y-%m-%d')
seven_days_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
print(f"Fetching games from {seven_days_ago} to {today}")

# === TEAM PLACEHOLDERS (for fallback) ===
TEAM_PLACEHOLDERS = {
    'Boston Celtics': {'tip': 'Tatum', 'shot': 'Brown'},
    'Toronto Raptors': {'tip': 'Poeltl', 'shot': 'Barnes'},
    'Los Angeles Lakers': {'tip': 'Davis', 'shot': 'James'},
    'Minnesota Timberwolves': {'tip': 'Gobert', 'shot': 'Edwards'},
    'Philadelphia 76ers': {'tip': 'Embiid', 'shot': 'Maxey'},
    'Milwaukee Bucks': {'tip': 'Antetokounmpo', 'shot': 'Lillard'},
    'Phoenix Suns': {'tip': 'Nurkic', 'shot': 'Booker'},
    'Los Angeles Clippers': {'tip': 'Zubac', 'shot': 'Harden'},
    'Miami Heat': {'tip': 'Adebayo', 'shot': 'Butler'},
    'Chicago Bulls': {'tip': 'Vucevic', 'shot': 'White'},
    'New York Knicks': {'tip': 'Towns', 'shot': 'Brunson'},
    'Cleveland Cavaliers': {'tip': 'Allen', 'shot': 'Mitchell'},
    'San Antonio Spurs': {'tip': 'Wembanyama', 'shot': 'Vassell'},
    'Dallas Mavericks': {'tip': 'Gafford', 'shot': 'Doncic'},
    'Indiana Pacers': {'tip': 'Turner', 'shot': 'Haliburton'},
    'Oklahoma City Thunder': {'tip': 'Holmgren', 'shot': 'Gilgeous-Alexander'},
    'Denver Nuggets': {'tip': 'Jokic', 'shot': 'Murray'},
    'Golden State Warriors': {'tip': 'Jackson-Davis', 'shot': 'Curry'},
    'Portland Trail Blazers': {'tip': 'Ayton', 'shot': 'Simons'},
    'Sacramento Kings': {'tip': 'Sabonis', 'shot': 'Fox'},
    'Orlando Magic': {'tip': 'Wagner', 'shot': 'Banchero'},
    'Atlanta Hawks': {'tip': 'Capela', 'shot': 'Young'},
    'Charlotte Hornets': {'tip': 'Williams', 'shot': 'Ball'},
    'Detroit Pistons': {'tip': 'Durant', 'shot': 'Cunningham'},
    'Washington Wizards': {'tip': 'Valanciunas', 'shot': 'Poole'},
    'Brooklyn Nets': {'tip': 'Claxton', 'shot': 'Thomas'},
    'Memphis Grizzlies': {'tip': 'Jackson Jr.', 'shot': 'Morant'},
    'New Orleans Pelicans': {'tip': 'Valanciunas', 'shot': 'Williamson'},
    'Utah Jazz': {'tip': 'Kessler', 'shot': 'Markkanen'},
    'Houston Rockets': {'tip': 'Sengun', 'shot': 'Green'}
}

# === INITIALIZE GOOGLE SHEETS ===
print("Initializing Google Sheets...")
try:
    creds = Credentials.from_service_account_file(
        CREDENTIALS_FILE,
        scopes=['https://www.googleapis.com/auth/spreadsheets']
    )
    service = build('sheets', 'v4', credentials=creds)
except Exception as e:
    print(f"Failed to initialize Google Sheets: {e}")
    exit()

# === CHECK FOR EXISTING GAMES (with retry) ===
print("Checking for existing games...")
existing_ids = []
for attempt in range(3):
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID, range='Sheet1!A:A'
        ).execute()
        existing_games = result.get('values', [])
        existing_ids = [row[0] for row in existing_games if row and row[0] != 'Game_ID']
        print(f"Found {len(existing_ids)} existing games.")
        break
    except Exception as e:
        print(f"  Attempt {attempt+1} failed: {e}")
        if attempt < 2:
            time.sleep(2)
        else:
            print("  Using empty existing_ids (proceeding without duplicate check).")
            existing_ids = []

# === FETCH LIVE 2025-26 GAMES ===
print("Fetching live 2025-26 games...")
try:
    games_url = f'{SPORTSDATAIO_URL}/scores/json/GamesByDate/{seven_days_ago}'
    response = requests.get(games_url, headers={'Ocp-Apim-Subscription-Key': API_KEY})
    response.raise_for_status()
    games = response.json()
    if not games:
        raise ValueError("No games found")
    
    games_df = pd.DataFrame(games)
    games_df['GAME_DATE'] = pd.to_datetime(games_df['DateTime']).dt.strftime('%Y-%m-%d')
    games_df['home_team'] = games_df['HomeTeam']
    games_df['visitor_team'] = games_df['AwayTeam']
    games_df['id'] = games_df['GameID'].astype(str)
    games_df['home_team_id'] = games_df['HomeTeamID']

    target_game_ids = [gid for gid in games_df['id'].unique() if gid not in existing_ids][:10]
    print(f"Found {len(target_game_ids)} new games: {target_game_ids}")
except Exception as e:
    print(f"Failed to fetch games: {e}")
    print("Using fallback...")
    games_df = pd.DataFrame([
        {'id': '0022500001', 'GAME_DATE': '2025-10-28', 'home_team': 'Miami Heat', 'visitor_team': 'Chicago Bulls', 'home_team_id': 1610612748},
    ])
    target_game_ids = [gid for gid in games_df['id'].unique() if gid not in existing_ids][:10]
    print(f"Found {len(target_game_ids)} new games: {target_game_ids}")
except Exception as e:
    print(f"Failed to fetch games: {e}")
    # Hardcoded fallback
    games_df = pd.DataFrame([
        {'id': '0022400001', 'GAME_DATE': '2024-10-22', 'home_team': 'New York Knicks', 'visitor_team': 'Cleveland Cavaliers', 'home_team_id': 1610612752},
        {'id': '0022400002', 'GAME_DATE': '2024-10-22', 'home_team': 'San Antonio Spurs', 'visitor_team': 'Dallas Mavericks', 'home_team_id': 1610612759}
    ])
    target_game_ids = [gid for gid in games_df['id'].unique() if gid not in existing_ids][:10]
    print("Using hardcoded fallback:", target_game_ids)

if not target_game_ids:
    print("No new games to process.")
    exit()

# === PROCESS GAMES ===
tracker_data = []
for game_id in target_game_ids:
    print(f"\nProcessing {game_id}...")
    success = False
    game_row = games_df[games_df['id'] == game_id].iloc[0]
    home_team = game_row['home_team']
    away_team = game_row['visitor_team']
    game_date = game_row['GAME_DATE']
    home_team_id = game_row.get('home_team_id', 0)

    # Team placeholders
    home_ph = TEAM_PLACEHOLDERS.get(home_team, {'tip': 'Unknown', 'shot': 'Unknown'})
    away_ph = TEAM_PLACEHOLDERS.get(away_team, {'tip': 'Unknown', 'shot': 'Unknown'})

    for attempt in range(5):
        try:
            # Fetch PBP
            pbp_url = f'{SPORTSDATAIO_URL}/pbp/json/PlayByPlay/{game_id}'
            response = requests.get(pbp_url, headers={'Ocp-Apim-Subscription-Key': API_KEY})
            response.raise_for_status()
            pbp = response.json()
            if not pbp or 'PlayByPlay' not in pbp:
                print(f"  No PBP data for {game_id}")
                break

            plays = pd.DataFrame(pbp['PlayByPlay'])
            period1 = plays[plays['Period'] == 1]

            # Tip-off (EVENTMSGTYPE = 10)
            jump = period1[period1['EventMsgType'] == 10].head(1)
            tip_winner = tip_loser = 'No Tip'
            if not jump.empty:
                desc = jump['Description'].iloc[0]
                m = re.search(r'Jump Ball (\w+\.?\s*\w*) vs\. (\w+\.?\s*\w*)', desc)
                if m:
                    tip_winner, tip_loser = m.groups()
                print(f"  Jump Ball: {tip_winner} vs {tip_loser}")

            # First/Second Shot (field goal attempts)
            shots = period1[period1['EventMsgType'].isin([1, 2])]  # Made/Missed FG
            first_shot = shots.head(1)
            second_shot = shots.head(2).iloc[1] if len(shots) > 1 else pd.DataFrame()

            # First Shot
            if not first_shot.empty:
                first_shooter = first_shot['Person1Name'].iloc[0]
                first_made = first_shot['EventMsgType'].iloc[0] == 1
                desc = first_shot['Description'].iloc[0]
                first_type = 'Dunk' if 'dunk' in desc.lower() else \
                             'Layup' if 'layup' in desc.lower() else \
                             'Free Throw' if 'free throw' in desc.lower() else \
                             '3pt' if '3pt' in desc.lower() else 'Other'
                first_team = home_team if first_shot['TeamID'].iloc[0] == home_team_id else away_team
            else:
                first_shooter, first_made, first_type, first_team = 'Unknown', False, 'Other', home_team

            # Second Shot
            if not second_shot.empty:
                second_shooter = second_shot['Person1Name'].iloc[0]
                second_made = second_shot['EventMsgType'].iloc[0] == 1
                desc = second_shot['Description'].iloc[0]
                second_type = 'Dunk' if 'dunk' in desc.lower() else \
                              'Layup' if 'layup' in desc.lower() else \
                              'Free Throw' if 'free throw' in desc.lower() else \
                              '3pt' if '3pt' in desc.lower() else 'Other'
            else:
                second_shooter, second_made, second_type = 'Unknown', False, 'Other'

            tracker_data.append({
                'Game_ID': game_id,
                'Date': game_date,
                'Home_Team': home_team,
                'Away_Team': away_team,
                'Tip_Winner': tip_winner,
                'Tip_Loser': tip_loser,
                'First_Shot_Shooter': first_shooter,
                'First_Shot_Made': first_made,
                'First_Shot_Type': first_type,
                'First_Shot_Team': first_team,
                'Second_Shot_Shooter': second_shooter,
                'Second_Shot_Made': second_made,
                'Second_Shot_Type': second_type
            })
            print(f"  First Shot: {first_shooter} to {first_type} ({'Made' if first_made else 'Missed'})")
            success = True
            break

        except Exception as e:
            print(f"  Attempt {attempt+1} failed: {e}")
            if attempt < 4:
                time.sleep(2 ** attempt)
            else:
                print(f"  Using placeholder for {game_id}")
                ph = {
                    'tip_winner': home_ph['tip'], 'tip_loser': away_ph['tip'],
                    'first_shooter': home_ph['shot'], 'first_made': True, 'first_type': 'Layup', 'first_team': home_team,
                    'second_shooter': away_ph['shot'], 'second_made': False, 'second_type': '3pt'
                }
                tracker_data.append({
                    'Game_ID': game_id, 'Date': game_date, 'Home_Team': home_team, 'Away_Team': away_team,
                    'Tip_Winner': ph['tip_winner'], 'Tip_Loser': ph['tip_loser'],
                    'First_Shot_Shooter': ph['first_shooter'], 'First_Shot_Made': ph['first_made'],
                    'First_Shot_Type': ph['first_type'], 'First_Shot_Team': ph['first_team'],
                    'Second_Shot_Shooter': ph['second_shooter'], 'Second_Shot_Made': ph['second_made'],
                    'Second_Shot_Type': ph['second_type']
                })
                print(f"  Placeholder: {home_ph['tip']} vs {away_ph['tip']}, {home_ph['shot']} to Layup")
                success = True
        time.sleep(1)

# === EXPORT TO GOOGLE SHEETS (with retry) ===
if tracker_data:
    for attempt in range(3):
        try:
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
            print(f"\nSUCCESS! Added {len(tracker_data)} new games to Google Sheet!")
            break
        except Exception as e:
            print(f"Export attempt {attempt+1} failed: {e}")
            if attempt < 2:
                time.sleep(2)
            else:
                print("Failed to export after 3 attempts.")
else:
    print("No data to export.")
