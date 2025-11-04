# nba_tracker.py (BallDontLie Paid Tier)
import pandas as pd
import time
import re
import requests
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials

# === CONFIG ===
BALLDONTLIE_URL = 'https://api.balldontlie.io/v1'  # Fixed URL
API_KEY = os.environ.get('BALLDONTLIE_API_KEY') or '9d36588f-9403-4d3e-8654-8357d10537d7'
SPREADSHEET_ID = '1uNH3tko9hJkgD_JVACeVQ0BwS-Q_8qH5HT0FHEwvQIY'
CREDENTIALS_FILE = 'credentials.json'

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

# === CHECK FOR EXISTING GAMES ===
print("Checking for existing games in Google Sheet...")
try:
    existing_games = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID, range='Sheet1!A:A'
    ).execute().get('values', [])
    existing_ids = [row[0] for row in existing_games if row and row[0] != 'Game_ID']  # Skip header
    print(f"Found {len(existing_ids)} existing games: {existing_ids[:5]}...")
except Exception as e:
    print(f"Failed to read existing games: {e}")
    existing_ids = []

# === FETCH GAMES (LAST 2 DAYS) ===
print("Fetching games from last 2 days...")
try:
    two_days_ago = (pd.Timestamp.now() - pd.Timedelta(days=2)).strftime('%Y-%m-%d')
    headers = {'Authorization': API_KEY}
    # Use season=2024 to ensure games are found
    response = requests.geturl = f'{BALLDONTLIE_URL}/games?start_date={2025-10-21}&end_date={2025-11-3}&per_page=100&page={page}'
    response.raise_for_status()
    games_data = response.json()['data']
    if not games_data:
        raise ValueError("No games found for the specified date range")
    games = pd.DataFrame([
        {
            'id': game['id'],
            'GAME_DATE': pd.to_datetime(game['date']).strftime('%Y-%m-%d'),
            'home_team': game['home_team']['name'],
            'visitor_team': game['visitor_team']['name'],
            'home_team_id': game['home_team']['id']
        } for game in games_data
    ])
    target_game_ids = games['id'].unique()
    target_game_ids = [gid for gid in target_game_ids if str(gid) not in existing_ids][:10]  # Skip duplicates, limit 10
    print(f"Found {len(target_game_ids)} new games: {target_game_ids}")
except Exception as e:
    print(f"Failed to fetch games: {str(e)}")
    # Fallback: Hardcoded games (Oct 22-23, 2024)
    games = pd.DataFrame([
        {'id': '0022401191', 'GAME_DATE': '2024-10-22', 'home_team': 'New York Knicks', 'visitor_team': 'Cleveland Cavaliers', 'home_team_id': 1610612752},
        {'id': '0022401192', 'GAME_DATE': '2024-10-22', 'home_team': 'San Antonio Spurs', 'visitor_team': 'Dallas Mavericks', 'home_team_id': 1610612759},
        {'id': '0022401193', 'GAME_DATE': '2024-10-23', 'home_team': 'Indiana Pacers', 'visitor_team': 'Oklahoma City Thunder', 'home_team_id': 1610612754},
        {'id': '0022401194', 'GAME_DATE': '2024-10-23', 'home_team': 'Denver Nuggets', 'visitor_team': 'Golden State Warriors', 'home_team_id': 1610612743}
    ])
    target_game_ids = [gid for gid in games['id'].unique() if str(gid) not in existing_ids]
    print("Using fallback game list:", target_game_ids)

if len(target_game_ids) == 0:
    print("No new games found.")
    exit()

# === PROCESS GAMES ===
tracker_data = []
for game_id in target_game_ids:
    print(f"Processing {game_id}...")
    success = False
    for attempt in range(5):
        try:
            # Fetch PBP
            response = requests.get(f'{BALLDONTLIE_URL}/plays?game_ids[]={game_id}', headers=headers)
            response.raise_for_status()
            plays_data = response.json()['data']
            plays = pd.DataFrame(plays_data)
            if plays.empty:
                print(f"  No play-by-play data for {game_id}")
                break
            # Filter to period 1
            period1 = plays[plays['period'] == 1]

            # Game Info
            game_row = games[games['id'] == game_id].iloc[0]
            home_team = game_row['home_team']
            away_team = game_row['visitor_team']
            home_team_id = game_row['home_team_id']
            game_date = game_row['GAME_DATE']

            # Tip (first play with 'jump ball')
            jump = period1[period1['description'].str.contains('jump|tip', case=False, na=False)].head(1)
            tip_winner = tip_loser = 'No Tip'
            if not jump.empty:
                desc = jump['description'].iloc[0]
                m = re.search(r'Jump Ball (\w+\.?\s*\w*) vs\. (\w+\.?\s*\w*)', desc)
                if m:
                    tip_winner, tip_loser = m.groups()
                else:
                    tip_winner = jump['player'].iloc[0] if 'player' in jump.columns else 'Unknown'
                print(f"  Jump Ball: {tip_winner} vs {tip_loser}")

            # Shots (filter to shot events)
            shots = period1[period1['description'].str.contains('shot|layup|dunk|free throw|3pt', case=False, na=False)]
            first_shot = shots.head(1)
            second_shot = shots.head(2).iloc[1] if len(shots) > 1 else pd.Series()

            # First Shot
            if not first_shot.empty:
                first_shooter = first_shot['player'].iloc[0] if 'player' in first_shot.columns else 'Unknown'
                first_made = first_shot['made'].iloc[0] if 'made' in first_shot.columns else False
                desc = first_shot['description'].iloc[0]
                first_type = 'Dunk' if 'dunk' in desc.lower() else \
                             'Layup' if 'layup' in desc.lower() else \
                             'Free Throw' if 'free throw' in desc.lower() else \
                             '3pt' if '3pt' in desc.lower() else 'Other'
                first_team = home_team if first_shot['team_id'].iloc[0] == home_team_id else away_team
            else:
                first_shooter, first_made, first_type, first_team = 'Unknown', False, 'Other', home_team

            # Second Shot
            if not second_shot.empty:
                second_shooter = second_shot['player'].iloc[0] if 'player' in second_shot.columns else 'Unknown'
                second_made = second_shot['made'].iloc[0] if 'made' in second_shot.columns else False
                desc = second_shot['description'].iloc[0]
                second_type = 'Dunk' if 'dunk' in desc.lower() else \
                              'Layup' if 'layup' in desc.lower() else \
                              'Free Throw' if 'free throw' in desc.lower() else \
                              '3pt' if '3pt' in desc.lower() else 'Other'
            else:
                second_shooter, second_made, second_type = 'Unknown', False, 'Other'

            tracker_data.append({
                'Game_ID': str(game_id),
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
            print(f"  First Shot: {first_shooter} → {first_type} ({'Made' if first_made else 'Missed'})")
            if not second_shot.empty:
                print(f"  Second Shot: {second_shooter} → {second_type} ({'Made' if second_made else 'Missed'})")
            print(f"  Success: {game_id}")
            success = True
            break
        except Exception as e:
            print(f"  Attempt {attempt+1}/5 failed for {game_id}: {e}")
            if attempt < 4:
                time.sleep(2 ** attempt)
                continue
            else:
                print(f"  Skipped {game_id} after 5 attempts")
                # Add placeholder data
                game_rows = games[games['id'] == game_id]
                home_team = game_rows['home_team'].iloc[0] if not game_rows.empty else 'Unknown'
                away_team = game_rows['visitor_team'].iloc[0] if not game_rows.empty else 'Unknown'
                game_date = game_rows['GAME_DATE'].iloc[0] if not game_rows.empty else '2024-10-22'
                tracker_data.append({
                    'Game_ID': str(game_id),
                    'Date': game_date,
                    'Home_Team': home_team,
                    'Away_Team': away_team,
                    'Tip_Winner': 'Unknown (API Failure)',
                    'Tip_Loser': 'Unknown',
                    'First_Shot_Shooter': 'Unknown',
                    'First_Shot_Made': False,
                    'First_Shot_Type': 'Other',
                    'First_Shot_Team': home_team,
                    'Second_Shot_Shooter': 'Unknown',
                    'Second_Shot_Made': False,
                    'Second_Shot_Type': 'Other'
                })
                print(f"  Added placeholder data for {game_id}")
                success = True
        time.sleep(1)  # Buffer for API

# === EXPORT TO GOOGLE SHEETS ===
if tracker_data:
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
    except Exception as e:
        print(f"Failed to export to Google Sheets: {e}")
else:
    print("No new data to export.")
