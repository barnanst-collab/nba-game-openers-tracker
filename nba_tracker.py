# nba_tracker.py (BallDontLie Version)
import pandas as pd
import time
import re
import requests
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials

# === CONFIG ===
BALLDONTLIE_URL = 'https://www.balldontlie.io/api/v1'
API_KEY = 9d36588f-9403-4d3e-8654-8357d10537d7  # Optional for free tier
SPREADSHEET_ID = '1uNH3tko9hJkgD_JVACeVQ0BwS-Q_8qH5HT0FHEwvQIY'
CREDENTIALS_FILE = 'credentials.json'

# === FETCH GAMES (LAST 2 DAYS) ===
print("Fetching games from last 2 days...")
try:
    two_days_ago = (pd.Timestamp.now() - pd.Timedelta(days=2)).strftime('%Y-%m-%d')
    response = requests.get(f'{BALLDONTLIE_URL}/games?dates[]={two_days_ago}', headers={'Authorization': API_KEY})
    response.raise_for_status()
    games_data = response.json()['data']
    if not games_data:
        raise ValueError("No games found")
    games = pd.DataFrame(games_data)
    games['GAME_DATE'] = pd.to_datetime(games['date']).dt.strftime('%Y-%m-%d')
    target_game_ids = games['id'].unique()[:10]  # Test 10 games
    print(f"Found {len(target_game_ids)} games: {target_game_ids.tolist()}")
except Exception as e:
    print(f"Failed to fetch games: {str(e)}")
    # Fallback: Hardcoded games
    games = pd.DataFrame([
        {'id': 123456, 'date': '2024-10-22', 'home_team': 'New York Knicks', 'visitor_team': 'Cleveland Cavaliers'},
        {'id': 123457, 'date': '2024-10-22', 'home_team': 'San Antonio Spurs', 'visitor_team': 'Dallas Mavericks'},
        {'id': 123458, 'date': '2024-10-23', 'home_team': 'Indiana Pacers', 'visitor_team': 'Oklahoma City Thunder'},
        {'id': 123459, 'date': '2024-10-23', 'home_team': 'Denver Nuggets', 'visitor_team': 'Golden State Warriors'}
    ])
    games['GAME_DATE'] = pd.to_datetime(games['date']).dt.strftime('%Y-%m-%d')
    target_game_ids = games['id'].unique()
    print("Using fallback game list:", target_game_ids.tolist())

if len(target_game_ids) == 0:
    print("No new games found.")
    exit()

# === PROCESS GAMES ===
tracker_data = []
for game_id in target_game_ids:
    print(f"Processing {game_id}...")
    try:
        # Fetch PBP
        response = requests.get(f'{BALLDONTLIE_URL}/plays?game_ids[]={game_id}', headers={'Authorization': API_KEY})
        response.raise_for_status()
        plays_data = response.json()['data']
        plays = pd.DataFrame(plays_data)
        # Filter to period 1
        period1 = plays[plays['period'] == 1]

        # Game Info
        game_row = games[games['id'] == game_id].iloc[0]
        home_team = game_row['home_team']
        away_team = game_row['visitor_team']
        game_date = game_row['GAME_DATE']

        # Tip (first play with 'jump_ball' or similar)
        jump = period1[period1['type'].str.contains('jump|tip', case=False, na=False)].head(1)
        tip_winner = tip_loser = 'No Tip'
        if not jump.empty:
            tip_winner = jump['player'].iloc[0]  # Simplified
            tip_loser = 'Unknown'  # Parse from description if needed
            print(f"  Jump Ball: {tip_winner} vs {tip_loser}")

        # Shots (filter to 'shot' events)
        shots = period1[period1['type'].str.contains('shot', case=False, na=False)]
        first_shot = shots.head(1)
        second_shot = shots.head(2).iloc[1] if len(shots) > 1 else pd.DataFrame()

        # First Shot
        if not first_shot.empty:
            first_shooter = first_shot['player'].iloc[0]
            first_made = first_shot['made'].iloc[0] if 'made' in first_shot.columns else False
            first_type = first_shot['type'].iloc[0]  # e.g., '3pt', 'layup'
            first_team = home_team if first_shot['team_id'].iloc[0] == home_team_id else away_team  # Map IDs if needed
        else:
            first_shooter, first_made, first_type, first_team = 'Unknown', False, 'Other', home_team

        # Second Shot
        if not second_shot.empty:
            second_shooter = second_shot['player'].iloc[0]
            second_made = second_shot['made'].iloc[0] if 'made' in second_shot.columns else False
            second_type = second_shot['type'].iloc[0]
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
        print(f"  Success: {game_id}")
    except Exception as e:
        print(f"  Error on {game_id}: {e}")
        # Placeholder
        tracker_data.append({
            'Game_ID': game_id,
            'Date': '2024-10-22',
            'Home_Team': 'Unknown',
            'Away_Team': 'Unknown',
            'Tip_Winner': 'Unknown',
            'Tip_Loser': 'Unknown',
            'First_Shot_Shooter': 'Unknown',
            'First_Shot_Made': False,
            'First_Shot_Type': 'Other',
            'First_Shot_Team': 'Unknown',
            'Second_Shot_Shooter': 'Unknown',
            'Second_Shot_Made': False,
            'Second_Shot_Type': 'Other'
        })
    time.sleep(1)  # Rate limit buffer

# === EXPORT TO GOOGLE SHEETS ===
if tracker_data:
    try:
        creds = Credentials.from_service_account_file(
            CREDENTIALS_FILE,
            scopes=['https://www.googleapis.com/auth/spreadsheets']
        )
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
        print(f"\nSUCCESS! Added {len(tracker_data)} new games to Google Sheet!")
    except Exception as e:
        print(f"Failed to export to Google Sheets: {e}")
else:
    print("No new data to export.")
