# nba_tracker.py (BallDontLie Paid Tier)
import pandas as pd
import time
import re
import requests
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials

# === CONFIG ===
BALLDONTLIE_URL = 'https://www.balldontlie.io/api/v1'
API_KEY = '9d36588f-9403-4d3e-8654-8357d10537d7'  # Your paid-tier key
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

# === FETCH GAMES (2024-25 SEASON, LAST 2 DAYS) ===
print("Fetching games from last 2 days...")
try:
    today = pd.Timestamp.now().strftime('%Y-%m-%d')
    headers = {'Authorization': API_KEY}
    games_data = []
    page = 1
    while True:
        response = requests.get(
            f'{BALLDONTLIE_URL}/games?season=2024&start_date=2024-10-22&end_date={today}&per_page=100&page={page}',
            headers=headers
        )
        response.raise_for_status()
        data = response.json()
        games_data.extend(data['data'])
        if not data['meta']['next_page']:
            break
        page += 1
        time.sleep(1)  # Respect API
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
    target_game_ids = [gid for gid in games['id'].unique() if str(gid) not in existing_ids][:10]  # Skip duplicates, limit 10
    print(f"Found {len(target_game_ids)} new games: {target_game_ids}")
except Exception as e:
    print(f"Failed to fetch games: {str(e)}")
    # Dynamic
