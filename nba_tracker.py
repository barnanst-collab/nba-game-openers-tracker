# nba_tracker.py
import pandas as pd
import time
import re
from nba_api.stats.endpoints import leaguegamefinder, playbyplay
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
from requests.exceptions import ReadTimeout
from datetime import datetime, timedelta

# === CONFIG ===
SPREADSHEET_ID = '1uNH3tko9hJkgD_JVACeVQ0BwS-Q_8qH5HT0FHEwvQIY'
CREDENTIALS_FILE = 'credentials.json'

# === FETCH GAMES (LAST 2 DAYS) ===
print("Fetching games from last 2 days...")
try:
    gamefinder = leaguegamefinder.LeagueGameFinder(
        season_nullable='2024-25',
        season_type_nullable='Regular Season',
        timeout=180
    )
    games = gamefinder.get_data_frames()[0]
    games['GAME_DATE'] = pd.to_datetime(games['GAME_DATE']).dt.strftime('%Y-%m-%d')
    two_days_ago = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
    target_game_ids = games[games['GAME_DATE'] >= two_days_ago]['GAME_ID'].unique()
    print(f"Found {len(target_game_ids)} games: {target_game_ids.tolist()}")
except Exception as e:
    print(f"Failed to fetch games: {str(e)}")
    # Fallback: Hardcoded games (Oct 22-23, 2025, 4 games)
    games = pd.DataFrame([
        {'GAME_ID': '0022401191', 'GAME_DATE': '2024-10-22', 'TEAM_NAME': 'New York Knicks', 'MATCHUP': 'NYK vs. CLE'},
        {'GAME_ID': '0022401191', 'GAME_DATE': '2024-10-22', 'TEAM_NAME': 'Cleveland Cavaliers', 'MATCHUP': 'CLE @ NYK'},
        {'GAME_ID': '0022401192', 'GAME_DATE': '2024-10-22', 'TEAM_NAME': 'San Antonio Spurs', 'MATCHUP': 'SAS vs. DAL'},
        {'GAME_ID': '0022401192', 'GAME_DATE': '2024-10-22', 'TEAM_NAME': 'Dallas Mavericks', 'MATCHUP': 'DAL @ SAS'},
        {'GAME_ID': '0022401193', 'GAME_DATE': '2024-10-23', 'TEAM_NAME': 'Oklahoma City Thunder', 'MATCHUP': 'OKC @ IND'},
        {'GAME_ID': '0022401193', 'GAME_DATE': '2024-10-23', 'TEAM_NAME': 'Indiana Pacers', 'MATCHUP': 'IND vs. OKC'},
        {'GAME_ID': '0022401194', 'GAME_DATE': '2024-10-23', 'TEAM_NAME': 'Denver Nuggets', 'MATCHUP': 'DEN vs. GSW'},
        {'GAME_ID': '0022401194', 'GAME_DATE': '2024-10-23', 'TEAM_NAME': 'Golden State Warriors', 'MATCHUP': 'GSW @ DEN'}
    ])
    target_game_ids = games['GAME_ID'].unique()
    print("Using fallback game list:", target_game_ids.tolist())

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
            pbp = playbyplay.PlayByPlay(game_id, timeout=180).get_data_frames()[0]
            if pbp.empty:
                print(f"  No data for {game_id}")
                break
            period1 = pbp[pbp['PERIOD'] == 1]
            if len(period1) == 0:
                print(f"  No Period 1 for {game_id}")
                break

            # Game Info
            game_rows = games[games['GAME_ID'] == game_id]
            home = game_rows[game_rows['MATCHUP'].str.contains(' vs. ')]['TEAM_NAME']
            away = game_rows[game_rows['MATCHUP'].str.contains(' @ ')]['TEAM_NAME']
            home_team = home.iloc[0] if not home.empty else 'Unknown'
            away_team = away.iloc[0] if not away.empty else 'Unknown'
            game_date = game_rows['GAME_DATE'].iloc[0]

            # Tip
            jump = period1[period1['EVENTMSGTYPE'] == 10].head(1)
            tip_winner = tip_loser = 'No Tip'
            if not jump.empty:
                desc = jump.iloc[0].get('HOMEDESCRIPTION', '') or jump.iloc[0].get('VISIT
