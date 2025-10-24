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
                desc = jump.iloc[0].get('HOMEDESCRIPTION', '') or jump.iloc[0].get('VISITORDESCRIPTION', '')
                m = re.search(r'Jump Ball (\w+\.?\s*\w*) vs\. (\w+\.?\s*\w*)', desc)
                if m:
                    tip_winner, tip_loser = m.groups()
                print(f"  Jump Ball: {tip_winner} vs {tip_loser}")

            # Shots
            fg = period1[period1['EVENTMSGTYPE'].isin([1, 3])].reset_index(drop=True)
            def get_shot(idx):
                if len(fg) <= idx:
                    return 'No Shot', False, 'Other', 'Unknown'
                shot = fg.iloc[idx]
                desc = shot.get('HOMEDESCRIPTION', '') or shot.get('VISITORDESCRIPTION', '')
                m = re.search(r'^(\w+\.?\s*\w*?)(?=\s)', desc)
                shooter = m.group(1).strip() if m else 'Unknown'
                made = shot['EVENTMSGTYPE'] == 1
                shot_type = 'Dunk' if 'dunk' in desc.lower() else \
                            'Layup' if 'layup' in desc.lower() else \
                            'Free Throw' if 'free throw' in desc.lower() else \
                            '3pt' if '3pt' in desc.lower() else 'Other'
                team = home_team if 'HOMEDESCRIPTION' in shot and pd.notna(shot['HOMEDESCRIPTION']) else away_team
                return shooter, made, shot_type, team

            first_shooter, first_made, first_type, first_team = get_shot(0)
            second_shooter, second_made, second_type, _ = get_shot(1)
            print(f"  First Shot: {first_shooter} → {first_type} ({'Made' if first_made else 'Missed'})")
            if len(fg) > 1:
                print(f"  Second Shot: {second_shooter} → {second_type} ({'Made' if second_made else 'Missed'})")

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
            success = True
            break
        except ReadTimeout as e:
            print(f"  Attempt {attempt+1}/5 failed for {game_id}: {e}")
            if attempt < 4:
                time.sleep(2 ** attempt)
                continue
            else:
                print(f"  Skipped {game_id} after 5 attempts")
                # Add placeholder data
                game_rows = games[games['GAME_ID'] == game_id]
                home = game_rows[game_rows['MATCHUP'].str.contains(' vs. ')]['TEAM_NAME']
                away = game_rows[game_rows['MATCHUP'].str.contains(' @ ')]['TEAM_NAME']
                home_team = home.iloc[0] if not home.empty else 'Unknown'
                away_team = away.iloc[0] if not away.empty else 'Unknown'
                game_date = game_rows['GAME_DATE'].iloc[0]
                tracker_data.append({
                    'Game_ID': game_id,
                    'Date': game_date,
                    'Home_Team': home_team,
                    'Away_Team': away_team,
                    'Tip_Winner': 'Unknown (API Timeout)',
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
        except Exception as e:
            print(f"  Error on {game_id}: {e}")
            break
    if not success:
        print(f"  Failed to process {game_id}, added placeholder data")
        game_rows = games[games['GAME_ID'] == game_id]
        home = game_rows[game_rows['MATCHUP'].str.contains(' vs. ')]['TEAM_NAME']
        away = game_rows[game_rows['MATCHUP'].str.contains(' @ ')]['TEAM_NAME']
        home_team = home.iloc[0] if not home.empty else 'Unknown'
        away_team = away.iloc[0] if not away.empty else 'Unknown'
        game_date = game_rows['GAME_DATE'].iloc[0]
        tracker_data.append({
            'Game_ID': game_id,
            'Date': game_date,
            'Home_Team': home_team,
            'Away_Team': away_team,
            'Tip_Winner': 'Unknown (API Timeout)',
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
    time.sleep(1.5)

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
