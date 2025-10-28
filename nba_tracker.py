# nba_tracker.py — NBA Game Openers Tracker (BallDontLie Paid Tier)
import pandas as pd
import time
import re
import requests
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
import os
from datetime import datetime, timedelta

# === CONFIG ===
BALLDONTLIE_URL = 'https://api.balldontlie.io/v1'
API_KEY = os.environ.get('BALLDONTLIE_API_KEY') or '9d36588f-9403-4d3e-8654-8357d10537d7'
SPREADSHEET_ID = '1uNH3tko9hJkgD_JVACeVQ0BwS-Q_8qH5HT0FHEwvQIY'
CREDENTIALS_FILE = 'credentials.json'

# Validate API key
if not API_KEY:
    print("ERROR: BALLDONTLIE_API_KEY is missing.")
    exit()

# === DYNAMIC DATES ===
today = datetime.now().strftime('%Y-%m-%d')
seven_days_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
print(f"Fetching games from {seven_days_ago} to {today}")

# Fallback dates (2024–25 season, known to have PBP)
FALLBACK_TODAY = '2024-10-23'
FALLBACK_SEVEN_DAYS_AGO = '2024-10-22'

# === TEAM PLACEHOLDERS ===
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

TEAM_NAME_MAP = {v.split()[-1]: k for k, v in TEAM_PLACEHOLDERS.items()}
TEAM_NAME_MAP.update({
    'Celtics': 'Boston Celtics', 'Raptors': 'Toronto Raptors', 'Lakers': 'Los Angeles Lakers',
    'Timberwolves': 'Minnesota Timberwolves', '76ers': 'Philadelphia 76ers', 'Bucks': 'Milwaukee Bucks',
    'Suns': 'Phoenix Suns', 'Clippers': 'Los Angeles Clippers', 'Heat': 'Miami Heat', 'Bulls': 'Chicago Bulls',
    'Knicks': 'New York Knicks', 'Cavaliers': 'Cleveland Cavaliers', 'Spurs': 'San Antonio Spurs',
    'Mavericks': 'Dallas Mavericks', 'Pacers': 'Indiana Pacers', 'Thunder': 'Oklahoma City Thunder',
    'Nuggets': 'Denver Nuggets', 'Warriors': 'Golden State Warriors', 'Trail Blazers': 'Portland Trail Blazers',
    'Kings': 'Sacramento Kings', 'Magic': 'Orlando Magic', 'Hawks': 'Atlanta Hawks', 'Hornets': 'Charlotte Hornets',
    'Pistons': 'Detroit Pistons', 'Wizards': 'Washington Wizards', 'Nets': 'Brooklyn Nets',
    'Grizzlies': 'Memphis Grizzlies', 'Pelicans': 'New Orleans Pelicans', 'Jazz': 'Utah Jazz', 'Rockets': 'Houston Rockets'
})

# === INITIALIZE GOOGLE SHEETS ===
print("Initializing Google Sheets...")
try:
    creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=['https://www.googleapis.com/auth/spreadsheets'])
    service = build('sheets', 'v4', credentials=creds)
except Exception as e:
    print(f"Failed to initialize Google Sheets: {e}")
    exit()

# === CHECK EXISTING GAMES ===
print("Checking for existing games...")
try:
    existing_games = service.spreadsheets().values().get(spreadsheetId=SPREADSHEET_ID, range='Sheet1!A:A').execute().get('values', [])
    existing_ids = [row[0] for row in existing_games if row and row[0] != 'Game_ID']
    print(f"Found {len(existing_ids)} existing games.")
except Exception as e:
    print(f"Failed to read existing games: {e}")
    existing_ids = []

# === FETCH GAMES FUNCTION ===
def fetch_games(start_date, end_date):
    headers = {'Authorization': f'Bearer {API_KEY}'}
    games_data = []
    page = 1
    while True:
        url = f'{BALLDONTLIE_URL}/games?start_date={start_date}&end_date={end_date}&per_page=100&page={page}'
        try:
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code != 200:
                print(f"API error {response.status_code}: {response.text}")
                return None
            data = response.json()
            print(f"  Page {page}: {len(data['data'])} games")
            games_data.extend(data['data'])
            if not data['meta'].get('next_cursor'):
                break
            page += 1
            time.sleep(1)
        except Exception as e:
            print(f"Request failed: {e}")
            return None
    return games_data

# === TRY RECENT GAMES ===
print(f"Fetching recent games ({seven_days_ago} to {today})...")
games_data = fetch_games(seven_days_ago, today)

# === FALLBACK IF NEEDED ===
if not games_data or len(games_data) == 0:
    print("No recent games. Trying fallback (2024-10-22 to 2024-10-23)...")
    games_data = fetch_games(FALLBACK_SEVEN_DAYS_AGO, FALLBACK_TODAY)

if not games_data:
    print("No games found. Using static fallback.")
    games = pd.DataFrame([{
        'id': '99999999', 'date': '2024-10-25', 'home_team': {'name': 'Miami Heat'}, 'visitor_team': {'name': 'Chicago Bulls'}, 'home_team_id': 1610612748
    }])
else:
    games = pd.DataFrame([
        {
            'id': str(game['id']),
            'GAME_DATE': pd.to_datetime(game['date']).strftime('%Y-%m-%d'),
            'home_team': TEAM_NAME_MAP.get(game['home_team']['name'], game['home_team']['name']),
            'visitor_team': TEAM_NAME_MAP.get(game['visitor_team']['name'], game['visitor_team']['name']),
            'home_team_id': game['home_team']['id']
        } for game in games_data
    ])

# === FILTER COMPLETED GAMES ONLY ===
now = datetime.now()
valid_games = []
for _, game in games.iterrows():
    game_end = pd.to_datetime(game['GAME_DATE']) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
    if game_end < now and game['id'] not in existing_ids:
        valid_games.append(game)

target_games = valid_games[:10]
target_game_ids = [g['id'] for g in target_games]

print(f"Found {len(target_game_ids)} completed new games: {target_game_ids}")
if not target_game_ids:
    print("No completed games to process. Exiting.")
    exit()

# === PROCESS GAMES ===
tracker_data = []
headers = {'Authorization': f'Bearer {API_KEY}'}

for game in target_games:
    game_id = game['id']
    print(f"\nProcessing Game ID: {game_id} | {game['visitor_team']} @ {game['home_team']} | {game['GAME_DATE']}")
    
    home_team = game['home_team']
    away_team = game['visitor_team']
    home_team_id = game['home_team_id']
    game_date = game['GAME_DATE']

    home_ph = TEAM_PLACEHOLDERS.get(home_team, {'tip': 'Unknown', 'shot': 'Unknown'})
    away_ph = TEAM_PLACEHOLDERS.get(away_team, {'tip': 'Unknown', 'shot': 'Unknown'})

    success = False
    for attempt in range(5):
        try:
            plays_data = []
            page = 1
            while True:
                resp = requests.get(
                    f'{BALLDONTLIE_URL}/plays?game_ids[]={game_id}&per_page=100&page={page}',
                    headers=headers, timeout=10
                )
                if resp.status_code == 404:
                    print(f"  404: No PBP data for {game_id}")
                    break
                resp.raise_for_status()
                data = resp.json()
                print(f"  PBP page {page}: {len(data['data'])} plays")
                plays_data.extend(data['data'])
                if not data['meta'].get('next_cursor'):
                    break
                page += 1
                time.sleep(1)

            if not plays_data:
                break

            plays = pd.DataFrame(plays_data)
            period1 = plays[plays['period'] == 1]

            # Tip-off
            jump = period1[period1['description'].str.contains('jump|tip', case=False, na=False)].head(1)
            tip_winner = tip_loser = 'No Tip'
            if not jump.empty:
                desc = jump.iloc[0]['description']
                m = re.search(r'Jump Ball (\w+\.?\s*\w*) vs\. (\w+\.?\s*\w*)', desc)
                if m:
                    tip_winner, tip_loser = m.groups()
                else:
                    tip_winner = jump.iloc[0].get('player', 'Unknown')
                print(f"  Jump Ball: {tip_winner} vs {tip_loser}")

            # Shots
            shots = period1[period1['description'].str.contains('shot|layup|dunk|free throw|3pt', case=False, na=False)]
            first_shot = shots.head(1)
            second_shot = shots.head(2).iloc[1] if len(shots) > 1 else pd.Series()

            def get_shot_info(shot):
                if shot.empty:
                    return 'Unknown', False, 'Other', home_team
                shooter = shot['player'].iloc[0] if 'player' in shot else 'Unknown'
                made = shot['made'].iloc[0] if 'made' in shot else False
                desc = shot['description'].iloc[0]
                shot_type = ('Dunk' if 'dunk' in desc.lower() else
                            'Layup' if 'layup' in desc.lower() else
                            'Free Throw' if 'free throw' in desc.lower() else
                            '3pt' if '3pt' in desc.lower() else 'Other')
                team = home_team if shot['team_id'].iloc[0] == home_team_id else away_team
                return shooter, made, shot_type, team

            first_shooter, first_made, first_type, first_team = get_shot_info(first_shot)
            second_shooter, second_made, second_type, _ = get_shot_info(second_shot)

            tracker_data.append({
                'Game_ID': game_id, 'Date': game_date, 'Home_Team': home_team, 'Away_Team': away_team,
                'Tip_Winner': tip_winner, 'Tip_Loser': tip_loser,
                'First_Shot_Shooter': first_shooter, 'First_Shot_Made': first_made,
                'First_Shot_Type': first_type, 'First_Shot_Team': first_team,
                'Second_Shot_Shooter': second_shooter, 'Second_Shot_Made': second_made,
                'Second_Shot_Type': second_type
            })
            print(f"  First Shot: {first_shooter} → {first_type} ({'Made' if first_made else 'Missed'})")
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
                print(f"  Placeholder: {home_ph['tip']} vs {away_ph['tip']}, {home_ph['shot']} → Layup")
                success = True
        time.sleep(1)

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
        print(f"Failed to export: {e}")
else:
    print("No data to export.")
