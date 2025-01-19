import requests
from bs4 import BeautifulSoup
import mysql.connector
from mysql.connector import Error
import re
from urllib.parse import urlparse
import os
from dotenv import load_dotenv
import time
import random
from datetime import datetime, timedelta

# Load environment variables for DB credentials
load_dotenv()

DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

# Define leagues and their corresponding league numbers
LEAGUES = [
    {"league_name": "Premier League", "league_number": 9},
    {"league_name": "La Liga", "league_number": 12},
    {"league_name": "Ligue 1", "league_number": 13},
    {"league_name": "Bundesliga", "league_number": 20},
    {"league_name": "Brazil Serie A", "league_number": 24},
    {"league_name": "Saudi Pro League", "league_number": 70},
    {"league_name": "MLS", "league_number": 22},
    {"league_name": "Serie A", "league_number": 11},
    {"league_name": "Champions League", "league_number": 8},
    {"league_name": "Liga MX", "league_number": 31},
    {"league_name": "Europa League", "league_number": 19},
    {"league_name": "Conference League", "league_number": 882},
    {"league_name": "Premier League", "league_number": 690},
    {"league_name": "Serie A", "league_number": 529},
    {"league_name": "Serie A", "league_number": 612},
    {"league_name": "La Liga", "league_number": 569},
    {"league_name": "Ligue 1", "league_number": 604}
]

# Create a mapping from league_number (as string) to league_name for easy lookup
league_mapping = {str(league["league_number"]): league["league_name"] for league in LEAGUES}

def get_db_connection():
    """Create and return a MySQL database connection."""
    try:
        conn = mysql.connector.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        if conn.is_connected():
            return conn
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
    return None

def get_most_recent_date_from_schedule():
    """
    Query the 'schedule' table for the MAX(date).
    Return that date (as a datetime.date or datetime object).
    If no rows exist, return None.
    """
    conn = get_db_connection()
    if not conn:
        return None

    cursor = conn.cursor()
    query = "SELECT MAX(date) FROM schedule"
    cursor.execute(query)
    result = cursor.fetchone()
    cursor.close()
    conn.close()

    if result and result[0]:
        return result[0]  # This should be a date or datetime object
    else:
        return None

def scrape_fixtures(url):
    """
    For the given 'url' (e.g. "https://fbref.com/en/matches/2024-12-14"),
    scrape the page to extract fixtures from all relevant league tables.
    Return a list of tuples: (date_str, home_team, away_team, league_name).
    """
    try:
        response = requests.get(url)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Error fetching URL {url}: {e}")
        return []

    # Extract the date directly from the URL
    parsed_url = urlparse(url)
    date = parsed_url.path.strip("/").split("/")[-1]  # e.g., "2024-12-14"

    soup = BeautifulSoup(response.text, 'html.parser')

    # Match table IDs like "sched_2024-2025_9"
    tables = soup.find_all('div', id=re.compile(r"sched_\d{4}-\d{4}_\d+"))
    if not tables:
        print("No tables found with the specified pattern.")
        return []

    fixtures = []

    for table in tables:
        table_id = table.get('id', '')
        league_number_match = re.search(r"_(\d+)$", table_id)
        if league_number_match:
            league_number = league_number_match.group(1)
            league_name = league_mapping.get(league_number)
            if not league_name:
                # Skip leagues not in the defined list
                print(f"Skipping unknown league number: {league_number}")
                continue
        else:
            print(f"Could not extract league number from table ID: {table_id}")
            continue

        print(f"Processing League: {league_name} (Number: {league_number})")

        # The actual table might be nested inside the div
        inner_table = table.find('table')
        if not inner_table:
            print(f"No inner table found in div with ID: {table_id}")
            continue

        tbody = inner_table.find('tbody')
        if not tbody:
            print(f"No tbody found in table with ID: {table_id}")
            continue

        rows = tbody.find_all('tr')
        for row in rows:
            # Skip rows that are just spacers
            if row.get('class') and 'spacer' in row.get('class'):
                continue

            start_time_cell = row.find('td', {'data-stat': 'start_time'})
            home_team_cell = row.find('td', {'data-stat': 'home_team'})
            away_team_cell = row.find('td', {'data-stat': 'away_team'})

            if start_time_cell and home_team_cell and away_team_cell:
                home_team = home_team_cell.text.strip()
                away_team = away_team_cell.text.strip()
                fixtures.append((date, home_team, away_team, league_name))
            else:
                print(f"Missing data in row: {row}")

    return fixtures

def insert_into_schedule(fixtures):
    """
    Insert the given fixtures into the 'schedule' table, ignoring duplicates (or updating the 'comp' column).
    """
    if not fixtures:
        print("No fixtures to insert.")
        return

    conn = get_db_connection()
    if not conn:
        print("Database connection failed. Cannot insert fixtures.")
        return

    try:
        cursor = conn.cursor()
        insert_query = """
            INSERT INTO schedule (`date`, `home`, `away`, `comp`)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE `comp` = VALUES(`comp`);
        """

        cursor.executemany(insert_query, fixtures)
        conn.commit()
        print(f"Inserted/Updated {cursor.rowcount} fixtures into the database.")
    except Error as e:
        print(f"Error inserting into the database: {e}")
    finally:
        if conn.is_connected():
            conn.close()

def main():
    # 1) Get the most recent date in 'schedule'
    most_recent_date = get_most_recent_date_from_schedule()
    if not most_recent_date:
        print("No existing records in 'schedule'; can't determine the start date.")
        return

    # Ensure we treat this as a datetime.date
    if isinstance(most_recent_date, datetime):
        most_recent_date = most_recent_date.date()

    # 2) We want to scrape the next 7 days after that date
    for i in range(1, 8):
        scrape_date = most_recent_date + timedelta(days=i)
        scrape_date_str = scrape_date.strftime("%Y-%m-%d")
        url = f"https://fbref.com/en/matches/{scrape_date_str}"

        print(f"\nScraping fixtures for date: {scrape_date_str}")
        fixtures = scrape_fixtures(url)
        if fixtures:
            print(f"Scraped {len(fixtures)} fixtures for {scrape_date_str}.")
            insert_into_schedule(fixtures)
        else:
            print(f"No fixtures found for date: {scrape_date_str}.")

        # 3) Wait a random time between 7 and 11 seconds
        wait_seconds = random.randint(7, 11)
        print(f"Sleeping {wait_seconds} seconds before next day...")
        time.sleep(wait_seconds)

    print("\nAll done! Scraped and inserted 7 days of fixtures.")

if __name__ == "__main__":
    main()
