import requests
from bs4 import BeautifulSoup
import mysql.connector
from mysql.connector import Error
import re
import os
from dotenv import load_dotenv
import time
import random

# Load environment variables for DB credentials
load_dotenv()

DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

# Regex to detect a "bad" URL => any with two 8-character alphanumeric codes in the path.
# (This is used in Python to quickly check if a URL is "bad".)
BAD_URL_REGEX = re.compile(r'/[0-9A-Za-z]{8}/[0-9A-Za-z]{8}/')

def is_bad_url(url_text):
    """
    Return True if the URL matches the pattern of being 'bad' in Python terms:
     - Contains two consecutive 8-character alphanumeric segments (like /abcdef12/ghijkl34/).
    """
    if not url_text:
        return False
    return bool(BAD_URL_REGEX.search(url_text.strip()))

def get_db_connection():
    """
    Create and return a MySQL database connection using .env credentials.
    """
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

def get_earliest_bad_or_null_url_date():
    """
    Look in 'schedule' table for rows whose match_report is either:
      - NULL, or
      - A BAD URL (i.e., has two 8-character alphanumeric codes in the path).
    Return the *earliest* date (MIN date) that matches either condition.
    If none found, return None.

    NOTE: We use MySQL's REGEXP with escaped slashes so that it only matches
          truly "bad" URLs and doesn't incorrectly flag good ones.
    """
    conn = get_db_connection()
    if not conn:
        return None

    cursor = conn.cursor()
    query = """
        SELECT MIN(date)
        FROM schedule
        WHERE match_report IS NULL
           OR match_report REGEXP '/[[:alnum:]]{8}\\/[[:alnum:]]{8}\\/'
    """
    cursor.execute(query)
    result = cursor.fetchone()
    cursor.close()
    conn.close()

    if result and result[0]:
        return result[0]  # e.g. 2023-01-01
    return None

def fetch_dates_from_earliest_bad_date(start_date):
    """
    Fetch all *distinct* dates from 'schedule', in ascending order,
    where date >= start_date.
    Return as a list of date strings (or date objects).
    """
    if not start_date:
        return []

    conn = get_db_connection()
    if not conn:
        return []
    cursor = conn.cursor()

    query = """
        SELECT DISTINCT date
        FROM schedule
        WHERE date >= %s
        ORDER BY date
    """
    cursor.execute(query, (start_date,))
    rows = cursor.fetchall()
    dates = [row[0] for row in rows]

    cursor.close()
    conn.close()
    return dates

def scrape_fixture_urls_for_date(date_str):
    """
    For a given date (e.g. '2024-12-22'), build the FBref daily matches URL:
      https://fbref.com/en/matches/<date_str>
    Then scrape every fixture on that page, returning a dictionary:
      (home_team, away_team) -> 'https://fbref.com/en/matches/...'
    """
    url = f"https://fbref.com/en/matches/{date_str}"

    try:
        response = requests.get(url)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Error fetching URL {url}: {e}")
        return {}

    soup = BeautifulSoup(response.text, 'html.parser')

    # Look for DIVs whose IDs match something like: sched_2024-2025_12
    tables = soup.find_all('div', id=re.compile(r"sched_\d{4}-\d{4}_\d+"))
    if not tables:
        print(f"No league tables found for date {date_str}.")
        return {}

    fixtures_dict = {}

    for table_div in tables:
        inner_table = table_div.find('table')
        if not inner_table:
            continue

        tbody = inner_table.find('tbody')
        if not tbody:
            continue

        rows = tbody.find_all('tr')

        for row in rows:
            # Skip spacer rows
            if row.get('class') and 'spacer' in row.get('class'):
                continue

            home_team_cell = row.find('td', {'data-stat': 'home_team'})
            away_team_cell = row.find('td', {'data-stat': 'away_team'})
            match_report_cell = row.find('td', {'data-stat': 'match_report'})

            if home_team_cell and away_team_cell:
                home_team = home_team_cell.get_text(strip=True)
                away_team = away_team_cell.get_text(strip=True)

                match_report_url = None
                if match_report_cell:
                    a_tag = match_report_cell.find('a')
                    if a_tag and 'href' in a_tag.attrs:
                        relative_url = a_tag['href'].strip()
                        # Construct the full URL
                        match_report_url = f"https://fbref.com{relative_url}"

                fixtures_dict[(home_team, away_team)] = match_report_url

    return fixtures_dict

def update_schedule_for_date(date_str, fixtures_dict):
    """
    For the specified date, update the 'match_report' column in 'schedule'
    for every (home, away) that is in fixtures_dict. This overwrites old values.

    We stop (by returning True) if we detect that the 'new' scraped URL is
    itself a BAD URL — meaning we don't want to continue further.

    Otherwise, we update the row and keep going.
    """
    conn = get_db_connection()
    if not conn:
        print("Database connection failed. Skipping update.")
        return False

    cursor = conn.cursor()

    select_query = """
        SELECT home, away, match_report
        FROM schedule
        WHERE date = %s
    """
    cursor.execute(select_query, (date_str,))
    rows = cursor.fetchall()

    update_query = """
        UPDATE schedule
        SET match_report = %s
        WHERE date = %s
          AND home = %s
          AND away = %s
    """

    rows_updated = 0
    stop_script = False

    for (home_team, away_team, old_url) in rows:
        # Attempt to get a new URL from fixtures_dict
        new_url = fixtures_dict.get((home_team, away_team))

        if not new_url:
            # If there's no new URL for this matchup, just skip.
            continue

        # Check if the new URL is "bad"
        if is_bad_url(new_url):
            print(f"Encountered newly scraped BAD URL => {new_url}")
            stop_script = True
            break

        # Otherwise, update the row
        cursor.execute(update_query, (new_url, date_str, home_team, away_team))
        rows_updated += 1

    if not stop_script:
        # Only commit if we are not stopping mid-way
        conn.commit()

    print(f"Updated {rows_updated} rows for date: {date_str}")

    cursor.close()
    conn.close()
    return stop_script

def main():
    # 1) Find the earliest date with a NULL or "bad" URL
    start_date = get_earliest_bad_or_null_url_date()
    if not start_date:
        print("No date found with a NULL or bad URL => nothing to do.")
        return

    # 2) Fetch all distinct dates from schedule >= that date, in ascending order
    dates_to_update = fetch_dates_from_earliest_bad_date(start_date)
    if not dates_to_update:
        print("No dates found for updating.")
        return

    # 3) Loop through each date, scrape matches, update DB
    for date_str in dates_to_update:
        print(f"\nProcessing date: {date_str}...")

        # Scrape that day's fixtures
        fixtures_dict = scrape_fixture_urls_for_date(date_str)
        if not fixtures_dict:
            print(f"No fixtures found for {date_str}, skipping DB update.")
        else:
            # Update schedule table with fresh match_report links
            stop_script = update_schedule_for_date(date_str, fixtures_dict)
            if stop_script:
                print("Stopping script because a newly scraped URL was also bad.")
                break

        # 4) Sleep for a random time between 8 and 15 seconds
        wait_seconds = random.randint(8, 15)
        print(f"Sleeping {wait_seconds} seconds to avoid rate-limiting.")
        time.sleep(wait_seconds)

    print("\nAll done!")

if __name__ == "__main__":
    main()

