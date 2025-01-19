import os
import random
import re
import time
import requests
from bs4 import BeautifulSoup
import mysql.connector
from dotenv import load_dotenv

# ---------------------------
# 1) Load environment variables from .env
# ---------------------------
load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# ---------------------------
# 2) (Optional) Big 5 leagues reference, if needed for logic
#    Not actually used in this new version, but can be retained if you'd like.
# ---------------------------
BIG_LEAGUES = [
    "Premier League",
    "La Liga",
    "Bundesliga",
    "Serie A",
    "Ligue 1"
]

# ---------------------------
# 3) Map FBref stats to scout_reports columns
#    stat_name => (db_col_per90, db_col_percentile)
# ---------------------------
MAPPING = {
    "Goals": ("goals_per90", "goals_percentile"),
    "Assists": ("assists_per90", "assists_percentile"),
    "Goals + Assists": ("goals_plus_assists_per90", "goals_plus_assists_percentile"),
    "Non-Penalty Goals": ("non_penalty_goals_per90", "non_penalty_goals_percentile"),
    "Penalty Kicks Made": ("penalty_kicks_made_per90", "penalty_kicks_made_percentile"),
    "Penalty Kicks Attempted": ("penalty_kicks_attempted_per90", "penalty_kicks_attempted_percentile"),
    "Yellow Cards": ("yellow_cards_per90", "yellow_cards_percentile"),
    "Red Cards": ("red_cards_per90", "red_cards_percentile"),

    "xG: Expected Goals": ("xg_per90", "xg_percentile"),
    "npxG: Non-Penalty xG": ("npxg_per90", "npxg_percentile"),
    "xAG: Exp. Assisted Goals": ("xag_per90", "xag_percentile"),
    "npxG + xAG": ("npxg_plus_xag_per90", "npxg_plus_xag_percentile"),

    "Progressive Carries": ("progressive_carries_per90", "progressive_carries_percentile"),
    "Progressive Passes": ("progressive_passes_per90", "progressive_passes_percentile"),
    "Progressive Passes Rec": ("progressive_passes_rec_per90", "progressive_passes_rec_percentile"),

    "Shots Total": ("shots_total_per90", "shots_total_percentile"),
    "Shots on Target": ("shots_on_target_per90", "shots_on_target_percentile"),
    "Shots on Target %": ("shots_on_target_pct_per90", "shots_on_target_pct_percentile"),
    "Goals/Shot": ("goals_per_shot_per90", "goals_per_shot_percentile"),
    "Goals/Shot on Target": ("goals_per_shot_on_target_per90", "goals_per_shot_on_target_percentile"),
    "Average Shot Distance": ("average_shot_distance_per90", "average_shot_distance_percentile"),
    "Shots from Free Kicks": ("shots_from_free_kicks_per90", "shots_from_free_kicks_percentile"),
    "npxG/Shot": ("npxg_per_shot_per90", "npxg_per_shot_percentile"),
    "Goals - xG": ("goals_minus_xg_per90", "goals_minus_xg_percentile"),
    "Non-Penalty Goals - npxG": ("non_penalty_goals_minus_npxg_per90", "non_penalty_goals_minus_npxg_percentile"),

    "Passes Completed": ("passes_completed_per90", "passes_completed_percentile"),
    "Passes Attempted": ("passes_attempted_per90", "passes_attempted_percentile"),
    "Pass Completion %": ("pass_completion_pct_per90", "pass_completion_pct_percentile"),
    "Total Passing Distance": ("total_passing_distance_per90", "total_passing_distance_percentile"),
    "Progressive Passing Distance": ("progressive_passing_distance_per90", "progressive_passing_distance_percentile"),

    "Passes Completed (Short)": ("passes_completed_short_per90", "passes_completed_short_percentile"),
    "Passes Attempted (Short)": ("passes_attempted_short_per90", "passes_attempted_short_percentile"),
    "Pass Completion % (Short)": ("pass_completion_pct_short_per90", "pass_completion_pct_short_percentile"),

    "Passes Completed (Medium)": ("passes_completed_medium_per90", "passes_completed_medium_percentile"),
    "Passes Attempted (Medium)": ("passes_attempted_medium_per90", "passes_attempted_medium_percentile"),
    "Pass Completion % (Medium)": ("pass_completion_pct_medium_per90", "pass_completion_pct_medium_percentile"),

    "Passes Completed (Long)": ("passes_completed_long_per90", "passes_completed_long_percentile"),
    "Passes Attempted (Long)": ("passes_attempted_long_per90", "passes_attempted_long_percentile"),
    "Pass Completion % (Long)": ("pass_completion_pct_long_per90", "pass_completion_pct_long_percentile"),

    "xA: Expected Assists": ("xa_per90", "xa_percentile"),
    "Key Passes": ("key_passes_per90", "key_passes_percentile"),
    "Passes into Final Third": ("passes_into_final_third_per90", "passes_into_final_third_percentile"),
    "Passes into Penalty Area": ("passes_into_penalty_area_per90", "passes_into_penalty_area_percentile"),
    "Crosses into Penalty Area": ("crosses_into_penalty_area_per90", "crosses_into_penalty_area_percentile"),

    "SCA (Live-ball Pass)": ("sca_live_ball_pass_per90", "sca_live_ball_pass_percentile"),
    "SCA (Dead-ball Pass)": ("sca_dead_ball_pass_per90", "sca_dead_ball_pass_percentile"),
    "SCA (Take-On)": ("sca_take_on_per90", "sca_take_on_percentile"),
    "SCA (Shot)": ("sca_shot_per90", "sca_shot_percentile"),
    "SCA (Fouls Drawn)": ("sca_fouls_drawn_per90", "sca_fouls_drawn_percentile"),
    "SCA (Defensive Action)": ("sca_defensive_action_per90", "sca_defensive_action_percentile"),

    "GCA (Live-ball Pass)": ("gca_live_ball_pass_per90", "gca_live_ball_pass_percentile"),
    "GCA (Dead-ball Pass)": ("gca_dead_ball_pass_per90", "gca_dead_ball_pass_percentile"),
    "GCA (Take-On)": ("gca_take_on_per90", "gca_take_on_percentile"),
    "GCA (Shot)": ("gca_shot_per90", "gca_shot_percentile"),
    "GCA (Fouls Drawn)": ("gca_fouls_drawn_per90", "gca_fouls_drawn_percentile"),
    "GCA (Defensive Action)": ("gca_defensive_action_per90", "gca_defensive_action_percentile"),

    "Tackles": ("tackles_per90", "tackles_percentile"),
    "Tackles Won": ("tackles_won_per90", "tackles_won_percentile"),
    "Tackles (Def 3rd)": ("tackles_def_3rd_per90", "tackles_def_3rd_percentile"),
    "Tackles (Mid 3rd)": ("tackles_mid_3rd_per90", "tackles_mid_3rd_percentile"),
    "Tackles (Att 3rd)": ("tackles_att_3rd_per90", "tackles_att_3rd_percentile"),
    "Dribblers Tackled": ("dribblers_tackled_per90", "dribblers_tackled_percentile"),
    "Dribbles Challenged": ("dribbles_challenged_per90", "dribbles_challenged_percentile"),
    "% of Dribblers Tackled": ("pct_of_dribblers_tackled_per90", "pct_of_dribblers_tackled_percentile"),
    "Challenges Lost": ("challenges_lost_per90", "challenges_lost_percentile"),
    "Blocks": ("blocks_per90", "blocks_percentile"),
    "Shots Blocked": ("shots_blocked_per90", "shots_blocked_percentile"),
    "Passes Blocked": ("passes_blocked_per90", "passes_blocked_percentile"),
    "Interceptions": ("interceptions_per90", "interceptions_percentile"),
    "Tkl+Int": ("tkl_plus_int_per90", "tkl_plus_int_percentile"),
    "Clearances": ("clearances_per90", "clearances_percentile"),
    "Errors": ("errors_per90", "errors_percentile"),

    "Touches": ("touches_per90", "touches_percentile"),
    "Touches (Def Pen)": ("touches_def_pen_per90", "touches_def_pen_percentile"),
    "Touches (Def 3rd)": ("touches_def_3rd_per90", "touches_def_3rd_percentile"),
    "Touches (Mid 3rd)": ("touches_mid_3rd_per90", "touches_mid_3rd_percentile"),
    "Touches (Att 3rd)": ("touches_att_3rd_per90", "touches_att_3rd_percentile"),
    "Touches (Att Pen)": ("touches_att_pen_per90", "touches_att_pen_percentile"),
    "Touches (Live-Ball)": ("touches_live_ball_per90", "touches_live_ball_percentile"),

    "Take-Ons Attempted": ("take_ons_attempted_per90", "take_ons_attempted_percentile"),
    "Successful Take-Ons": ("successful_take_ons_per90", "successful_take_ons_percentile"),
    "Successful Take-On %": ("successful_take_on_pct_per90", "successful_take_on_pct_percentile"),
    "Times Tackled During Take-On": ("times_tackled_during_take_on_per90", "times_tackled_during_take_on_percentile"),
    "Tackled During Take-On Percentage": ("tackled_during_take_on_pct_per90", "tackled_during_take_on_pct_percentile"),

    "Carries": ("carries_per90", "carries_percentile"),
    "Total Carrying Distance": ("total_carrying_distance_per90", "total_carrying_distance_percentile"),
    "Progressive Carrying Distance": ("progressive_carrying_distance_per90", "progressive_carrying_distance_percentile"),
    "Carries into Final Third": ("carries_into_final_third_per90", "carries_into_final_third_percentile"),
    "Carries into Penalty Area": ("carries_into_penalty_area_per90", "carries_into_penalty_area_percentile"),
    "Miscontrols": ("miscontrols_per90", "miscontrols_percentile"),
    "Dispossessed": ("dispossessed_per90", "dispossessed_percentile"),

    "Passes Received": ("passes_received_per90", "passes_received_percentile"),

    "Second Yellow Card": ("second_yellow_card_per90", "second_yellow_card_percentile"),
    "Fouls Committed": ("fouls_committed_per90", "fouls_committed_percentile"),
    "Fouls Drawn": ("fouls_drawn_per90", "fouls_drawn_percentile"),
    "Offsides": ("offsides_per90", "offsides_percentile"),
    "Crosses": ("crosses_per90", "crosses_percentile"),
    "Passes Offside": ("passes_offside_per90", "passes_offside_percentile"),

    "Penalty Kicks Won": ("penalty_kicks_won_per90", "penalty_kicks_won_percentile"),
    "Penalty Kicks Conceded": ("penalty_kicks_conceded_per90", "penalty_kicks_conceded_percentile"),

    "Own Goals": ("own_goals_per90", "own_goals_percentile"),

    "Ball Recoveries": ("ball_recoveries_per90", "ball_recoveries_percentile"),

    "Aerials Won": ("aerials_won_per90", "aerials_won_percentile"),
    "Aerials Lost": ("aerials_lost_per90", "aerials_lost_percentile"),
    "% of Aerials Won": ("pct_of_aerials_won_per90", "pct_of_aerials_won_percentile"),
}


def extract_stats(url):
    """
    Scrape the scouting data from FBref, along with the 'compared_position' (e.g. 'vs. Forwards').

    Returns: (stats_dict, compared_position, error_occurred)
      - stats_dict: dict of extracted stats
      - compared_position: e.g. 'vs. Forwards'
      - error_occurred: True if a requests/connection error occurred
    """
    error_occurred = False
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
    except Exception as e:
        print(f"[ERROR] Request failed for {url}: {e}")
        return {}, None, True

    soup = BeautifulSoup(resp.text, 'html.parser')

    # Extract something like "vs. Forwards"
    compared_position_el = soup.select_one('div.filter.switcher div.current a.sr_preset')
    compared_position = compared_position_el.get_text(strip=True) if compared_position_el else None

    scout_tables = soup.find_all('table', id=re.compile(r'^scout_full_'))
    stats_dict = {}

    for table in scout_tables:
        for tr in table.find_all('tr'):
            stat_th = tr.find('th', {'data-stat': 'statistic'})
            if not stat_th:
                continue

            stat_name = stat_th.get_text(strip=True)
            per90_td = tr.find('td', {'data-stat': 'per90'})
            percentile_td = tr.find('td', {'data-stat': 'percentile'})

            per90_val = per90_td.get_text(strip=True) if per90_td else None
            percentile_val = percentile_td.get_text(strip=True) if percentile_td else None

            # If this stat is mapped, store it
            if stat_name in MAPPING:
                db_col_per90, db_col_percentile = MAPPING[stat_name]

                # Convert per90_val to float if numeric
                if per90_val:
                    numeric_per90 = per90_val.replace("%", "")
                    if re.match(r"^-?\d+(\.\d+)?$", numeric_per90):
                        stats_dict[db_col_per90] = float(numeric_per90)
                    else:
                        stats_dict[db_col_per90] = None
                else:
                    stats_dict[db_col_per90] = None

                # Convert percentile_val to int if numeric
                if percentile_val and percentile_val.isdigit():
                    stats_dict[db_col_percentile] = int(percentile_val)
                else:
                    stats_dict[db_col_percentile] = None

    return stats_dict, compared_position, error_occurred


def main():
    # ---------------------------
    # Connect to the DB
    # ---------------------------
    try:
        conn = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
    except mysql.connector.Error as err:
        print(f"[ERROR] Could not connect to the database: {err}")
        return

    cursor = conn.cursor(dictionary=True)

    # Keep track of scraped line_ids in this run so we don't do duplicates
    scraped_line_ids = set()

    # -------------------------------------------------
    # 1) Get all rows from 'scout_reports_url' that are
    #    NOT already in 'scout_reports_done' via line_id.
    # -------------------------------------------------
    select_query = """
        SELECT sur.line_id,
               sur.player_id,
               sur.player,
               sur.squad,
               sur.primary_position
        FROM scout_reports_url sur
        LEFT JOIN scout_reports_done srd
               ON sur.line_id = srd.line_id
        WHERE srd.line_id IS NULL;
    """
    try:
        cursor.execute(select_query)
        rows = cursor.fetchall()
    except mysql.connector.Error as err:
        print(f"[ERROR] Failed to execute SELECT query: {err}")
        cursor.close()
        conn.close()
        return

    base_url = "https://fbref.com/en/players"

    for row in rows:
        line_id = row["line_id"]
        if line_id in scraped_line_ids:
            # Already scraped this line_id in this run, skip
            continue

        player_id = row["player_id"]
        player_name = row["player"]
        squad = row["squad"]
        primary_position = row["primary_position"]

        # Build the FBref URL
        safe_name = re.sub(r'\s+', '-', player_name.strip())
        url = f"{base_url}/{player_id}/scout/365_m1/{safe_name}-Scouting-Report"

        print(f"Scraping line_id={line_id} => {player_name} from URL: {url}")

        # Extract stats
        stats_dict, compared_position, error_occurred = extract_stats(url)

        # DNS/Connection error => Sleep 3:33, skip this line
        if error_occurred:
            print("[ALERT] DNS or connection error. Resting 3 minutes 33 seconds...\n")
            time.sleep(213)  # 3:33
            # Always do a final 10–15s sleep before next iteration
            delay = random.randint(10, 15)
            print(f"Waiting {delay}s before next row...\n")
            time.sleep(delay)
            continue

        # No stats => skip, but still do the final 10–15s sleep
        if not stats_dict:
            print(f"[WARNING] No data found for {player_name}, skipping...")
            delay = random.randint(10, 15)
            print(f"Waiting {delay}s before next row...\n")
            time.sleep(delay)
            scraped_line_ids.add(line_id)
            continue

        # Insert data into scout_reports
        # We'll store the player_id, player, squad, position (from primary_position),
        # and the compared_position that we scraped.
        stats_dict["player_id"] = player_id
        stats_dict["player"] = player_name
        stats_dict["squad"] = squad
        stats_dict["position"] = primary_position
        stats_dict["compared_position"] = compared_position

        cols = list(stats_dict.keys())
        colnames = ", ".join([f"`{c}`" for c in cols])
        placeholders = ", ".join(["%s"] * len(cols))
        update_clause = ", ".join([f"`{c}` = VALUES(`{c}`)" for c in cols if c != "player_id"])

        insert_sql = f"""
            INSERT INTO scout_reports ({colnames})
            VALUES ({placeholders})
            ON DUPLICATE KEY UPDATE
            {update_clause};
        """

        values = [stats_dict[c] for c in cols]

        try:
            cursor.execute(insert_sql, values)
            conn.commit()
            print(f"[SUCCESS] Insert/Update into scout_reports for {player_name}.")
        except mysql.connector.Error as err:
            conn.rollback()
            print(f"[ERROR] Insert/Update failed for {player_name}: {err}")
            # Sleep after a failure, but continue to next row
            delay = random.randint(10, 15)
            print(f"Waiting {delay}s before next row...\n")
            time.sleep(delay)
            scraped_line_ids.add(line_id)
            continue

        # ---------------------------------
        # Insert a record into scout_reports_done
        # ---------------------------------
        done_insert_sql = """
            INSERT INTO scout_reports_done (line_id, player_id, player, squad, position, url)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        done_values = (
            line_id,
            player_id,
            player_name,
            squad,
            primary_position,
            url
        )
        try:
            cursor.execute(done_insert_sql, done_values)
            conn.commit()
            print("[SUCCESS] Inserted into scout_reports_done.")
        except mysql.connector.Error as err:
            conn.rollback()
            print(f"[ERROR] Could not insert into scout_reports_done for line_id={line_id}: {err}")
            # If there's an error writing to done table, we still continue, but that row won't be flagged as done
            # so it may be retried next run (depending on your logic).

        # Mark this line_id as scraped so we don't attempt again
        scraped_line_ids.add(line_id)

        # Always sleep 10–15s after every request, even if successful
        delay = random.randint(10, 15)
        print(f"Waiting {delay}s before next row...\n")
        time.sleep(delay)

    # ---------------------------
    # Close
    # ---------------------------
    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()
