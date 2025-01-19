import os
import re
import time
import random
import requests
from datetime import datetime
import mysql.connector
from mysql.connector import Error
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Database configuration
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

def get_db_connection():
    """Create and return a MySQL database connection."""
    return mysql.connector.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

def get_already_scraped_urls():
    """
    Fetch all distinct 'match_report_url' values from BOTH 'all_matchups_players'
    AND 'all_matchups_teams' so we can skip duplicates in either table.
    Return them as a set.
    """
    conn = None
    scraped_set = set()
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # 1) From all_matchups_players
        cursor.execute("SELECT DISTINCT match_report_url FROM all_matchups_players")
        rows_players = cursor.fetchall()
        for (url_val,) in rows_players:
            if url_val:
                scraped_set.add(url_val.strip())

        # 2) From all_matchups_teams
        cursor.execute("SELECT DISTINCT match_report_url FROM all_matchups_teams")
        rows_teams = cursor.fetchall()
        for (url_val,) in rows_teams:
            if url_val:
                scraped_set.add(url_val.strip())

        cursor.close()
    except Error as e:
        print("DB Error in get_already_scraped_urls =>", e)
    finally:
        if conn and conn.is_connected():
            conn.close()

    return scraped_set

def get_earliest_unscraped_date(scraped_urls):
    """
    Query the 'schedule' table in ascending date order for rows where:
      1) match_report IS NOT NULL,
      2) match_report is not in the 'scraped_urls' set,
      3) match_report doesn't match the "bad" URL pattern
         (the pattern is: /[0-9A-Za-z]{8}/[0-9A-Za-z]{8}/).
    Then return the *first* such date found.
    If none found, return None.
    """
    conn = None
    earliest_date = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        sql = """
            SELECT date, match_report
            FROM schedule
            WHERE match_report IS NOT NULL
            ORDER BY date ASC
        """
        cursor.execute(sql)
        rows = cursor.fetchall()
        cursor.close()

        bad_url_regex = re.compile(r'/[0-9A-Za-z]{8}/[0-9A-Za-z]{8}/')

        for (dt, report_url) in rows:
            if not report_url:
                continue

            # Already scraped => skip
            if report_url.strip() in scraped_urls:
                continue

            # Bad URL => skip
            if bad_url_regex.search(report_url.strip()):
                continue

            # Found the earliest unscraped, not-bad URL
            earliest_date = dt
            break

        return earliest_date

    except Error as e:
        print("DB Error in get_earliest_unscraped_date =>", e)
        return None
    finally:
        if conn and conn.is_connected():
            conn.close()

def fetch_schedule_since_date(start_date, scraped_urls):
    """
    Fetch rows from 'schedule' table that have a non-null match_report,
    only starting from 'start_date' inclusive, ordering by date ascending,
    and filtering out those whose match_report is already in 'scraped_urls'.
    
    Returns list of tuples: (date, home, away, comp, match_report).
    """
    if not start_date:
        return []

    conn = None
    result_rows = []
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        sql = """
            SELECT date, home, away, comp, match_report
            FROM schedule
            WHERE match_report IS NOT NULL
              AND date >= %s
            ORDER BY date ASC
        """
        cursor.execute(sql, (start_date,))
        rows = cursor.fetchall()
        cursor.close()

        # Filter out any that are already scraped
        for (dt, home, away, comp, url_) in rows:
            if not url_:
                continue
            if url_.strip() in scraped_urls:
                continue
            result_rows.append((dt, home, away, comp, url_))

        return result_rows

    except Error as e:
        print("DB Error =>", e)
        return []
    finally:
        if conn and conn.is_connected():
            conn.close()

def generate_insert_sql(table_name, columns):
    """
    Dynamically generate an INSERT SQL statement with placeholders based on columns.
    Includes ON DUPLICATE KEY UPDATE clause.
    """
    placeholders = ", ".join(["%s"] * len(columns))
    columns_joined = ", ".join(columns)
    update_clause = ", ".join([f"{col} = VALUES({col})" for col in columns])
    sql = f"""
        INSERT INTO {table_name} ({columns_joined})
        VALUES ({placeholders})
        ON DUPLICATE KEY UPDATE
            {update_clause}
    """
    return sql

def scrape_and_insert_match_data(match_date, home_team, away_team, comp, match_url):
    """
    Scrapes the match report from FBref, extracts team/player stats,
    applies fallback ID logic, handles substitution logic,
    uses the new approach to decide starter="yes"/"no",
    and then inserts/updates data into:
      - all_matchups_teams
      - all_matchups_players
      => THEN scrapes the shot events table and inserts into 'match_events'.

    NOTE: We do all DB insertion in one go (same connection), so if any part fails,
    we can rollback. We also reuse the same 'soup' object to avoid re-requesting.
    """

    # Convert date to string if needed
    if isinstance(match_date, datetime):
        date_str = match_date.strftime("%Y-%m-%d")
    else:
        date_str = str(match_date)

    matchup_str = f"{away_team} @ {home_team}"

    # Ensure URL has "https://"
    if match_url.startswith("fbref.com"):
        match_url = "https://" + match_url

    # ==================== REQUEST PAGE ====================
    try:
        resp = requests.get(match_url)
        resp.raise_for_status()
    except requests.RequestException as ex:
        print(f"Request error => {ex}")
        return

    soup = BeautifulSoup(resp.text, "html.parser")

    # Check for .scorebox
    scorebox = soup.find('div', class_='scorebox')
    if not scorebox:
        print("No .scorebox => skipping match.")
        return

    # Regex utility
    def extract_team_id(hval):
        m = re.search(r'/en/squads/([^/]+)/', hval)
        return m.group(1) if m else "0000000000"

    # Collect team IDs from the scorebox
    a_tags = scorebox.find_all('a', href=re.compile(r'^/en/squads/'))
    found_team_map = {}
    for a_tag in a_tags:
        t_name = a_tag.get_text(strip=True)
        t_id = extract_team_id(a_tag['href'])
        found_team_map[t_name] = t_id

    def safe_lookup(name_):
        if name_ in found_team_map:
            return found_team_map[name_]
        for k, v in found_team_map.items():
            if name_.lower() in k.lower():
                return v
        return "0000000000"

    home_team_id = safe_lookup(home_team)
    away_team_id = safe_lookup(away_team)

    # Force certain known IDs
    if home_team == "Wolves":
        home_team_id = "8cec06e1"
    if away_team == "Wolves":
        away_team_id = "8cec06e1"
    if home_team == "Manchester Utd":
        home_team_id = "19538871"
    if away_team == "Manchester Utd":
        away_team_id = "19538871"
    if home_team == "Newcastle Utd":
        home_team_id = "b2b47a98"
    if away_team == "Newcastle Utd":
        away_team_id = "b2b47a98"

    team_ids_found = set(found_team_map.values())

    # If exactly one team matched, attempt fallback for the other
    if home_team_id != "0000000000" and away_team_id == "0000000000":
        leftover = team_ids_found - {home_team_id}
        if len(leftover) == 1:
            away_team_id = leftover.pop()
        else:
            print(f"Warning => Fallback not possible for away_team. leftover IDs: {leftover}")

    elif away_team_id != "0000000000" and home_team_id == "0000000000":
        leftover = team_ids_found - {away_team_id}
        if len(leftover) == 1:
            home_team_id = leftover.pop()
        else:
            print(f"Warning => Fallback not possible for home_team. leftover IDs: {leftover}")

    if home_team_id == "0000000000" and away_team_id == "0000000000":
        print("Warning => Both teams failed to match. Team IDs defaulted to 0000000000.")

    if len(team_ids_found) > 2:
        print(f"Warning => More than two unique team IDs found: {team_ids_found}")

    # ============ SCOREBOX LOGIC ============
    sc_divs = scorebox.find_all("div", class_="score")
    if len(sc_divs) == 2:
        home_score = sc_divs[0].get_text(strip=True)
        away_score = sc_divs[1].get_text(strip=True)
    else:
        home_score, away_score = "0", "0"
    final_score = f"{home_score} : {away_score}"

    # ============ POSSESSION ============
    home_pos = "0%"
    away_pos = "0%"
    team_stats_div = soup.find('div', id='team_stats')
    if team_stats_div:
        poss_th = team_stats_div.find('th', colspan='2', string="Possession")
        if poss_th:
            parent_tr = poss_th.find_parent('tr')
            if parent_tr:
                nxt_tr = parent_tr.find_next_sibling('tr')
                if nxt_tr:
                    tds_ = nxt_tr.find_all('td')
                    if len(tds_) == 2:
                        s_home = tds_[0].find('strong')
                        s_away = tds_[1].find('strong')
                        if s_home: home_pos = s_home.get_text(strip=True)
                        if s_away: away_pos = s_away.get_text(strip=True)

    # ============ FORMATIONS ============
    home_formation = "Unknown"
    away_formation = "Unknown"
    home_lineup_div = soup.find('div', class_='lineup', id='a')
    away_lineup_div = soup.find('div', class_='lineup', id='b')
    if home_lineup_div and away_lineup_div:
        home_formation_th = home_lineup_div.find('th', colspan='2')
        away_formation_th = away_lineup_div.find('th', colspan='2')
        if home_formation_th and away_formation_th:
            home_formation_text = home_formation_th.get_text(strip=True)
            away_formation_text = away_formation_th.get_text(strip=True)

            home_formation_match = re.search(r'\(([^)]+)\)', home_formation_text)
            away_formation_match = re.search(r'\(([^)]+)\)', away_formation_text)
            home_formation = home_formation_match.group(1) if home_formation_match else "Unknown"
            away_formation = away_formation_match.group(1) if away_formation_match else "Unknown"

    # ================= PLAYER STAT MAP =================
    player_stat_map = {
        "shots":                      "shots",
        "shots_on_target":            "shots_on_target",
        "fouls":                      "fouls",
        "corner_kicks":               "corner_kicks",
        "crosses":                    "crosses",
        "touches":                    "touches",
        "tackles":                    "tackles",
        "interceptions":              "interceptions",
        "passes":                     "passes",
        "assisted_shots":             "shots_assisted",
        "take_ons":                   "take_ons",
        "progressive_carries":        "progressive_carries",
        "clearances":                 "clearances",
        "tackles_def_3rd":            "tackles_def_3rd",
        "tackles_mid_3rd":            "tackles_mid_3rd",
        "tackles_att_3rd":            "tackles_att_3rd",
        "blocked_shots":              "blocked_shots",
        "blocked_passes":             "blocked_passes",
        "challenges":                 "challenges",
        "carries_into_final_third":   "carries_into_final_third",
        "progressive_passes_received":"progressive_passes_received",
        "passes_into_final_third":    "passes_into_final_third",
        "passes_into_penalty_area":   "passes_into_penalty_area",
        "touches_def_3rd":            "touches_def_3rd",
        "touches_mid_3rd":            "touches_mid_3rd",
        "touches_att_3rd":            "touches_att_3rd",
        "touches_att_pen_area":       "touches_att_pen_area",
        "sca":                        "sca"
    }

    # Initialize player dicts
    home_players = {}
    away_players = {}

    # Initialize team sums
    team_sums = {}
    for db_col_ in player_stat_map.values():
        team_sums[f"home_{db_col_}"] = 0
        team_sums[f"away_{db_col_}"] = 0

    def init_player_dict(pid_, pname_):
        d_ = {
            "player_id": pid_,
            "player": pname_,
            "position": "N/A",
            "minutes": 0,
            "starter": "no",
            "sub_in_out": "n/a"
        }
        for dbcol in player_stat_map.values():
            d_[dbcol] = 0
        return d_

    # ============== Parse Stats Tables ==============
    # (summary, passing, defense, possession, misc, passing_types)
    stats_tables = soup.find_all('table', id=re.compile(r'^stats_.*_(summary|passing|defense|possession|misc|passing_types)$'))

    for tbl in stats_tables:
        tid_ = tbl.get("id","")
        m_ = re.search(r'^stats_([^_]+)_', tid_)
        if not m_:
            continue
        code_ = m_.group(1)

        if code_ == home_team_id:
            is_home = True
        elif code_ == away_team_id:
            is_home = False
        else:
            continue

        tbody_ = tbl.find('tbody')
        if not tbody_:
            continue

        rows_ = tbody_.find_all('tr')
        for row_ in rows_:
            pth_ = row_.find('th', {'data-stat':'player'})
            if not pth_:
                continue
            a_ = pth_.find('a', href=re.compile(r'^/en/players/'))
            if not a_:
                continue

            href_ = a_.get('href', '')
            mm_ = re.search(r'/en/players/([^/]+)/', href_)
            pidv = mm_.group(1) if mm_ else "0000000000"
            pname_ = a_.get_text(strip=True)

            if is_home:
                if pidv not in home_players:
                    home_players[pidv] = init_player_dict(pidv, pname_)
                pdict = home_players[pidv]
            else:
                if pidv not in away_players:
                    away_players[pidv] = init_player_dict(pidv, pname_)
                pdict = away_players[pidv]

            # Populate stats
            for fbref_stat, dbcol in player_stat_map.items():
                cell_ = row_.find('td', {'data-stat': fbref_stat})
                if cell_:
                    val_ = cell_.get_text(strip=True)
                    if fbref_stat == "sca":
                        try:
                            pdict[dbcol] = float(val_)
                        except:
                            pdict[dbcol] = 0.0
                    else:
                        try:
                            pdict[dbcol] = int(val_)
                        except:
                            pdict[dbcol] = 0

            # Position
            pos_cell = row_.find('td', {'data-stat':'position'})
            if pos_cell:
                pdict["position"] = pos_cell.get_text(strip=True) or "N/A"

            # Minutes
            min_cell = row_.find('td', {'data-stat':'minutes'})
            if min_cell:
                try:
                    pdict["minutes"] = int(min_cell.get_text(strip=True))
                except:
                    pdict["minutes"] = 0

    # ============== Substitution Logic ==============
    home_subs_val = 0
    away_subs_val = 0
    events_wrap = soup.find('div', id='events_wrap')
    if events_wrap:
        event_divs = events_wrap.find_all('div', class_=re.compile(r'^event'))
        for event_div in event_divs:
            icon_div = event_div.find('div', class_=re.compile(r'event_icon'))
            if not icon_div:
                continue

            icon_classes = icon_div.get('class', [])
            if 'substitute_in' in icon_classes:
                # figure out which team made the substitution
                event_class = event_div.get('class', [])
                if 'a' in event_class:
                    substitution_team = 'home'
                elif 'b' in event_class:
                    substitution_team = 'away'
                else:
                    substitution_team = None

                # parse the minute if present
                minute_divs = event_div.find_all('div', recursive=False)
                minute = 0
                if len(minute_divs) > 0:
                    min_text = minute_divs[0].get_text(strip=True)
                    minute_match = re.search(r'(\d+)’', min_text)
                    if minute_match:
                        minute = minute_match.group(1)

                info_div = icon_div.find_next_sibling('div')
                if info_div:
                    player_in_tag = info_div.find('a')
                    player_out_tag = None
                    small_tag = info_div.find('small')
                    if small_tag:
                        player_out_tag = small_tag.find('a')

                    if player_in_tag and player_out_tag:
                        href_in = player_in_tag.get('href', '')
                        mm_in = re.search(r'/en/players/([^/]+)/', href_in)
                        player_in_id = mm_in.group(1) if mm_in else None

                        href_out = player_out_tag.get('href', '')
                        mm_out = re.search(r'/en/players/([^/]+)/', href_out)
                        player_out_id = mm_out.group(1) if mm_out else None

                        if substitution_team == 'home':
                            if player_in_id in home_players:
                                home_players[player_in_id]['sub_in_out'] = f"In | {minute}’"
                            if player_out_id in home_players:
                                home_players[player_out_id]['sub_in_out'] = f"Out | {minute}’"
                            home_subs_val += 1
                        elif substitution_team == 'away':
                            if player_in_id in away_players:
                                away_players[player_in_id]['sub_in_out'] = f"In | {minute}’"
                            if player_out_id in away_players:
                                away_players[player_out_id]['sub_in_out'] = f"Out | {minute}’"
                            away_subs_val += 1
    else:
        print("No events_wrap found; skipping substitutions.")

    # ============== Determine "starter" ==============
    def assign_starter_flag(pdict):
        mins = pdict["minutes"]
        sub_info = pdict["sub_in_out"].lower()  # e.g. "In | 45’", "Out | 62’", "n/a"

        # Rule 1) If minutes < 90 and "out" => yes
        if mins < 90 and "out" in sub_info:
            return "yes"
        # Rule 2) If minutes < 90 and "in" => no
        if mins < 90 and "in" in sub_info and "out" not in sub_info:
            return "no"
        # Rule 3) If minutes >= 90 and no sub => yes
        if mins >= 90 and sub_info == "n/a":
            return "yes"
        return "no"

    for pid_, pvals_ in home_players.items():
        home_players[pid_]["starter"] = assign_starter_flag(pvals_)
    for pid_, pvals_ in away_players.items():
        away_players[pid_]["starter"] = assign_starter_flag(pvals_)

    # Sum up team stats
    def sum_side(pmap, prefix):
        for pv in pmap.values():
            for dbcol in player_stat_map.values():
                team_sums[f"{prefix}_{dbcol}"] += pv[dbcol]

    sum_side(home_players, "home")
    sum_side(away_players, "away")

    home_sca_val = int(round(sum(x["sca"] for x in home_players.values())))
    away_sca_val = int(round(sum(x["sca"] for x in away_players.values())))

    print(f"\nDEBUG => date={date_str}, {home_team} vs {away_team}, comp={comp}")
    print(f"home_team_id={home_team_id}, away_team_id={away_team_id}, score={home_score}:{away_score}, possession={home_pos}-{away_pos}")
    print(f"home_formation={home_formation}, away_formation={away_formation}")
    print(f"home_subs={home_subs_val}, away_subs={away_subs_val}")
    print("----------- HOME PLAYERS -----------")
    for k, v in home_players.items():
        print(f"{k}: {v['player']} => mins={v['minutes']}, sub={v['sub_in_out']}, starter={v['starter']}")
    print("----------- AWAY PLAYERS -----------")
    for k, v in away_players.items():
        print(f"{k}: {v['player']} => mins={v['minutes']}, sub={v['sub_in_out']}, starter={v['starter']}")

    # =================================================
    # === DB INSERTION for TEAMS and PLAYERS FIRST ====
    # =================================================
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # 1) Insert/Update all_matchups_teams
        teams_columns = [
            "date", "matchup", "comp",
            "home_squad", "away_squad",
            "score",
            "home_formation", "away_formation",
            "home_possession", "away_possession",
            "home_subs", "away_subs",
            "home_shots", "away_shots",
            "home_shots_on_target", "away_shots_on_target",
            "home_fouls", "away_fouls",
            "home_corner_kicks", "away_corner_kicks",
            "home_crosses", "away_crosses",
            "home_touches", "away_touches",
            "home_tackles", "away_tackles",
            "home_interceptions", "away_interceptions",
            "home_passes", "away_passes",
            "home_shots_assisted", "away_shots_assisted",
            "home_take_ons", "away_take_ons",
            "home_progressive_carries", "away_progressive_carries",
            "home_clearances", "away_clearances",
            "home_tackles_def_3rd", "away_tackles_def_3rd",
            "home_tackles_mid_3rd", "away_tackles_mid_3rd",
            "home_tackles_att_3rd", "away_tackles_att_3rd",
            "home_blocked_shots", "away_blocked_shots",
            "home_blocked_passes", "away_blocked_passes",
            "home_challenges", "away_challenges",
            "home_carries_into_final_third", "away_carries_into_final_third",
            "home_progressive_passes_received", "away_progressive_passes_received",
            "home_passes_into_final_third", "away_passes_into_final_third",
            "home_passes_into_penalty_area", "away_passes_into_penalty_area",
            "home_touches_def_3rd", "away_touches_def_3rd",
            "home_touches_mid_3rd", "away_touches_mid_3rd",
            "home_touches_att_3rd", "away_touches_att_3rd",
            "home_touches_att_pen_area", "away_touches_att_pen_area",
            "match_report_url",
            "home_sca", "away_sca",
            "home_team_id", "away_team_id"
        ]

        insert_teams_sql = generate_insert_sql("all_matchups_teams", teams_columns)
        teams_param = (
            date_str, matchup_str, comp,
            home_team, away_team,
            final_score,
            home_formation, away_formation,
            home_pos, away_pos,
            home_subs_val, away_subs_val,
            team_sums["home_shots"], team_sums["away_shots"],
            team_sums["home_shots_on_target"], team_sums["away_shots_on_target"],
            team_sums["home_fouls"], team_sums["away_fouls"],
            team_sums["home_corner_kicks"], team_sums["away_corner_kicks"],
            team_sums["home_crosses"], team_sums["away_crosses"],
            team_sums["home_touches"], team_sums["away_touches"],
            team_sums["home_tackles"], team_sums["away_tackles"],
            team_sums["home_interceptions"], team_sums["away_interceptions"],
            team_sums["home_passes"], team_sums["away_passes"],
            team_sums["home_shots_assisted"], team_sums["away_shots_assisted"],
            team_sums["home_take_ons"], team_sums["away_take_ons"],
            team_sums["home_progressive_carries"], team_sums["away_progressive_carries"],
            team_sums["home_clearances"], team_sums["away_clearances"],
            team_sums["home_tackles_def_3rd"], team_sums["away_tackles_def_3rd"],
            team_sums["home_tackles_mid_3rd"], team_sums["away_tackles_mid_3rd"],
            team_sums["home_tackles_att_3rd"], team_sums["away_tackles_att_3rd"],
            team_sums["home_blocked_shots"], team_sums["away_blocked_shots"],
            team_sums["home_blocked_passes"], team_sums["away_blocked_passes"],
            team_sums["home_challenges"], team_sums["away_challenges"],
            team_sums["home_carries_into_final_third"], team_sums["away_carries_into_final_third"],
            team_sums["home_progressive_passes_received"], team_sums["away_progressive_passes_received"],
            team_sums["home_passes_into_final_third"], team_sums["away_passes_into_final_third"],
            team_sums["home_passes_into_penalty_area"], team_sums["away_passes_into_penalty_area"],
            team_sums["home_touches_def_3rd"], team_sums["away_touches_def_3rd"],
            team_sums["home_touches_mid_3rd"], team_sums["away_touches_mid_3rd"],
            team_sums["home_touches_att_3rd"], team_sums["away_touches_att_3rd"],
            team_sums["home_touches_att_pen_area"], team_sums["away_touches_att_pen_area"],
            match_url,
            home_sca_val, away_sca_val,
            home_team_id, away_team_id
        )
        cursor.execute(insert_teams_sql, teams_param)

        # 2) Insert/Update all_matchups_players
        players_columns = [
            "date", "matchup", "comp",
            "player", "player_id", "position", "squad", "opponent", "home_away",
            "minutes", "starter",
            "shots", "shots_on_target", "fouls", "corner_kicks", "crosses",
            "touches", "tackles", "interceptions",
            "passes", "shots_assisted", "take_ons", "progressive_carries",
            "clearances",
            "tackles_def_3rd", "tackles_mid_3rd", "tackles_att_3rd",
            "blocked_shots", "blocked_passes", "challenges",
            "carries_into_final_third", "progressive_passes_received",
            "passes_into_final_third", "passes_into_penalty_area",
            "touches_def_3rd", "touches_mid_3rd", "touches_att_3rd", "touches_att_pen_area",
            "sca",
            "sub_in_out",
            "match_report_url"
        ]
        insert_players_sql = generate_insert_sql("all_matchups_players", players_columns)

        def insert_players(p_map, squad_name, opp_name, home_away_val):
            for _, pdict in p_map.items():
                param_player = (
                    date_str, matchup_str, comp,
                    pdict["player"], pdict["player_id"], pdict["position"],
                    squad_name, opp_name, home_away_val,
                    pdict["minutes"], pdict["starter"],
                    pdict["shots"], pdict["shots_on_target"], pdict["fouls"],
                    pdict["corner_kicks"], pdict["crosses"], pdict["touches"],
                    pdict["tackles"], pdict["interceptions"], pdict["passes"],
                    pdict["shots_assisted"], pdict["take_ons"], pdict["progressive_carries"],
                    pdict["clearances"],
                    pdict["tackles_def_3rd"], pdict["tackles_mid_3rd"], pdict["tackles_att_3rd"],
                    pdict["blocked_shots"], pdict["blocked_passes"], pdict["challenges"],
                    pdict["carries_into_final_third"], pdict["progressive_passes_received"],
                    pdict["passes_into_final_third"], pdict["passes_into_penalty_area"],
                    pdict["touches_def_3rd"], pdict["touches_mid_3rd"],
                    pdict["touches_att_3rd"], pdict["touches_att_pen_area"],
                    pdict["sca"],
                    pdict["sub_in_out"],
                    match_url
                )
                cursor.execute(insert_players_sql, param_player)

        # Insert home players
        insert_players(home_players, home_team, away_team, "home")
        # Insert away players
        insert_players(away_players, away_team, home_team, "away")

        # =================================================
        # === NOW SCRAPE & INSERT SHOT EVENTS (match_events)
        # =================================================
        # We'll reuse the same `soup` and `match_url` but define a helper function:

        def get_value_from_td_or_th(row_, data_stat):
            """Return the stripped text from either <td> or <th> with data-stat, else None."""
            tag_ = row_.find(lambda t: t.name in ["td","th"] and t.get("data-stat") == data_stat)
            return tag_.get_text(strip=True) if tag_ else None

        def get_player_id(row_, data_stat):
            """
            Get the player's ID from 'data-append-csv' or the anchor's href 
            in <td>/<th> with data-stat="...".
            """
            tag_ = row_.find(lambda t: t.name in ["td","th"] and t.get("data-stat") == data_stat)
            if not tag_:
                return None
            append_csv_ = tag_.get("data-append-csv")
            if append_csv_:
                return append_csv_.strip()
            a_tag_ = tag_.find("a", href=True)
            if a_tag_:
                href_val = a_tag_["href"]
                parts_ = href_val.split("/")
                if len(parts_) > 3:
                    return parts_[3]
            return None

        # Shots table is "shots_all"
        shots_table = soup.find("table", {"id": "shots_all"})
        if shots_table:
            tbody_el = shots_table.find("tbody")
            if tbody_el:
                rows__ = tbody_el.find_all("tr", recursive=False)
                if rows__:
                    insert_events_sql = """
                        INSERT INTO match_events (
                            match_report_url,
                            minute,
                            player_id,
                            player,
                            squad,
                            outcome,
                            distance,
                            body_part,
                            sca_1_player_id,
                            sca_1_player,
                            sca_1_event,
                            sca_2_player_id,
                            sca_2_player,
                            sca_2_event
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """

                    data_batch = []
                    shot_rows_parsed = 0
                    for row__ in rows__:
                        row_class = row__.get("class", [])
                        if "spacer" in row_class or "partial_table" in row_class:
                            # Skip these filler rows
                            continue

                        minute_val = get_value_from_td_or_th(row__, "minute")
                        player_val = get_value_from_td_or_th(row__, "player")
                        squad_val  = get_value_from_td_or_th(row__, "team")
                        outcome_val = get_value_from_td_or_th(row__, "outcome")
                        distance_val = get_value_from_td_or_th(row__, "distance")
                        body_part_val = get_value_from_td_or_th(row__, "body_part")
                        sca_1_player  = get_value_from_td_or_th(row__, "sca_1_player")
                        sca_1_event   = get_value_from_td_or_th(row__, "sca_1_type")
                        sca_2_player  = get_value_from_td_or_th(row__, "sca_2_player")
                        sca_2_event   = get_value_from_td_or_th(row__, "sca_2_type")

                        player_id_val      = get_player_id(row__, "player")
                        sca_1_player_id_val= get_player_id(row__, "sca_1_player")
                        sca_2_player_id_val= get_player_id(row__, "sca_2_player")

                        # Basic skip check
                        if not minute_val or not player_val:
                            continue

                        data_batch.append((
                            match_url,
                            minute_val,
                            player_id_val,
                            player_val,
                            squad_val,
                            outcome_val,
                            distance_val,
                            body_part_val,
                            sca_1_player_id_val,
                            sca_1_player,
                            sca_1_event,
                            sca_2_player_id_val,
                            sca_2_player,
                            sca_2_event
                        ))
                        shot_rows_parsed += 1

                    if data_batch:
                        cursor.executemany(insert_events_sql, data_batch)
                        print(f"Inserted shot events => {shot_rows_parsed} rows into match_events.")
                    else:
                        print("No valid shot rows found in shots_all table.")
                else:
                    print("No <tr> rows in shots_all table.")
            else:
                print("No <tbody> in shots_all table.")
        else:
            print("shots_all table not found => skipping shot events insertion.")

        # Commit after all inserts
        conn.commit()
        cursor.close()
        conn.close()

        print(f"\nInserted => {date_str} => {matchup_str}\n")

    except Error as e:
        print("DB Insert Error =>", e)
        if conn and conn.is_connected():
            conn.rollback()
    finally:
        if conn and conn.is_connected():
            conn.close()

def main():
    """
    1) Get the set of already-scraped URLs from BOTH all_matchups_teams AND all_matchups_players.
    2) Find the earliest date from 'schedule' that has a match_report not in that set
       and not a "bad" URL.
    3) Fetch schedule rows from that date onward.
    4) For each row, if the URL is "bad" => stop. Otherwise => scrape + 
       do (teams + players) insertion first, and then insert shot events.
    """

    # 1) Get all already-scraped URLs
    scraped_urls = get_already_scraped_urls()

    # 2) Find earliest unscraped date
    earliest_date = get_earliest_unscraped_date(scraped_urls)
    if not earliest_date:
        print("No unscraped matches found => done.")
        return

    # 3) Fetch schedule from that earliest date onward
    rows = fetch_schedule_since_date(earliest_date, scraped_urls)
    if not rows:
        print("No matches => done.")
        return

    # 4) Process each row
    bad_url_regex = re.compile(r'/[0-9A-Za-z]{8}/[0-9A-Za-z]{8}/')
    for (mdate, hteam, ateam, c_, url_) in rows:
        if not url_:
            continue

        # Check if the URL has two 8-character codes in the path
        if bad_url_regex.search(url_.strip()):
            print(f"Encountered a URL with two 8-character codes => {url_}")
            print("Stopping script as instructed...")
            break

        print(f"\nProcessing => {mdate}, {hteam} vs {ateam}")
        scrape_and_insert_match_data(mdate, hteam, ateam, c_, url_)

        # A small random pause to avoid hammering the site
        time.sleep(random.randint(7, 11))

    print("\nAll done!")

if __name__ == "__main__":
    main()
