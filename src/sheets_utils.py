import os
import time
from datetime import datetime

import gspread
import pandas as pd
from gspread import Worksheet
from gspread_dataframe import get_as_dataframe, set_with_dataframe

from src.charts.chart_utils import country_name_to_code_dict
from src.logging_config import logger
from dotenv import load_dotenv
from tqdm import tqdm
load_dotenv()

def get_spreadsheet_with_gspread(
        url: str = "https://docs.google.com/spreadsheets/d/1IqBH8AOOrKgo9OXiVzniHP3XFu24lbPGvWT2yf3M-a0/edit?gid=1885596214#gid=1885596214") -> gspread.Spreadsheet:
    credentials_filename = os.getenv("CREDENTIALS_FILENAME")
    authorized_user_filename = os.getenv("AUTHORIZED_USER_FILENAME")
    gc = gspread.oauth(credentials_filename=credentials_filename, authorized_user_filename=authorized_user_filename)
    sheets_file = gc.open_by_url(url)
    print(f"Opened {sheets_file.title} with gspread")

    return sheets_file


SPREADSHEET = get_spreadsheet_with_gspread()


def get_inputs_worksheet_as_df() -> pd.DataFrame:
    worksheet = SPREADSHEET.worksheet("inputs")
    df = get_as_dataframe(worksheet)
    df = swap_column_names_to_first_row(df)
    return df


def order_worksheets_by_date():
    scrape_worksheets = [worksheet for worksheet in SPREADSHEET.worksheets() if
                         worksheet.title.startswith("chart_") or worksheet.title.startswith("playlist_")
                         or worksheet.title.startswith("general_")]

    notes = SPREADSHEET.worksheet("notes")
    inputs = SPREADSHEET.worksheet("inputs")
    label_watchlist = SPREADSHEET.worksheet("label_watchlist")

    sorted_worksheets = sorted(scrape_worksheets,
                               key=lambda x: datetime.strptime(x.title.split('_')[1], "%Y-%m-%d"), reverse=True)

    reordered_sheets = [notes, inputs, label_watchlist] + sorted_worksheets
    SPREADSHEET.reorder_worksheets(reordered_sheets)
    logger.info("Reordered worksheets by date")


def get_all_chart_and_playlist_worksheets_as_df() -> list[pd.DataFrame]:
    worksheets = SPREADSHEET.worksheets()

    # Filter out the chart and playlist worksheets
    result = []
    for worksheet in tqdm(worksheets):
        title = worksheet.title
        time.sleep(1)
        if title.startswith("chart_") or title.startswith("playlist_"):
            result.append(get_as_dataframe(worksheet))

    # Set the first row as column names
    dfs = [swap_column_names_to_first_row(df) for df in result]

    return dfs


def add_past_appearances_to_df(df: pd.DataFrame) -> pd.DataFrame:
    past_chart_and_playlist_dfs: list[pd.DataFrame] = get_all_chart_and_playlist_worksheets_as_df()
    sets_of_uuids: list[set[str]] = get_list_of_sets_of_song_uuids_from_dfs(past_chart_and_playlist_dfs)
    df["past_appearances"] = df["song_uuid"].apply(lambda x: check_uuid_appearances(x, sets_of_uuids))
    return df


def get_list_of_sets_of_song_uuids_from_dfs(dfs: list[pd.DataFrame]) -> list[set[str]]:
    result = []
    for df in dfs:
        if "song_uuid" in df.columns:
            uuids = set(df["song_uuid"].to_list())
            result.append(uuids)

    return result


def check_uuid_appearances(uuid: str, sets_of_uuids: list[set[str]]) -> int:
    appearances = 0
    for _set in sets_of_uuids:
        if uuid in _set:
            appearances += 1
            logger.debug(f"Found {uuid} in past appearance up to {appearances}")

    return appearances


def create_and_set_new_worksheet_to_df(title: str, df: pd.DataFrame) -> Worksheet:
    worksheets = SPREADSHEET.worksheets()

    # Check if the worksheet already exists
    worksheet_titles = [worksheet.title for worksheet in worksheets]

    if title not in worksheet_titles:
        SPREADSHEET.add_worksheet(title=title, rows=10, cols=10)
        logger.info(f"Created new worksheet: {title}")

    worksheet = SPREADSHEET.worksheet(title)
    set_with_dataframe(worksheet, df, resize=True)
    logger.info(f"Set worksheet: {title}")
    return worksheet


def get_watchlist_sheet_as_df_and_concat(watchlist_df_to_concat: pd.DataFrame,
                                         title: str = "label_watchlist") -> pd.DataFrame:
    worksheet = SPREADSHEET.worksheet(title)
    df = get_as_dataframe(worksheet)

    worksheet_watchlist_uuids = set(df["song_uuid"].to_list())
    new_watchlist_uuids = set(watchlist_df_to_concat["song_uuid"].to_list())
    new_watchlist_uuids = new_watchlist_uuids - worksheet_watchlist_uuids

    # Add the new watchlist songs to the worksheet
    if watchlist_df_to_concat is not None:
        watchlist_df_to_concat = watchlist_df_to_concat[watchlist_df_to_concat["song_uuid"].isin(new_watchlist_uuids)]
        df = pd.concat([df, watchlist_df_to_concat], ignore_index=True)

    # Set the new concatenated df to the worksheet
    set_with_dataframe(worksheet, df)

    return watchlist_df_to_concat


def add_spotify_playlist_link_at_top_of_worksheet(playlist_url: str, worksheet: Worksheet) -> None:
    worksheet.insert_row(["Spotify Playlist", playlist_url], 1)
    logger.info(f"Inserted playlist URL at top of worksheet: {playlist_url}")


def get_value_for_condition_and_assert(df: pd.DataFrame, condition: str) -> int:
    value = df[condition].iloc[0]
    assert isinstance(value, int), f"{condition} is not an integer"
    return value


def swap_column_names_to_first_row(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.iloc[0]  # Set the first row as column names
    df = df.drop(df.index[0])  # Remove the first row
    return df.reset_index(drop=True)  # Reset the index


def get_column_values_as_list_without_nan(df: pd.DataFrame, column_name: str) -> list:
    return df[column_name].dropna().to_list()


def replace_name_with_code_and_expand_all_countries(platform_genre_country_chart_tuples):
    """
    Expand the list of tuples to include all countries.
    """
    expanded_list = []
    for platform, genre, country in platform_genre_country_chart_tuples:
        if country == "All Countries":
            all_country_codes = list(country_name_to_code_dict.values())
            all_country_tuples = [(platform, genre, country_code) for country_code in all_country_codes]
            expanded_list.extend(all_country_tuples)

        else:
            new_tuple = (platform, genre, country_name_to_code_dict[country])
            expanded_list.append(new_tuple)

    return expanded_list


def drop_songs_that_appeared_in_past(result_df: pd.DataFrame) -> pd.DataFrame:
    # Drop rows that have appeared in the past
    past_chart_and_playlist_dfs: list[pd.DataFrame] = get_all_chart_and_playlist_worksheets_as_df()
    sets_of_uuids: list[set[str]] = get_list_of_sets_of_song_uuids_from_dfs(past_chart_and_playlist_dfs)
    past_uuids = set.union(*sets_of_uuids)
    result_df = result_df[~result_df["song_uuid"].isin(past_uuids)]
    return result_df
