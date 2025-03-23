import os
from datetime import datetime

import pandas as pd
from gspread import Worksheet

from src.sheets_utils import order_worksheets_by_date, create_and_set_new_worksheet_to_df, \
    add_spotify_playlist_link_at_top_of_worksheet
from src.spotify_playlister import create_playlist_on_spotify_for_songs_in_df
from src.utils import convert_dataframe_to_csv
from dotenv import load_dotenv

load_dotenv()


def process_scrape_output(results: pd.DataFrame, type_of_scrape: str, date: str = None) -> tuple[
    str, pd.DataFrame, str] | tuple[None, None, None]:
    """
    Takes a result df from a scrape, converts it to a csv, adds to a new google sheet and creates a spotify playlist
    """
    if not date:
        date: str = str(datetime.now().date())

    if len(results) == 0:
        return None, None, None

    new_sheet_name = f"{type_of_scrape}_{date}"
    OUTPUT_FOLDER = os.getenv("OUTPUT_FOLDER")
    convert_dataframe_to_csv(results, filename=f"{OUTPUT_FOLDER}/{new_sheet_name}.csv")

    worksheet: Worksheet = create_and_set_new_worksheet_to_df(new_sheet_name, results)
    spotify_playlist = create_playlist_on_spotify_for_songs_in_df(results, f"{type_of_scrape}_{date}")
    add_spotify_playlist_link_at_top_of_worksheet(spotify_playlist, worksheet)
    order_worksheets_by_date()
    return new_sheet_name, results, spotify_playlist
