import pandas as pd

from src.filters import apply_follower_stream_listeners_filters_and_drop_duplicates
from src.logging_config import logger
from src.sheets_utils import get_watchlist_sheet_as_df_and_concat

global_label_watchlist_df_list = []

watchlist_columns = [
    'date_added_to_watchlist',
    'song_uuid',
    'song_name',
    'url',
    'labels',
    'artists',
    'main_artist',
    'main_artist_uuid',
    'main_artist_spotify_followers',
    'main_artist_spotify_monthly_listeners',
    'main_artist_spotify_conversion_rate',
    'instrumentalness',
    'root_genres',
    'sub_genres',
    'release_date',
    'duration',
    'today_streams',
    'yesterday_streams',
    'day_1-3_average',
    'day_7-9_average',
    '%_increase',
    '14_day_max',
    '14_day_median',
    'total_streams',
    'main_artist_tiktok_followers'
]


def concat_filter_process_label_watchlist_df(watchlist_song_df_list) -> pd.DataFrame:
    if not watchlist_song_df_list:
        logger.info("No songs in watchlist")
        return pd.DataFrame()

    watchlist_df = pd.concat(watchlist_song_df_list, ignore_index=True)
    watchlist_df = apply_follower_stream_listeners_filters_and_drop_duplicates(watchlist_df)

    # Set the date_added_to_watchlist column to the current date
    watchlist_df["date_added_to_watchlist"] = pd.Timestamp.now().date()
    watchlist_df = watchlist_df[watchlist_columns]

    watchlist_df = get_watchlist_sheet_as_df_and_concat(watchlist_df)

    # DF containing new songs added to watchlist
    return watchlist_df
