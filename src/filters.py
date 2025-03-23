import os
import string
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd
from dotenv import load_dotenv

from src import input_lists
from src.credentials_key_info import BASE_API_URL, credentials
from src.logging_config import logger
from src.session_manager import session

load_dotenv()


def get_today_streams(stream_data_df):
    try:
        today_streams = stream_data_df.iloc[0]["daily_streams"]
    except IndexError:
        today_streams = np.nan
    return today_streams


def get_yesterday_streams(stream_data_df):
    try:
        yesterday_streams = stream_data_df.iloc[1]["daily_streams"]
    except IndexError:
        yesterday_streams = np.nan
    return yesterday_streams


def get_total_streams(stream_data_df):
    try:
        last_14_days = stream_data_df[:14]
        last_2_weeks_metrics = last_14_days.describe()
        last_2_weeks_metrics = last_2_weeks_metrics.to_dict()
        total_streams = last_2_weeks_metrics["total_streams"]["max"]
    except IndexError:
        total_streams = np.nan
    return total_streams


def x_percent_of_streams_are_from_one_day_in_last_14_days(stream_data_df, x):
    last_14_days_df = stream_data_df[:14]
    last_14_days_df = last_14_days_df[last_14_days_df["daily_streams"] > 0]

    if last_14_days_df.empty:
        return False

    total_streams = get_total_streams(stream_data_df)

    x = x / 100
    if last_14_days_df["daily_streams"].max() > (total_streams * x):
        return True

    return False


def get_14_day_daily_max_median(stream_data_df):
    last_2_weeks_metrics = stream_data_df[:14].describe().to_dict()
    fourteen_day_max = last_2_weeks_metrics["daily_streams"]["max"]
    fourteen_day_median = last_2_weeks_metrics["daily_streams"]["50%"]
    return fourteen_day_max, fourteen_day_median


def get_last_week_3_day_avg(stream_data_df):
    last_week_3_day_sample = stream_data_df[6:9]
    last_week_metrics = last_week_3_day_sample.describe().to_dict()
    last_week_3_day_avg = last_week_metrics["daily_streams"]["mean"]
    return last_week_3_day_avg


def get_last_week_7_day_avg(stream_data_df):
    last_week_7_day_sample = stream_data_df[6:13]
    last_week_metrics = last_week_7_day_sample.describe().to_dict()
    last_week_7_day_avg = last_week_metrics["daily_streams"]["mean"]
    return last_week_7_day_avg


def get_this_week_7_day_avg(stream_data_df):
    this_week_7_day_sample = stream_data_df[:7]
    this_week_metrics = this_week_7_day_sample.describe().to_dict()
    this_week_7_day_avg = this_week_metrics["daily_streams"]["mean"]
    return this_week_7_day_avg


def get_increase_between_avg(three_day_average, last_week_3_day_avg):
    if not last_week_3_day_avg or last_week_3_day_avg < 1:
        increase_between_avg = np.nan
    else:
        increase_between_avg = ((three_day_average - last_week_3_day_avg) / last_week_3_day_avg) * 100
    return increase_between_avg


def get_three_day_average_from_stream_df(stream_data_df):
    """Ensure that the df is not empty before using it"""
    if stream_data_df.empty:
        return 0

    three_day_average = stream_data_df.iloc[1:4]["daily_streams"].to_list()
    three_day_average_list = [x for x in three_day_average if x > 0][:3]

    if not three_day_average_list:
        return 0

    three_day_average = np.mean(three_day_average_list)

    return int(three_day_average)


def signed_to_banned_label(song_label_list: list) -> bool:
    exclusion_list = input_lists.label_blocklist
    exclusion_list = [x.lower().strip() for x in exclusion_list]
    song_label_list = [x.lower().strip() for x in song_label_list]

    # check if any of the labels are contained in any string in the exclusion list
    for excluded_label in exclusion_list:
        if any(excluded_label == label for label in song_label_list):
            return True

    return False


def is_english(text: str):
    """
    Check if a string contains only English letters, numbers, and basic punctuation.

    Args:
        text (str): The string to check

    Returns:
        bool: True if string contains only allowed characters, False otherwise

    Allowed characters include:
    - ASCII letters (a-z, A-Z)
    - Numbers (0-9)
    - Common punctuation marks
    - Whitespace
    """
    if not isinstance(text, str):
        return False

    # Create set of allowed characters
    allowed_chars = set(string.ascii_letters +  # a-zA-Z
                        string.digits +  # 0-9
                        string.punctuation +  # !"#$%&'()*+,-./:;<=>?@[\]^_`{|}~
                        string.whitespace)  # space, tab, newline

    # Check if any character in text is not in allowed_chars
    return all(char in allowed_chars for char in text)


def log_dropped_rows(pre_df: pd.DataFrame,
                     post_df: pd.DataFrame,
                     filter_name: str,
                     uuid_column: str = 'song_uuid') -> set:
    """
    Logs each dropped UUID with its filter name.

    Args:
        pre_df: DataFrame before filtering
        post_df: DataFrame after filtering
        filter_name: Name of the filter being applied
        uuid_column: Name of the UUID column
    """
    pre_uuids = set(pre_df[uuid_column])
    post_uuids = set(post_df[uuid_column])

    dropped_uuids = pre_uuids - post_uuids

    for uuid in dropped_uuids:
        logger.debug(f"Song {uuid} dropped due to {filter_name} filter")

    # Append the rows that were droppped to min_streams_dropped.csv
    if filter_name == "min_streams_if_above_0_average":
        dropped_uuids_df = pre_df[pre_df[uuid_column].isin(dropped_uuids)]

        OUTPUT_FOLDER = os.getenv("OUTPUT_FOLDER")
        dropped_uuids_df.to_csv(f"{OUTPUT_FOLDER}/min_streams_dropped.csv", mode='a')

    return dropped_uuids


def fetch_tiktok_followers_threaded(df):
    """
    Fetch TikTok followers using threading without batching.
    """
    unique_artists = df['main_artist_uuid'].unique()
    results = {}

    with ThreadPoolExecutor(max_workers=10) as executor:
        # Create future tasks for each unique artist
        future_to_artist = {
            executor.submit(get_artist_audience, artist_id, "tiktok"): artist_id
            for artist_id in unique_artists
        }

        # Process completed tasks as they finish
        for future in as_completed(future_to_artist):
            artist_id = future_to_artist[future]
            try:
                followers = future.result()
                results[artist_id] = followers
            except Exception as e:
                print(f"Error fetching followers for {artist_id}: {e}")
                results[artist_id] = None

    # Map results back to dataframe
    return df['main_artist_uuid'].map(results)


def apply_follower_stream_listeners_filters_and_drop_duplicates(df):
    try:
        # Replace all NaN values with 0
        df = df.fillna(0)

        stream_filtered_df = df[(df["total_streams"] <= input_lists.max_streams)]
        log_dropped_rows(df, stream_filtered_df, "max_streams")

        follower_filtered_df = stream_filtered_df[
            (stream_filtered_df["main_artist_spotify_followers"] <= input_lists.max_spotify_followers)]
        log_dropped_rows(stream_filtered_df, follower_filtered_df, "max_spotify_followers")

        # For rows with less than 100k monthly listeners, remove rows with less than input_lists.min followers
        minimum_spotify_followers_if_100k_monthly_listeners_filtered_df = follower_filtered_df[
            # Either: monthly listeners < 100k (keep these rows untouched)
            (follower_filtered_df["main_artist_spotify_monthly_listeners"] < 100000)
            |
            # OR: monthly listeners >= 100k AND followers meet minimum threshold
            (
                    (follower_filtered_df["main_artist_spotify_monthly_listeners"] >= 100000) &
                    (follower_filtered_df[
                         "main_artist_spotify_followers"] >= input_lists.minimum_spotify_followers_if_100k_monthly_listeners)
            )
            ]
        log_dropped_rows(follower_filtered_df, minimum_spotify_followers_if_100k_monthly_listeners_filtered_df,
                         "minimum_spotify_followers_if_100k_monthly_listeners")

        min_stream_filter_df = minimum_spotify_followers_if_100k_monthly_listeners_filtered_df[
            # Keep rows where either:
            (minimum_spotify_followers_if_100k_monthly_listeners_filtered_df["day_1-3_average"] == 0) |  # is zero
            (minimum_spotify_followers_if_100k_monthly_listeners_filtered_df[
                 "day_1-3_average"] >= input_lists.min_average_streams_if_above_0)
            ]
        log_dropped_rows(minimum_spotify_followers_if_100k_monthly_listeners_filtered_df, min_stream_filter_df,
                         "min_streams_if_above_0_average")

        # For songs that made it this far get the artist tiktok followers
        min_stream_filter_df["main_artist_tiktok_followers"] = fetch_tiktok_followers_threaded(min_stream_filter_df)

        # Filter artists with over input_lists.max_tiktok_followers if above 0
        tiktok_filtered_df = min_stream_filter_df[
            (min_stream_filter_df["main_artist_tiktok_followers"] == 0) |
            (min_stream_filter_df["main_artist_tiktok_followers"] <= input_lists.max_tiktok_followers)]
        log_dropped_rows(min_stream_filter_df, tiktok_filtered_df, "max_tiktok_followers")

        # Dropping duplicates
        df = tiktok_filtered_df.drop_duplicates(subset="url")
        # Sort by today_streams in descending order
        df = df.sort_values(by="today_streams", ascending=False)

        # Log all songs that passed all filters
        for uuid in df['song_uuid']:
            logger.debug(f"song {uuid} passed all filters")

        return df

    except Exception as e:
        logger.error(f"Error in apply_follower_stream_listeners_filters_and_drop_duplicates: {e}")
        return pd.DataFrame()


def get_artist_audience(artist_uuid: str, platform: str) -> int:
    try:
        response = session.get(
            BASE_API_URL + f"/v2/artist/{artist_uuid}/audience/{platform}", headers=credentials
        )
        if response.status_code != 200:
            return 0
        items = response.json()["items"]
        most_recent_day = items[-1]
        follower_count: int = most_recent_day.get("followerCount")
        logger.debug(f"Artist {artist_uuid} has {follower_count} {platform} followers")
        return follower_count
    except Exception as e:
        logger.debug(f"Failed getting artist audience {e}")
        return 0


def non_instrumental_non_english(song_metadata, instrumentalness):
    if not is_english(song_metadata.name):
        # If the song is not in English, we check if it is instrumental
        if isinstance(instrumentalness, float):
            # If the song has less than 50% instrumentalness, we consider it non-instrumental
            if instrumentalness < 0.5:
                # Non-english and non-instrumental
                return True
        else:
            return True
    return False


def signed_to_watchlist_label(song_label_list: list[str]) -> bool:
    watchlist_labels = [label.lower().strip() for label in input_lists.label_watchlist]
    song_label_list = [label.lower().strip() for label in song_label_list]

    for song_label in song_label_list:
        if any(watchlist_label == song_label for watchlist_label in watchlist_labels):
            return True

    return False
