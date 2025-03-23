import datetime
from types import SimpleNamespace
from typing import Any

import numpy as np
import pandas as pd
import requests

from src import input_lists
from src.credentials_key_info import BASE_API_URL, credentials
from src.filters import get_three_day_average_from_stream_df, get_last_week_3_day_avg, \
    get_increase_between_avg, get_today_streams, get_yesterday_streams, get_14_day_daily_max_median, get_total_streams, \
    signed_to_banned_label, x_percent_of_streams_are_from_one_day_in_last_14_days, is_english, \
    signed_to_watchlist_label, get_last_week_7_day_avg, get_this_week_7_day_avg
from src.logging_config import logger
from src.session_manager import session
from src.utils import extract_label_list_from_song_metadata, get_artist_names_and_main_artist_uuid, \
    get_instrumentalness_from_song_metadata, get_root_genres_from_song_metadata, get_sub_genres_from_song_metadata, \
    get_remaining_api_quota_from_headers_and_update_remaining_quota, banned_artist, get_uuid_from_url
from src.watchlist import global_label_watchlist_df_list


def get_song_metadata(uuid: str) -> SimpleNamespace:
    try:
        response: requests.Response = session.get(
            BASE_API_URL + "/v2.25/" + "song/" + uuid, headers=credentials
        )
        rj: dict[str, Any] = response.json()

        # replace None with "N/A"
        for key, value in rj["object"].items():
            if value is None:
                rj["object"][key] = "N/A"

        data: SimpleNamespace = SimpleNamespace(**rj["object"])

        get_remaining_api_quota_from_headers_and_update_remaining_quota(response)

        return data
    except:
        return None


def get_artist_follower_count_from_uuid(uuid: str, platform: str):
    try:
        response = session.get(
            BASE_API_URL + "/v2/" + "artist/" + uuid + "/audience/" + platform, headers=credentials
        )
        items = response.json()["items"]
        main_artist = items[0]
        follower_count = main_artist["followerCount"]
        return follower_count
    except Exception:
        logger.debug("Failed getting artist follower count from uuid")
        return np.nan


def get_spotify_followers_monthly_listeners_conversion_rate(artist_uuid: str):
    try:
        url = BASE_API_URL + f"/v2/artist/{artist_uuid}/spotify/retention"
        response = session.get(url, headers=credentials)
        items = response.json()["items"]

        most_recent_day = items[-1]

        last_day_followers = most_recent_day.get("followers")
        last_day_listeners = most_recent_day.get("listeners")
        last_day_conversion_rate = most_recent_day.get("conversionRate")

        return last_day_followers, last_day_listeners, last_day_conversion_rate

    except IndexError:
        logger.debug("artist has no spotify followers_monthly listeners and conversion rate data")
        return np.nan, np.nan, np.nan

    except Exception as e:
        # logger.error(f"Failed getting artist spotify followers_monthly listeners and conversion rate {e}")
        return np.nan, np.nan, np.nan


def get_average_spotify_popularity_for_plots(plots: list[dict]) -> int:
    sum_of_values = 0
    count = 0
    for plot in plots:
        count += 1
        sum_of_values += plot.get("value")

    average_spotify_popularity = sum_of_values // count
    return average_spotify_popularity


def get_song_spotify_popularity_history(uuid: str):
    url = BASE_API_URL + f"/api/v2/song/{uuid}/spotify/identifier/popularity"
    response = session.get(url, headers=credentials)
    response_json = response.json()
    items = response_json.get("items")

    date_value_tuple_ls = []
    for item in items:
        date = item.get("date")
        plots = item.get("plots")

        average_spotify_popularity = get_average_spotify_popularity_for_plots(plots)

        date_value_tuple_ls.append((date, average_spotify_popularity))

    return date_value_tuple_ls


def get_spotify_popularity_growth(uuid: str):
    try:
        popularity_history = get_song_spotify_popularity_history(uuid)

        most_recent_date, most_recent_popularity = popularity_history[0]
        oldest_date, oldest_popularity = popularity_history[-1]

        growth = oldest_popularity - most_recent_popularity
        return growth
    except IndexError:
        return np.nan


def add_daily_streams_column(stream_data_df):
    stream_data_df["daily_streams"] = stream_data_df["total_streams"].diff().fillna(0)
    stream_data_df["daily_streams"] = stream_data_df["daily_streams"].shift(-1) * -1
    return stream_data_df


def get_stream_df_from_response(response):
    try:
        days = response.json()["items"]

        stream_dict = {}
        for day in days:
            date_string = day["date"].split("T")[0]
            stream_dict[date_string] = day["plots"][0]["value"]

        stream_data_df = pd.DataFrame(list(stream_dict.items()), columns=["date", "total_streams"])

        stream_data_df = add_daily_streams_column(stream_data_df)

        return stream_data_df
    except KeyError:
        return pd.DataFrame()


def get_song_audience(uuid, platform, start_date=None, end_date=None):
    url = BASE_API_URL + f"/v2/song/{uuid}/audience/{platform}"

    if start_date and end_date:
        url += f"?startDate={start_date}&endDate={end_date}"

    response = session.get(url, headers=credentials)
    if response.status_code != 200:
        logger.debug(f"Song {uuid} has no audience metrics")
        return pd.DataFrame()

    stream_data_df = get_stream_df_from_response(response)
    return stream_data_df


def get_song_audience_from_date(uuid, oldest_date_to_collect, platform: str = "spotify"):
    # Get today's date
    today = datetime.datetime.now().date()
    if pd.isna(oldest_date_to_collect):
        oldest_date_to_collect = today - datetime.timedelta(days=365)
    else:
        # Convert oldest_date_to_collect to datetime object
        oldest_date_to_collect = datetime.datetime.strptime(oldest_date_to_collect, "%Y-%m-%d").date()

    # Initialize list to store dataframes
    audience_chunks = []

    i = 0
    start_date = today
    while start_date > oldest_date_to_collect:
        # Calculate end date (last day of the 90-day period)
        end_date = today - datetime.timedelta(days=i * 90)

        distance_from_oldest_date = (end_date - oldest_date_to_collect).days
        if distance_from_oldest_date > 89:
            # Calculate start date (first day of the 90-day period)
            start_date = end_date - datetime.timedelta(days=89)
        else:
            start_date = oldest_date_to_collect

        # Convert dates to the required format
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")

        # Retrieve audience data for this chunk
        chunk_data = get_song_audience(
            uuid,
            platform,
            start_date=start_date_str,
            end_date=end_date_str
        )

        i += 1

        # Only append non-empty dataframes
        if not chunk_data.empty:
            audience_chunks.append(chunk_data)

    df = pd.concat(audience_chunks, ignore_index=True)
    return df



def in_song_blocklist(uuid: str) -> bool:
    for url in input_lists.song_blocklist_urls:
        if get_uuid_from_url(url) == uuid:
            return True

    return False


def failed_artist_label_english_filters(song_uuid, song_metadata):
    # Get info from the song metadata
    song_label_list = extract_label_list_from_song_metadata(song_metadata)
    artist_names, main_artist_uuid = get_artist_names_and_main_artist_uuid(song_metadata)
    instrumentalness = get_instrumentalness_from_song_metadata(song_metadata)

    if signed_to_watchlist_label(song_label_list):
        song_metadata_df: pd.DataFrame = extract_metadata_to_df(song_metadata)
        song_audience_df: pd.DataFrame = get_song_audience(song_uuid, "spotify")
        metrics_df: pd.DataFrame = get_metrics_df(song_audience_df, song_uuid)
        result_df: pd.DataFrame = pd.concat([song_metadata_df, metrics_df], axis=1)

        global_label_watchlist_df_list.append(result_df)
        logger.debug(f"Song {song_uuid} added to watchlist label df")
        return True

    if in_song_blocklist(song_uuid):
        logger.debug(f"Song {song_uuid} is in the song blocklist, skipping")
        return True

    # Check if song has more than 1 artist
    if len(artist_names) > input_lists.max_artists_on_track:
        logger.debug(f"Song {song_uuid} has more than {input_lists.max_artists_on_track} artists, skipping")
        return True

    if signed_to_banned_label(song_label_list):
        logger.debug(f"Song {song_uuid} is signed to banned label, skipping")
        return True

    if banned_artist(artist_names[0]):
        logger.debug(f"Song {song_uuid} is by a banned artist, skipping")
        return True

    # if non_instrumental_non_english(song_metadata, instrumentalness):
    #     logger.debug(f"Song {song_uuid} is non-english non-instrumental, skipping")
    #     return True

    if not is_english(song_metadata.name):
        logger.debug(f"Song {song_uuid} is not in English, skipping")
        return True

    return False


def extract_metadata_to_df(song_metadata) -> pd.DataFrame:
    # Get info from the song metadata
    song_label_list = extract_label_list_from_song_metadata(song_metadata)
    artist_names, main_artist_uuid = get_artist_names_and_main_artist_uuid(song_metadata)
    instrumentalness = get_instrumentalness_from_song_metadata(song_metadata)

    followers, listeners, conversion_rate = get_spotify_followers_monthly_listeners_conversion_rate(main_artist_uuid)

    metadata_dict = {
        "song_uuid": song_metadata.uuid,
        "song_name": song_metadata.name,
        "url": song_metadata.appUrl.replace("overview", "trends"),
        "labels": ", ".join(song_label_list),
        "artists": ", ".join(artist_names),
        "main_artist": artist_names[0],
        "main_artist_uuid": main_artist_uuid,
        "main_artist_spotify_followers": followers,
        "main_artist_spotify_monthly_listeners": listeners,
        "main_artist_spotify_conversion_rate": conversion_rate,
        "instrumentalness": instrumentalness,
        "root_genres": get_root_genres_from_song_metadata(song_metadata),
        "sub_genres": get_sub_genres_from_song_metadata(song_metadata),
        "release_date": song_metadata.releaseDate.split("T")[0],
        "duration": song_metadata.duration,
    }

    metadata_df = pd.DataFrame(metadata_dict, index=[0])
    return metadata_df


def get_metrics_df(song_audience_df: pd.DataFrame, song_uuid: str) -> pd.DataFrame:
    if song_audience_df.empty:
        logger.debug(f"Song {song_uuid} has no stream data, leaving with empty metrics")
        return pd.DataFrame(
            columns=["today_streams", "yesterday_streams", "day_1-3_average", "day_7-9_average", "%_increase",
                     "14_day_max", "14_day_median", "total_streams"])

    # Three day averages
    three_day_average: int = get_three_day_average_from_stream_df(song_audience_df)
    last_week_3_day_avg: int = get_last_week_3_day_avg(song_audience_df)
    # Increase between averages
    increase_between_avg: int = get_increase_between_avg(three_day_average, last_week_3_day_avg)

    # 7 Day Averages used for general ranking
    this_week_seven_day_average: int = get_this_week_7_day_avg(song_audience_df)
    last_week_seven_day_average: int = get_last_week_7_day_avg(song_audience_df)
    try:
        week_to_week_percentage_increase: float = (
                                                          this_week_seven_day_average - last_week_seven_day_average) / last_week_seven_day_average * 100
    except ZeroDivisionError:
        week_to_week_percentage_increase = 0

    # Today and yesterday streams
    today_streams: int = get_today_streams(song_audience_df)
    yesterday_streams: int = get_yesterday_streams(song_audience_df)

    # 14 day max and median
    fourteen_day_max, fourteen_day_median = get_14_day_daily_max_median(song_audience_df)

    # Total streams
    total_streams = get_total_streams(song_audience_df)

    # Create a dataframe with the metrics
    df = pd.DataFrame(
        {
            "today_streams": [round(today_streams, 2)],
            "yesterday_streams": [round(yesterday_streams, 2)],

            # Just used for general ranking
            "this_week_7_day_average": [round(this_week_seven_day_average, 2)],
            "last_week_7_day_average": [round(last_week_seven_day_average, 2)],
            "week_to_week_percentage_increase": [round(week_to_week_percentage_increase, 2)],

            "day_1-3_average": [round(three_day_average, 2)],
            "day_7-9_average": [round(last_week_3_day_avg, 2)],
            "%_increase": [round(increase_between_avg, 2)],
            "14_day_max": [round(fourteen_day_max, 2)],
            "14_day_median": [round(fourteen_day_median, 2)],
            "total_streams": [total_streams],
        }
    )
    return df


def get_all_song_info(song_uuid) -> pd.DataFrame:
    song_metadata = get_song_metadata(song_uuid)

    if not song_metadata:
        return pd.DataFrame()

    if failed_artist_label_english_filters(song_uuid, song_metadata):
        return pd.DataFrame()

    song_metadata_df: pd.DataFrame = extract_metadata_to_df(song_metadata)
    song_audience_df = get_song_audience(song_uuid, "spotify")

    if not song_audience_df.empty and x_percent_of_streams_are_from_one_day_in_last_14_days(song_audience_df,
                                                                                            input_lists.max_percent_of_streams_on_one_day):
        logger.debug(
            f"Song {song_uuid} has < {input_lists.max_percent_of_streams_on_one_day} of streams from one day, skipping")
        return pd.DataFrame()

    metrics_df = get_metrics_df(song_audience_df, song_uuid)
    result_df = pd.concat([song_metadata_df, metrics_df], axis=1)

    return result_df
