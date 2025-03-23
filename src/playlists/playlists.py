import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

import pandas as pd
import requests
from tqdm import tqdm

from src.common_columns import COMMON_COLUMNS
from src.credentials_key_info import BASE_API_URL, credentials
from src.filters import apply_follower_stream_listeners_filters_and_drop_duplicates
from src.logging_config import logger
from src.output import process_scrape_output
from src.session_manager import session
from src.sheets_utils import add_past_appearances_to_df, drop_songs_that_appeared_in_past
from src.song_info import get_all_song_info
from src.utils import get_uuid_from_url, convert_dataframe_to_csv, \
    get_remaining_api_quota_from_headers_and_update_remaining_quota

playlist_columns = ["date_added",
                    "playlist_name"] + COMMON_COLUMNS


def get_start_end_datetime(start: str, end: str) -> tuple[datetime, datetime]:
    start = datetime.fromisoformat(start + "T00:00:00+00:00")
    end = datetime.fromisoformat(end + "T23:59:59+00:00")
    return start, end


def scrape_tracklist_dates(uuid: str, tracklisting_dates: list[str]) -> pd.DataFrame:
    """
    Scrape the tracklist for a list of dates, return a combined DataFrame
    """
    tracklist_ls = []
    for date in tracklisting_dates:
        tracklist_ls.append(get_playlist_tracklist_on_date_from_uuid_with_playlist_info(uuid, date))

    tracklist_df = pd.concat(tracklist_ls, ignore_index=True)
    return tracklist_df


def get_tracklist_df_for_today_and_yesterday_and_playlist_info(uuid: str) -> (pd.DataFrame, pd.DataFrame):
    tracklisting_dates = get_available_tracklisting_dates(uuid)
    today, yesterday = tracklisting_dates[0], tracklisting_dates[1]
    today_tracklist_df = get_playlist_tracklist_on_date_from_uuid_with_playlist_info(uuid, today)
    yesterday_tracklist_df = get_playlist_tracklist_on_date_from_uuid_with_playlist_info(uuid, yesterday)
    logger.debug("Today: {}, Yesterday: {}".format(today, yesterday))
    return today_tracklist_df, yesterday_tracklist_df


def add_accurate_date_added_to_columns(tracklist_df: pd.DataFrame) -> pd.DataFrame:
    pd.options.mode.copy_on_write = True

    # Sort by date, convert to date
    tracklist_df['playlist_crawl_date'] = pd.to_datetime(tracklist_df['playlist_crawl_date'])
    # Extract date from datetime
    tracklist_df['playlist_crawl_date'] = tracklist_df['playlist_crawl_date'].dt.date
    tracklist_df = tracklist_df.sort_values('playlist_crawl_date')

    # Add a column for the date_added
    for uuid in tracklist_df['song_uuid'].unique():
        # Get all the rows for the song
        song_df = tracklist_df[tracklist_df['song_uuid'] == uuid]
        # Set the date_added column to the oldest date
        oldest_date_added = song_df['playlist_crawl_date'].min()
        tracklist_df.loc[song_df.index, 'date_added'] = oldest_date_added

    return tracklist_df


def remove_songs_not_added_on_latest_crawl_date(tracklist_df: pd.DataFrame) -> pd.DataFrame:
    if tracklist_df.empty:
        logger.error("No tracklist data")
        return tracklist_df

    if 'playlist_crawl_date' not in tracklist_df.columns:
        logger.error("No playlist_crawl_date column")
        return tracklist_df

    # Get the freshest date
    crawl_dates = pd.to_datetime(tracklist_df['playlist_crawl_date'])
    freshest_date = crawl_dates.max().date()

    # Add accurate date added to columns
    tracklist_df = add_accurate_date_added_to_columns(tracklist_df)

    # Filter to songs added today
    tracklist_df = tracklist_df[tracklist_df['date_added'] == freshest_date]

    return tracklist_df


def get_songs_added_to_playlist_in_last_day(uuid: str) -> pd.DataFrame:
    """
      Identify songs added to a playlist in the last day.

      Args:
          uuid (str): The playlist UUID.

      Returns:
          pd.DataFrame: DataFrame of new songs added to the playlist.
      """
    today_tracklist_df, yesterday_tracklist_df = get_tracklist_df_for_today_and_yesterday_and_playlist_info(uuid)
    combined_df = pd.concat([today_tracklist_df, yesterday_tracklist_df], ignore_index=True)
    combined_df = remove_songs_not_added_on_latest_crawl_date(combined_df)
    combined_df_uuids = set(combined_df['song_uuid'])
    yesterday_length, today_length = len(yesterday_tracklist_df), len(today_tracklist_df)
    logger.info(
        f"Today: {today_length}, Yesterday: {yesterday_length} - {len(combined_df)} new songs added to playlist {uuid}: {combined_df_uuids}")

    return combined_df


def get_complete_song_info_for_songs_added_to_playlist_in_last_day(playlist_url: str) -> pd.DataFrame:
    uuid: str = get_uuid_from_url(playlist_url)
    tracklist: pd.DataFrame = get_songs_added_to_playlist_in_last_day(uuid)
    complete_song_info_df: pd.DataFrame = get_song_info_and_combine_with_playlist_info(tracklist)
    logger.debug(f"Finished scraping {playlist_url}")
    return complete_song_info_df


def copy_over_playlist_info(copy_from, copy_to):
    copy_to["date_added"] = copy_from["date_added"]
    copy_to["playlist_name"] = copy_from["playlist_name"]
    copy_to["playlist_uuid"] = copy_from["playlist_uuid"]
    copy_to["playlist_platform"] = copy_from["playlist_platform"]
    copy_to["playlist_crawl_date"] = copy_from["playlist_crawl_date"]
    return copy_to


def get_song_info_and_combine_with_playlist_info(tracklist: pd.DataFrame) -> pd.DataFrame:
    if tracklist.empty:
        return pd.DataFrame()

    result_ls = []

    def process_song(song):
        song_info_df = get_all_song_info(song["song_uuid"])
        return copy_over_playlist_info(song, song_info_df)

    # Use ThreadPoolExecutor to process songs in parallel
    with ThreadPoolExecutor() as executor:
        future_to_song = {
            executor.submit(process_song, song): song
            for _, song in tracklist.iterrows()
        }

        for future in as_completed(future_to_song):
            try:
                result_ls.append(future.result())
            except Exception as e:
                # Handle exceptions if needed, e.g., log errors
                print(f"Error processing song: {e}")

    if not result_ls:
        return pd.DataFrame()

    return pd.concat(result_ls, ignore_index=True)


def convert_tracklist_to_df(tracklist: list[dict]) -> pd.DataFrame:
    tracklist_df_ls = []
    for track in tracklist:
        row: pd.DataFrame = pd.DataFrame(
            {
                "song_name": [track["name"]],
                "song_uuid": [track["uuid"]],
            }
        )
        tracklist_df_ls.append(row)

    tracklist_df = pd.concat(tracklist_df_ls, ignore_index=True)
    return tracklist_df


def get_playlist_tracklist_on_date_from_uuid_with_playlist_info(
        uuid: str, date_and_time: str
) -> pd.DataFrame:
    try:
        tracklist: list[dict] = []

        _next: str = "0"
        while _next is not None:
            url = BASE_API_URL + f"/v2.20/playlist/{uuid}/tracks/{date_and_time}?offset={str(_next)}&limit=100"
            response_json = session.get(url, headers=credentials).json()

            items = response_json.get("items")
            if not items:
                break
            songs: list[dict] = [item["song"] for item in items]
            tracklist += songs

            _next = response_json["page"]["next"]

        tracklist_df = convert_tracklist_to_df(tracklist)

        playlist_metadata = response_json["related"]["playlist"]
        tracklist_df["playlist_name"] = playlist_metadata.get("name")
        tracklist_df["playlist_uuid"] = playlist_metadata.get("uuid")
        tracklist_df["playlist_platform"] = playlist_metadata.get("platform")

        # Note do not use latestCrawlDate as it updates any time they get new data use the date from the related object
        tracklist_df["playlist_crawl_date"] = response_json["related"].get("date")

        return tracklist_df

    except Exception as e:
        logger.debug(f"Error in get_playlist_tracklist_on_date_from_uuid {e}")
        return pd.DataFrame()


def get_available_tracklisting_dates(playlist_uuid: str) -> list[str]:
    end_date = datetime.now().date()
    periods = 5
    all_dates = []
    while periods > 0:
        try:
            end_date_str = end_date.strftime("%Y-%m-%d")
            response: requests.Response = session.get(
                BASE_API_URL + "/v2.20/playlist/" + playlist_uuid + "/available-tracklistings?offset=0&endDate=" + end_date_str,
                headers=credentials,
            )
            print(response.json())
            tracklisting_dates = response.json()["items"]
            all_dates += tracklisting_dates
            end_date -= timedelta(days=89)
            periods -= 1
        except Exception as e:
            logger.debug(f"Failed to get available tracklisting dates for {playlist_uuid} - {e}")
            return []

    get_remaining_api_quota_from_headers_and_update_remaining_quota(response)
    return all_dates


def run_playlist_scrape(playlist_list) -> tuple[str, pd.DataFrame, str]:
    logger.info("Scraping playlist data!".center(100, "-"))

    result_list: list[pd.DataFrame] = [pd.DataFrame(columns=playlist_columns)]

    for playlist in tqdm(playlist_list):
        result_df = get_complete_song_info_for_songs_added_to_playlist_in_last_day(playlist)
        if not result_df.empty:
            result_list.append(result_df)

    result_df = pd.concat(result_list, ignore_index=True)
    result_df = apply_follower_stream_listeners_filters_and_drop_duplicates(df=result_df)

    result_df = drop_songs_that_appeared_in_past(result_df)
    # result_df = add_past_appearances_to_df(result_df)

    # Reorder columns
    result_df = result_df[playlist_columns]
    logger.info("Finished scraping {} songs from playlists".format(len(result_df)))

    return process_scrape_output(result_df, "playlist")


def run_playlist_history_scrape(playlist_list: list[str], start_day: str, end_day: str) -> pd.DataFrame:
    logger.info("Scraping playlist history data!".center(100, "-"))

    result_list: list[pd.DataFrame] = []

    count = 0

    for playlist_url in tqdm(playlist_list):
        uuid: str = get_uuid_from_url(playlist_url)
        tracklisting_dates: list[str] = get_available_tracklisting_dates(uuid)

        # Convert input date strings to datetime, set start_day to the day before
        start, end = get_start_end_datetime(start_day, end_day)
        logger.info(f"Scraping playlist {playlist_url} from {start_day} to {end_day}")

        # Now filter the timestamps for between the start and end day
        tracklisting_dates_in_date_range = [date for date in tracklisting_dates if
                                            start <= datetime.fromisoformat(date) <= end]

        tracklist_df = scrape_tracklist_dates(uuid, tracklisting_dates_in_date_range)

        # Add accurate date added to columns
        tracklist_df = add_accurate_date_added_to_columns(tracklist_df)

        # We dont want to include songs that were added on the oldest crawl date, as this is outside our date range
        # It is necessary that we include that date in our scrape though to accurately get the songs added on start_day
        oldest_crawl_date = tracklist_df['playlist_crawl_date'].min()
        tracklist_df = tracklist_df[tracklist_df['date_added'] != oldest_crawl_date]

        # Remove duplicates based on song_uuid
        tracklist_df = tracklist_df.drop_duplicates('song_uuid', keep='last')

        complete_song_info_df: pd.DataFrame = get_song_info_and_combine_with_playlist_info(tracklist_df)
        df = complete_song_info_df
        if not df.empty:
            result_list.append(df)

        count += 1
        logger.info(f"Finished scraping {count}/{len(playlist_list)} playlists. Last playlist: {playlist_url}")

    if not result_list:
        logger.debug("No results from playlist scrape")
        return pd.DataFrame()

    results = pd.concat(result_list, ignore_index=True)
    results = apply_follower_stream_listeners_filters_and_drop_duplicates(df=results)
    # Reorder columns
    results = results[playlist_columns]

    OUTPUT_FOLDER = os.getenv("OUPUT_FOLDER", "/app/output")
    convert_dataframe_to_csv(results, filename=f"{OUTPUT_FOLDER}/playlist_history_{start_day}_{end_day}.csv")

    logger.info("Finished scraping {} songs from playlists".format(len(results)))
    return results
