import datetime

import pandas as pd
import requests

from src.credentials_key_info import BASE_API_URL, credentials
from src.filters import apply_follower_stream_listeners_filters_and_drop_duplicates
from src.input_lists import max_change_in_total_streams_over_period, \
    min_change_in_total_streams_over_period, period, sort_by, ranking_max_total_streams, \
    ranking_min_total_streams, ranking_pages_to_collect
from src.logging_config import logger
from src.output import process_scrape_output
from src.session_manager import session
from src.song_info import get_all_song_info

columns = [
    # Identifier columns
    "song_uuid",
    "song_name",
    "url",

    # Artist information
    "main_artist",
    # "main_artist_uuid",
    # "artists",
    "labels",

    # Spotify artist metrics
    "main_artist_spotify_followers",
    "main_artist_spotify_monthly_listeners",
    # "main_artist_spotify_conversion_rate",

    # Genre information
    "root_genres",
    "sub_genres",

    # Song characteristics
    "release_date",
    "duration",
    "instrumentalness",

    # Streaming metrics - current week
    "total_streams",
    # "change_in_total_streams_this_week",
    # "%_change_in_total_streams_this_week",

    "this_week_7_day_average",
    "last_week_7_day_average",
    "week_to_week_percentage_increase",

    # Daily streaming data
    "today_streams",
    "yesterday_streams",
    "day_1-3_average",
    "day_7-9_average",
    "%_increase",

    # Historical streaming metrics
    "14_day_max",
    "14_day_median",
]


def extract_songs(response: requests.Response):
    songs = []
    for song in response.json().get("items"):
        songs.append({
            # "song_name": song.get("song").get("name"),
            "song_uuid": song.get("song").get("uuid"),
            # "main_artist": song.get("song").get("creditName"),
            "total": song.get("total"),
            "change": song.get("change"),
            "percent": song.get("percent"),

        })

    return songs


def get_updated_at_for_song_ranking():
    url = (f"{BASE_API_URL}/v2/top-song/spotify/streams?sortBy=percent&period=week&minValue=0&maxValue=1000000"
           f"&minChange=0&maxChange=100000")
    response = session.get(url, headers=credentials)
    updated_at: str = response.json().get("related").get("updatedAt")
    updated_at_datetime = datetime.datetime.fromisoformat(updated_at)
    return updated_at_datetime


def get_song_ranking(
        audience_max_change,
        audience_min_change,
        period,
        platform,
        metric,
        sort_by,
        pages_to_collect,
        max_total_audience,
        min_total_audience,
        country_code: str = None,
):
    url = f"{BASE_API_URL}/v2/top-song/{platform}/{metric}?sortBy={sort_by}&period={period}&minValue={min_total_audience}&maxValue={max_total_audience}&minChange={audience_min_change}&maxChange={audience_max_change}"
    logger.debug(url)

    if country_code:
        url += f"&countryCode={country_code}"
    next_page = url

    songs = []
    while pages_to_collect > 0 and next_page:
        response = session.get(next_page, headers=credentials)
        logger.debug(response.json())
        songs += extract_songs(response)

        pages_to_collect -= 1
        _next: str = response.json().get("page").get("next")
        if _next:
            _next = _next.removeprefix("/api")
            next_page = BASE_API_URL + _next
        else:
            next_page = None
        logger.debug(f"{pages_to_collect} pages left to collect".center(50, "*"))

    df = pd.DataFrame(songs)

    df = df.rename(columns={
        "change": f"change_in_total_streams_this_{period}",
        "percent": f"%_change_in_total_streams_this_{period}",
    }
    )

    return df


def enrich_dataframe(original_df):
    # Create a copy to avoid modifying the original DataFrame
    enriched_df = original_df.copy()

    # Create a list to store song information DataFrames
    song_info_dfs = []

    # Iterate through unique song UUIDs
    total = len(original_df['song_uuid'].unique())
    count = 0
    for song_uuid in original_df['song_uuid'].unique():
        # Get song info for each unique song UUID
        song_info = get_all_song_info(song_uuid)
        if not song_info.empty:
            song_info_dfs.append(song_info)

        count += 1
        logger.debug(f"Got song info for {count}/{total} songs")

    # Concatenate all song info DataFrames
    song_info_combined = pd.concat(song_info_dfs, ignore_index=True)

    # Merge the original DataFrame with the song info
    enriched_df = pd.merge(enriched_df, song_info_combined, on='song_uuid', how='right', validate="one_to_one")

    return enriched_df


def updated_within_24_hours(updated_at: datetime.datetime) -> bool:
    right_now = datetime.datetime.now(tz=datetime.timezone.utc)
    return right_now - updated_at < datetime.timedelta(days=1)


def run_general_ranking_scrape(on: bool = False) -> pd.DataFrame | None:
    logger.info("Running general ranking scrape".center(50, "-"))

    updated_at = get_updated_at_for_song_ranking()

    if updated_within_24_hours(updated_at) and on:
        # Do the full scrape
        # Get the top songs for the week
        song_ranking = get_song_ranking(
            audience_max_change=max_change_in_total_streams_over_period,
            audience_min_change=min_change_in_total_streams_over_period,
            period=period,
            platform="spotify",
            metric="streams",
            sort_by=sort_by,
            pages_to_collect=ranking_pages_to_collect,
            max_total_audience=ranking_max_total_streams,
            min_total_audience=ranking_min_total_streams,
        )

        enriched_song_ranking = enrich_dataframe(song_ranking)

        # Filter songs without 100% increase in week_to_week_percentage_increase
        enriched_song_ranking = enriched_song_ranking[enriched_song_ranking["week_to_week_percentage_increase"] >= 100]

        result_df = apply_follower_stream_listeners_filters_and_drop_duplicates(df=enriched_song_ranking)
        result_df = result_df[columns]
        return process_scrape_output(result_df, "general", str(updated_at.date()))

    else:
        return None, None, None
