import os
from typing import List

import requests

from src import input_lists
from src.logging_config import logger
from dotenv import load_dotenv

load_dotenv()

def get_uuid_from_url(url: str) -> str:
    link_components: List[str] = url.split("/")
    # Find id based on containing -
    for component in link_components:
        if "-" in component:
            return component




def convert_dataframe_to_csv(df, filename):
    df.to_csv(filename, index=False)
    logger.info("Dataframe saved as: " + filename)


def stream_spike_filter(df, multiplier=1.2):
    # Filter out songs with a spike in streams
    pre_spike_length = len(df)
    # Only keep rows with today_streams greater than day_1-3_average * 1.2
    df = df[df["today_streams"] > df["day_1-3_average"] * multiplier]
    # Check how many songs were removed
    post_spike_length = len(df)
    logger.info(f"Scraped Songs: {pre_spike_length}, Songs after spike filter: {post_spike_length}")
    return df


def process_future(future, result_list):
    """
    Process a future and append the result to the result_list if it is not empty.
    """
    stats = future.result()
    if not stats.empty:  # Only append if there is data
        result_list.append(stats)


def get_remaining_api_quota_from_headers_and_update_remaining_quota(response: requests.Response) -> int:
    try:
        quota = response.headers.get("X-Quota-Remaining")
        quota = int(quota)
        update_quota_in_file(quota)
        return quota
    except Exception as e:
        logger.debug(f"Failed to update quota {e}")
        return 0


def update_quota_in_file(quota: int):


    LOGS_FOLDER = os.getenv("LOGS_FOLDER")
    file_path = f"{LOGS_FOLDER}/quota.txt"
    with open(file=file_path, mode="w") as f:
        f.write(str(quota))


def extract_label_list_from_song_metadata(song_metadata):
    if song_metadata:
        song_label = [label["name"] for label in song_metadata.labels]
        return song_label
    else:
        return ["N/A"]


def get_root_genres_from_song_metadata(song_metadata):
    root_genres = [genre["root"] for genre in song_metadata.genres]
    return ", ".join(root_genres)


def get_sub_genres_from_song_metadata(song_metadata):
    subgenres = [genre["sub"] for genre in song_metadata.genres]

    return ", ".join(
        [
            item
            for sublist in subgenres
            for item in sublist
        ]
    )


def get_artist_names_and_main_artist_uuid(song_metadata):
    if song_metadata.artists:
        artist_names = [artist["name"] for artist in song_metadata.artists]
        main_artist_uuid = song_metadata.artists[0]["uuid"]
    else:
        artist_names = ["N/A"]
        main_artist_uuid = "N/A"

    return artist_names, main_artist_uuid


def banned_artist(artist_name):
    for artist in input_lists.artist_blocklist:
        if artist in artist_name:
            return True
    return False


def get_instrumentalness_from_song_metadata(song_metadata):
    audio_features = song_metadata.audio
    if isinstance(audio_features, dict) is False:
        instrumentalness = "N/A"
    else:
        instrumentalness = audio_features["instrumentalness"]
    return instrumentalness


