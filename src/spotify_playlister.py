import os

import pandas
import spotipy
from dotenv import load_dotenv
from more_itertools import chunked as batched
from spotipy.oauth2 import SpotifyOAuth

from src.credentials_key_info import BASE_API_URL, credentials
from src.logging_config import logger
from src.session_manager import session
from src.utils import get_uuid_from_url

load_dotenv()
client_id = os.getenv("SPOTIPY_CLIENT_ID")
client_secret = os.getenv("SPOTIPY_CLIENT_SECRET")
redirect_uri = os.getenv("SPOTIPY_REDIRECT_URI")
user_id = os.getenv("SPOTIPY_USER_ID")


class DefaultSpotipy:
    scope = "playlist-modify-public"
    sp = spotipy.Spotify(auth_manager=SpotifyOAuth(client_id=client_id,
                                                   client_secret=client_secret,
                                                   redirect_uri=redirect_uri,
                                                   scope=scope))


def create_playlist(name):
    sp = DefaultSpotipy().sp
    playlist = sp.user_playlist_create(user=user_id, name=name)
    playlist_id = playlist["id"]
    logger.info("Created playlist")
    return playlist_id


def add_to_playlist(playlist_id, tracks: list[str]):
    if not tracks:
        return
    sp = DefaultSpotipy().sp

    tracks = list(batched(tracks, 90))
    for tracklist in tracks:
        sp.playlist_add_items(playlist_id, tracklist)
        logger.info("Added tracks to playlist")


def create_playlist_from_spotify_id_list(spotify_id_list: list, name) -> str:
    from tqdm import tqdm
    """

    Args:
        spotify_id_list:
        name:

    Returns:
        playlist_url

    """
    # Convert the list of spotify ids into a list of uris
    uris = []
    for spotify_id in tqdm(spotify_id_list):
        uris.append(convert_spotify_id_to_uri_for_track(spotify_id))
    playlist = create_playlist(name)
    add_to_playlist(playlist, uris)
    playlist_url = f"https://open.spotify.com/playlist/{playlist}"
    return playlist_url


def convert_spotify_id_to_uri_for_track(spotify_id):
    return "spotify:track:" + spotify_id


def get_uuids_from_song_result_df(playlist_df: pandas.DataFrame):
    urls = playlist_df["url"].tolist()
    uuids = [get_uuid_from_url(url) for url in urls]
    logger.info("Got uuids from song result df")
    return uuids


def create_playlist_on_spotify_for_songs_in_df(df, playlist_name):
    logger.info("Starting to create playlist on Spotify!".center(100, "-"))
    uuids = get_uuids_from_song_result_df(df)
    spotify_id_list = get_spotify_ids(uuids)
    playlist_url = create_playlist_from_spotify_id_list(spotify_id_list=spotify_id_list, name=playlist_name)
    return playlist_url


def get_spotify_id(uuid):
    try:
        spotify_id = get_spotify_id_with_uuid_platform_search(uuid)
        return spotify_id
    except TimeoutError:
        logger.debug(f"Timeout error for {uuid}")
        return None
    except Exception as e:
        logger.debug("Error in get_spotify_id {e}")
        return None


def get_spotify_ids(uuid_list: list):
    from tqdm import tqdm
    spotify_ids = []
    for uuid in tqdm(uuid_list):
        spotify_id = get_spotify_id(uuid)
        if spotify_id is not None:
            spotify_ids.append(spotify_id)

    return spotify_ids


def get_spotify_id_with_uuid_platform_search(uuid):
    try:
        _next = BASE_API_URL + f"/v2/song/{uuid}/identifiers?platform=spotify&offset=0&limit=100"
        response = session.get(_next, headers=credentials)
        items = response.json()["items"]
        _id = items[0]["identifier"]
        return _id
    except IndexError as e:
        logger.debug(f"{e} - No spotify link for {uuid}")
        return None
