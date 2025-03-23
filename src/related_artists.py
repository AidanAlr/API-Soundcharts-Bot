from dataclasses import dataclass

import pandas as pd
import requests

from src.credentials_key_info import BASE_API_URL, credentials
from src.logging_config import logger
from src.song_info import add_daily_streams_column


@dataclass
class Artist:
    name: str
    uuid: str
    slug: str
    appUrl: str
    imageUrl: str


def get_related_artists(artist_uuid: str) -> list[Artist]:
    try:
        url = BASE_API_URL + f"/v2/artist/{artist_uuid}/related"
        response = requests.get(url, headers=credentials)
        artists = response.json().get("items")
        artists = [Artist(name=artist.get("name"), uuid=artist.get("uuid"), slug=artist.get("slug"),
                          appUrl=artist.get("appUrl"), imageUrl=artist.get("imageUrl")) for artist in artists]
        return artists
    except Exception as e:
        print(e)
        return None


def get_related_artists_and_their_related_artists(artist_uuid: str):
    result = []
    initial_related_artists: list[Artist] = get_related_artists(artist_uuid)
    result.extend(initial_related_artists)

    for artist in initial_related_artists:
        result.extend(get_related_artists(artist.uuid))

    return result


def get_artist_streaming_audience(artist_uuid: str, start_date: str | None = None, end_date: str | None = None,
                                  platforn: str = "spotify"):
    url = BASE_API_URL + f"/v2/artist/{artist_uuid}/streaming/{platforn}/listening?"
    if start_date:
        url += f"?startDate={start_date}"
    if end_date:
        url += f"&endDate={end_date}"

    response = requests.get(url, headers=credentials)
    days = response.json().get("items")

    stream_dict = {}
    for day in days:
        date_string = day["date"].split("T")[0]
        stream_dict[date_string] = day["value"]

    # Must be reversed so that the most recent date is at the top for the add_daily_streams_column function
    stream_data_df = pd.DataFrame(reversed(list(stream_dict.items())), columns=["date", "total_streams"])
    stream_data_df = add_daily_streams_column(stream_data_df)
    return stream_data_df


def get_artist_average_streams_12_months_ago(artist_uuid: str):
    today = pd.Timestamp.now()
    twelve_months_ago = (today - pd.DateOffset(months=12)).strftime("%Y-%m-%d")
    eleven_months_ago = (today - pd.DateOffset(months=11)).strftime("%Y-%m-%d")
    stream_data_df = get_artist_streaming_audience(artist_uuid, start_date=twelve_months_ago,
                                                   end_date=eleven_months_ago)
    logger.info(f"Got stream data for {artist_uuid} from {twelve_months_ago} to {eleven_months_ago}")

    # Remove rows that are not between the start and end date
    stream_data_df = stream_data_df[stream_data_df["date"] >= twelve_months_ago]
    stream_data_df = stream_data_df[stream_data_df["date"] <= eleven_months_ago]
    print(stream_data_df)

    return stream_data_df["daily_streams"].mean()


print(get_artist_average_streams_12_months_ago("ca22091a-3c00-11e9-974f-549f35141000"))
