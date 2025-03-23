import pandas as pd
import requests
from wrapt_timeout_decorator import wrapt_timeout_decorator

from src.charts.chart_utils import filter_charts_by_doc, set_extra_values_for_df, get_uuid_toc_streams_tuples_from_items, \
    translate_input_genre_into_keywords_and_exlusion_list, get_filtered_sluglist, country_code_to_name_dict
from src.sheets_utils import drop_songs_that_appeared_in_past
from src.credentials_key_info import BASE_API_URL, credentials
from src.logging_config import logger
from src.session_manager import session
from src.song_info import get_all_song_info
from src.common_columns import COMMON_COLUMNS
from src.input_lists import max_streams, platform_genre_country_chart_tuples
from src.filters import apply_follower_stream_listeners_filters_and_drop_duplicates
from src.output import process_scrape_output

chart_columns = [
                    "country",
                    "platform",
                    "chart_slug",
                    "time_on_chart",
                ] + COMMON_COLUMNS


@wrapt_timeout_decorator.timeout(60)
def scrape_charts(slug_list: list, country_code: str, platform: str) -> pd.DataFrame:
    song_df_ls = []
    for slug in slug_list:
        logger.info(f"Scraping {slug}".center(50, "-"))

        # Get uuid, toc, streams for songs on chart
        uuid_toc_streams_for_songs_on_chart: list[tuple[str, int, int]] = get_uuid_toc_streams_for_songs_on_chart(slug)

        # Early Filtering
        # Get rid of songs with more than 1 day on chart
        uuid_toc_streams: list[tuple[str, int, int]] = filter_charts_by_doc(1, uuid_toc_streams_for_songs_on_chart)

        logger.debug("Songs with 1 day on chart".center(50, "*"))
        logger.debug(uuid_toc_streams)
        logger.debug("".center(50, "*"))

        # Remove songs with greater than max_streams
        uuid_toc_streams = [(uuid, toc, streams) for uuid, toc, streams in uuid_toc_streams if
                            streams < max_streams]

        # Get all song info for each song
        for uuid, toc, streams in uuid_toc_streams:
            song_df = get_all_song_info(uuid)
            country_name = country_code_to_name_dict[country_code]
            # Set extra values for the df
            song_df = set_extra_values_for_df(song_df, country_code, country_name, platform, uuid, toc, slug)
            song_df_ls.append(song_df)

    if not song_df_ls:
        return pd.DataFrame()

    return pd.concat(song_df_ls, ignore_index=True)


def get_uuid_toc_streams_for_songs_on_chart(chart_slug: str, date: str | None = None) -> list[tuple[str, int, int]]:
    logger.debug(f"Getting chart data for {chart_slug}")

    if not date:
        _next: str = BASE_API_URL + "/v2.14/chart/song/" + chart_slug + "/ranking/latest?offset=0&limit=100"
    else:
        response: requests.Response = session.get(BASE_API_URL + "/v2/chart/song/" + chart_slug + "/available-rankings?offset=0&limit=100",
                                                  headers=credentials)
        date_times = response.json()["items"]
        for dt in date_times:
            if date in str(dt):
                date = dt
                logger.warn(date)
                break
        _next: str = BASE_API_URL + "/v2.14/chart/song/" + chart_slug + "/ranking/" + date + "?offset=0&limit=100"

    result: list[tuple[str, int, int]] = []
    while _next:
        try:
            response: requests.Response = session.get(
                _next,
                headers=credentials,
            )
            items: list[dict] = response.json().get("items")
            list_of_uuid_toc_streams_tuples: list[tuple[str, int, int]] = get_uuid_toc_streams_tuples_from_items(items)
            result += list_of_uuid_toc_streams_tuples

            _next: str = response.json()["page"]["next"]
            if _next:
                _next = _next.removeprefix("/api")
                _next = BASE_API_URL + _next

        except Exception as e:
            logger.debug(f"Error in get_uuid_toc_streams_for_songs_on_chart {e}")
            return []

    logger.debug(f"Got {len(result)} songs from chart {chart_slug}")
    return result


def get_all_chart_slugs(platform: str, country_code: str) -> list[str] | None:
    try:
        _next = BASE_API_URL + f"/v2/chart/song/by-platform/{platform}?countryCode={country_code.lower()}&offset=0&limit=100"

        slug_result_list = []
        while _next:
            response: requests.Response = session.get(_next, headers=credentials)
            response_json: dict = response.json()

            _next = response_json.get("page")["next"]

            if _next:
                _next = _next.removeprefix("/api")
                _next = BASE_API_URL + _next

            items: dict = response_json["items"]
            slugs: list[str] = [item["slug"] for item in items]
            slug_result_list += slugs

        logger.debug(f"Got {len(slug_result_list)} chart slugs for {platform} and {country_code}")

        return slug_result_list

    except Exception as e:
        logger.debug(f"Failed to get chart slugs for {platform} and {country_code} for {e}")
        return None


def run_charts_scrape(pgc_tuples) -> tuple[str, pd.DataFrame, str]:
    logger.info("Scraping charts!".center(50, "-"))
    result_ls: list[pd.DataFrame] = [pd.DataFrame()]

    # Create a dictionary to store the chart slugs for each platform and country
    slug_cache = {}

    count = 0
    for platform, genre, country_code in pgc_tuples:

        # If the chart slugs for the platform and country have not been fetched yet, fetch them
        if (platform, country_code) not in slug_cache:
            slug_cache[(platform, country_code)] = get_all_chart_slugs(platform, country_code)

        # Translate the genre into a list of keywords, exclusion words and filter the chart slugs by these keywords
        keywords, exclusion_list = translate_input_genre_into_keywords_and_exlusion_list(genre)
        filtered_slug_list: list[str] = get_filtered_sluglist(slug_cache[(platform, country_code)],
                                                              keywords, exclusion_list)

        try:
            # Scrape the chart data for the filtered chart slugs
            scrape = scrape_charts(filtered_slug_list, country_code, platform)
            result_ls.append(scrape)
        except TimeoutError:
            logger.error(f"Timeout error when scraping {platform}, {genre}, {country_code}")
            continue

        count += 1

    result_df: pd.DataFrame = pd.concat(result_ls, ignore_index=True)
    result_df = apply_follower_stream_listeners_filters_and_drop_duplicates(df=result_df)

    result_df = drop_songs_that_appeared_in_past(result_df)
    # result_df = add_past_appearances_to_df(result_df)

    # Reorder the columns
    result_df = result_df[chart_columns]
    result_df.to_csv("chart_scrape.csv", index=False)
    logger.info(f"Finished scraping {len(result_df)} songs from charts".center(50, "-"))

    return process_scrape_output(result_df, "chart")