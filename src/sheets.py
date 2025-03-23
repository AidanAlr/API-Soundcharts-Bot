import pandas as pd
from dotenv import load_dotenv

from src.sheets_utils import get_value_for_condition_and_assert, get_column_values_as_list_without_nan, \
    replace_name_with_code_and_expand_all_countries

load_dotenv()
pd.set_option('display.max_columns', None)


def get_playlist_url_list_from_df(df: pd.DataFrame) -> list:
    return get_column_values_as_list_without_nan(df, "playlist_url")


def get_song_blocklist_urls_from_df(df: pd.DataFrame) -> list:
    return get_column_values_as_list_without_nan(df, "song_blocklist_urls")


def get_label_watchlist_from_df(df: pd.DataFrame) -> list:
    return get_column_values_as_list_without_nan(df, "label_watchlist")


def get_label_blocklist_from_df(df: pd.DataFrame) -> list:
    ls = get_column_values_as_list_without_nan(df, "label_blocklist")
    # Convert all values to strings
    return [str(x) for x in ls]


def get_artist_blocklist_from_df(df: pd.DataFrame) -> list:
    return get_column_values_as_list_without_nan(df, "artist_blocklist")


def get_max_spotify_followers_from_df(df: pd.DataFrame) -> int:
    return get_value_for_condition_and_assert(df, "max_spotify_followers")


def get_max_artists_on_track_from_df(df: pd.DataFrame) -> int:
    return get_value_for_condition_and_assert(df, "max_artists_on_track")


def get_max_streams_from_df(df: pd.DataFrame) -> int:
    return get_value_for_condition_and_assert(df, "max_streams")


def get_max_streams_on_song_in_catalogue_from_df(df: pd.DataFrame) -> int:
    return get_value_for_condition_and_assert(df, "max_streams_on_song_in_catalogue")


def get_max_tiktok_followers_from_df(df: pd.DataFrame) -> int:
    return get_value_for_condition_and_assert(df, "max_tiktok_followers")


def get_minimum_spotify_followers_if_100k_monthly_listeners_from_df(df: pd.DataFrame) -> int:
    return get_value_for_condition_and_assert(df, "minimum_spotify_followers_if_100k_monthly_listeners")


def get_x_percent_of_streams_are_from_one_day_in_last_14_days_from_df(df: pd.DataFrame) -> int:
    return df["x_percent_of_streams_are_from_one_day_in_last_14_days"].iloc[0]


def get_minimum_average_streams_if_above_0_from_df(df: pd.DataFrame) -> int:
    return get_value_for_condition_and_assert(df, "minimum_average_streams_if_above_0")


def get_ranking_max_total_streams(df: pd.DataFrame) -> int:
    return get_value_for_condition_and_assert(df, "ranking_max_total_streams")


def get_ranking_min_total_streams(df: pd.DataFrame) -> int:
    return get_value_for_condition_and_assert(df, "ranking_min_total_streams")


def get_period_from_df(df: pd.DataFrame) -> str:
    return df["period"].iloc[0]


def get_max_percent_change_in_total_streams_over_period_from_df(df: pd.DataFrame) -> int:
    return get_value_for_condition_and_assert(df, "max_change_in_total_streams_over_period")


def get_min_percent_change_in_total_streams_over_period_from_df(df: pd.DataFrame) -> int:
    return get_value_for_condition_and_assert(df, "min_change_in_total_streams_over_period")


def get_sort_by_from_df(df: pd.DataFrame) -> str:
    return df["sort_by"].iloc[0]


def get_ranking_pages_to_collect(df: pd.DataFrame) -> int:
    return df["ranking_pages_to_collect"].iloc[0]


def get_list_of_platform_genre_country_chart_tuples_from_df(df: pd.DataFrame) -> list[tuple[str, str, str]]:
    platform_genre_country_df: pd.DataFrame = df[['platform', 'genre', 'country']].dropna()
    tuple_list: list[tuple[str, str, str]] = list(platform_genre_country_df.itertuples(index=False, name=None))

    result: list[tuple[str, str, str]] = []
    for platform, genre, country in tuple_list:
        parts = [part.capitalize() for part in country.split("-")]
        correct_country_formatting = " ".join(parts)
        result.append((platform, genre, correct_country_formatting))

    result = replace_name_with_code_and_expand_all_countries(result)

    return result
