import numpy as np
import pandas as pd
from gspread_dataframe import get_as_dataframe, set_with_dataframe

import src.sheets_utils as sheets_utils
from src import utils
from src.logging_config import logger
from src.song_info import get_song_audience_from_date, get_song_metadata


def get_last_year_audience_for_list_of_songs(uuid_start: list[tuple[str, str]]) -> dict[str, pd.DataFrame]:
    result_dict = {}
    for uuid, oldest_day_to_collect in uuid_start:

        if pd.isna(oldest_day_to_collect) or oldest_day_to_collect == "release":
            song_release_date = get_song_metadata(uuid).releaseDate
            song_release_date = str(pd.to_datetime(song_release_date).date())
            # song_name = get_song_metadata(uuid).name
            # print(song_name, song_release_date)
            oldest_day_to_collect = song_release_date

        df = get_song_audience_from_date(uuid, oldest_day_to_collect)

        # Set the index to date column
        df.set_index("date", inplace=True)
        result_dict[uuid] = df

    return result_dict


def get_input_song_uuids_and_start_date_from_sheets() -> list[tuple[str, str]]:
    spreadsheet = sheets_utils.get_spreadsheet_with_gspread()
    report_sheet = spreadsheet.worksheet("report")
    df = get_as_dataframe(report_sheet)
    df = df[["song_url", "start_date"]]
    df = df.dropna(subset=["song_url"])
    url_start_date_tuples = list(df.itertuples(index=False, name=None))
    uuid_start_date_tuples = [(utils.get_uuid_from_url(url), start_date) for url, start_date in url_start_date_tuples]
    logger.info("Got input song uuids and start date from sheets")
    logger.info(uuid_start_date_tuples)
    return uuid_start_date_tuples


def calculate_total_streams(df):
    """
    Calculate a column of total streams up to each date

    Parameters:
    -----------
    df : pandas.DataFrame
        DataFrame with a date index and cumulative_daily_streams column

    Returns:
    --------
    pandas.DataFrame
        DataFrame with an additional total_streams column
    """

    # Sort the index to ensure chronological order
    df_sorted = df.sort_index()

    # Calculate cumulative sum of daily streams
    df_sorted['total_streams'] = df_sorted['cumulative_daily_streams'].cumsum()

    return df_sorted


def combine_song_dataframes(dataframes: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Combine multiple song DataFrames into a single comprehensive DataFrame.

    Parameters:
    -----------
    dataframes : dict
        A dictionary where keys are song names and values are pandas DataFrames.
        Each DataFrame should have date as the index and a 'daily_streams' column.

    Returns:
    --------
    pandas.DataFrame: Combined DataFrame with:
    - Individual daily streams for each song (renamed to song names)
    - Cumulative daily streams across all songs
    - Cumulative total streams across all songs

    Example:
    --------
    song_dfs = {
        'Song1': df1,
        'Song2': df2,
        # ... more song DataFrames
    }
    combined_df = combine_song_dataframes(song_dfs)
    """
    # Validate input
    if not isinstance(dataframes, dict) or len(dataframes) == 0:
        raise ValueError("Input must be a non-empty dictionary of DataFrames")

    # Find all unique dates across all DataFrames
    all_dates = sorted(set().union(*[df.index for df in dataframes.values()]))

    # Create a list to store processed DataFrames
    daily_stream_dfs = []

    for uuid, df in dataframes.items():
        # Reindex DataFrame to include all dates, filling missing values with 0
        reindexed_df = df.reindex(all_dates, fill_value=np.nan)

        # Get the song name from the UUID
        song_name = get_song_metadata(uuid).name
        # Keep only the 'daily_streams' column and rename it to the song name
        daily_streams = reindexed_df['daily_streams'].rename(song_name + ' daily_streams')
        # Append to the list of processed DataFrames
        daily_stream_dfs.append(daily_streams)

    # Combine all individual daily streams into one DataFrame
    df = pd.concat(daily_stream_dfs, axis=1)

    # Calculate cumulative daily streams across all songs
    df['cumulative_daily_streams'] = df.sum(axis=1)

    df = calculate_total_streams(df)

    return df


def make_report():
    uuid_start_tuples: list[tuple[str, str]] = get_input_song_uuids_and_start_date_from_sheets()
    last_year_audience = get_last_year_audience_for_list_of_songs(uuid_start_tuples)
    combined_df = combine_song_dataframes(last_year_audience)

    return combined_df


def set_report_data_on_sheets():
    spreadsheet = sheets_utils.get_spreadsheet_with_gspread()
    report_data_sheet = spreadsheet.worksheet("report_data")

    df = make_report()
    set_with_dataframe(report_data_sheet, df, resize=True, include_index=True)
    logger.info("Set report data on sheets")


if __name__ == "__main__":
    set_report_data_on_sheets()
