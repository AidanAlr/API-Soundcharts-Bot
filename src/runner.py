import os

import cronitor
import pandas as pd

from src import input_lists
from src.charts.charts import run_charts_scrape
from src.credentials_key_info import cronitor_api_key
from src.general_ranking.general_ranking import run_general_ranking_scrape, get_song_ranking
from src.my_email import send_email
from src.playlists.playlists import run_playlist_scrape, get_available_tracklisting_dates
from src.watchlist import global_label_watchlist_df_list, concat_filter_process_label_watchlist_df
from src.charts.charts import get_uuid_toc_streams_for_songs_on_chart
# Create a dictionary containing the credentials
cronitor.api_key = cronitor_api_key
cronitor.Monitor.put(
    key="Jon-Song-Scrape",
    type="job",
)


def send_daily_email(tuples, label_watchlist_df, aidan_only=False):
    aidan_email = os.getenv("AIDAN_EMAIL")
    jon_email = os.getenv("JON_EMAIL")
    spreadsheet_url = os.getenv("SPREADSHEET_URL")

    message = f"SoundCharts results have been updated in the google sheet:\n{spreadsheet_url}\n"

    for sheet_name, df, spotify_url in tuples:
        message += f"\n{sheet_name} -> collected {len(df)} songs.\n"
        message += f"Listen here: {spotify_url}\n"

    if label_watchlist_df is not None:
        message += f"\nLabel Watchlist -> {len(label_watchlist_df)} songs"

    today_date = str(pd.Timestamp.now().date())
    subject = f"SoundCharts Scrape {today_date}"

    send_email(recipient=aidan_email,
               subject=subject,
               message=message,
               attachment_paths=[])

    if not aidan_only:
        send_email(recipient=jon_email,
                   subject=subject,
                   message=message,
                   attachment_paths=[])


@cronitor.job("Jon-Song-Scrape")
def run_all_scrapes() -> None:
    playlist_sheet_name, playlist_df, playlist_spotify_url = run_playlist_scrape(input_lists.playlist_list)
    chart_sheet_name, chart_df, chart_spotify_url = run_charts_scrape(input_lists.platform_genre_country_chart_tuples)
    general_ranking_name, general_ranking_df, general_ranking_spotify_url = run_general_ranking_scrape(on=False)

    label_watchlist_df = concat_filter_process_label_watchlist_df(global_label_watchlist_df_list)

    scrape_name_df_playlist_tuples = [
        (playlist_sheet_name, playlist_df, playlist_spotify_url),
        (chart_sheet_name, chart_df, chart_spotify_url),
        (general_ranking_name, general_ranking_df, general_ranking_spotify_url)
    ]

    scrape_name_df_playlist_tuples = [scrape for scrape in scrape_name_df_playlist_tuples if scrape[1] is not None]

    send_daily_email(scrape_name_df_playlist_tuples, label_watchlist_df, aidan_only=False)


if __name__ == "__main__":
    run_all_scrapes()

