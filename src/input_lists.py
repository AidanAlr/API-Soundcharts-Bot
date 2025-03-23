from src.sheets import (get_playlist_url_list_from_df,
                        get_artist_blocklist_from_df,
                        get_label_blocklist_from_df,
                        get_label_watchlist_from_df,
                        get_list_of_platform_genre_country_chart_tuples_from_df,
                        get_max_artists_on_track_from_df,
                        get_max_spotify_followers_from_df,
                        get_max_streams_from_df,
                        get_max_streams_on_song_in_catalogue_from_df,
                        get_minimum_spotify_followers_if_100k_monthly_listeners_from_df,
                        get_x_percent_of_streams_are_from_one_day_in_last_14_days_from_df,
                        get_minimum_average_streams_if_above_0_from_df,
                        get_max_tiktok_followers_from_df,
                        get_song_blocklist_urls_from_df,
                        get_period_from_df,
                        get_sort_by_from_df,
                        get_ranking_max_total_streams,
                        get_ranking_min_total_streams,
                        get_max_percent_change_in_total_streams_over_period_from_df,
                        get_min_percent_change_in_total_streams_over_period_from_df,
                        get_ranking_pages_to_collect,
                        )
from src.sheets_utils import get_inputs_worksheet_as_df

# Get the dataframe once
inputs_df = get_inputs_worksheet_as_df()

# Create a dictionary for all the extracted data
data = {
    "label_watchlist": get_label_watchlist_from_df(inputs_df),
    "label_blocklist": get_label_blocklist_from_df(inputs_df),
    "artist_blocklist": get_artist_blocklist_from_df(inputs_df),
    "song_blocklist_urls": get_song_blocklist_urls_from_df(inputs_df),
    "playlist_list": get_playlist_url_list_from_df(inputs_df),
    "platform_genre_country_chart_tuples": get_list_of_platform_genre_country_chart_tuples_from_df(inputs_df),
    "max_artists_on_track": get_max_artists_on_track_from_df(inputs_df),
    "max_spotify_followers": get_max_spotify_followers_from_df(inputs_df),
    "max_streams": get_max_streams_from_df(inputs_df),
    "min_average_streams_if_above_0": get_minimum_average_streams_if_above_0_from_df(inputs_df),
    "max_streams_on_song_in_catalogue": get_max_streams_on_song_in_catalogue_from_df(inputs_df),
    "max_tiktok_followers": get_max_tiktok_followers_from_df(inputs_df),
    "minimum_spotify_followers_if_100k_monthly_listeners": get_minimum_spotify_followers_if_100k_monthly_listeners_from_df(inputs_df),
    "max_percent_of_streams_on_one_day": get_x_percent_of_streams_are_from_one_day_in_last_14_days_from_df(inputs_df),
    "ranking_max_total_streams": get_ranking_max_total_streams(inputs_df),
    "ranking_min_total_streams": get_ranking_min_total_streams(inputs_df),
    "period": get_period_from_df(inputs_df),
    "max_change_in_total_streams_over_period": get_max_percent_change_in_total_streams_over_period_from_df(inputs_df),
    "min_change_in_total_streams_over_period": get_min_percent_change_in_total_streams_over_period_from_df(inputs_df),
    "sort_by": get_sort_by_from_df(inputs_df),
    "ranking_pages_to_collect": get_ranking_pages_to_collect(inputs_df),
}

# Access the data from the dictionary
label_watchlist = data["label_watchlist"]
label_blocklist = data["label_blocklist"]
artist_blocklist = data["artist_blocklist"]
song_blocklist_urls = data["song_blocklist_urls"]
playlist_list = data["playlist_list"]
platform_genre_country_chart_tuples = data["platform_genre_country_chart_tuples"]
max_artists_on_track = data["max_artists_on_track"]
max_spotify_followers = data["max_spotify_followers"]
max_streams = data["max_streams"]
min_average_streams_if_above_0 = data["min_average_streams_if_above_0"]
max_streams_on_song_in_catalogue = data["max_streams_on_song_in_catalogue"]
max_tiktok_followers = data["max_tiktok_followers"]
minimum_spotify_followers_if_100k_monthly_listeners = data["minimum_spotify_followers_if_100k_monthly_listeners"]
max_percent_of_streams_on_one_day = data["max_percent_of_streams_on_one_day"]
ranking_max_total_streams = data["ranking_max_total_streams"]
ranking_min_total_streams = data["ranking_min_total_streams"]
period = data["period"]
max_change_in_total_streams_over_period = data["max_change_in_total_streams_over_period"]
min_change_in_total_streams_over_period = data["min_change_in_total_streams_over_period"]
sort_by = data["sort_by"]
ranking_pages_to_collect = data["ranking_pages_to_collect"]
