import pandas as pd

from src.filters import is_english
from src.input_lists import max_artists_on_track, max_spotify_followers, max_streams, max_streams_on_song_in_catalogue, \
    max_tiktok_followers, minimum_spotify_followers_if_100k_monthly_listeners, \
    max_percent_of_streams_on_one_day, label_blocklist, label_watchlist, artist_blocklist, playlist_list, \
    platform_genre_country_chart_tuples
from src.playlists.playlists import add_accurate_date_added_to_columns, remove_songs_not_added_on_latest_crawl_date


def test_conditions_return_integers():
    """
    Test that all derived max/summary variables are integers.
    """

    # List of variables to check
    variables = [
        max_artists_on_track,
        max_spotify_followers,
        max_streams,
        max_streams_on_song_in_catalogue,
        max_tiktok_followers,
        minimum_spotify_followers_if_100k_monthly_listeners,
        max_percent_of_streams_on_one_day
    ]

    # Check each variable
    for var in variables:
        assert isinstance(var, int), f"Variable {var} is not an integer"


def run_list_assertions(list_name, list_var):
    """
    Run assertions on a list.
    """
    assert isinstance(list_var, list), f"{list_name} is not a list"
    assert all(isinstance(x, str) for x in list_var), f"{list_name} contains non-string elements"
    assert list_var, f"{list_name} is empty"


def test_label_blocklist():
    """
    Test that label blocklist is a list.
    """
    run_list_assertions("label_blocklist", label_blocklist)


def test_playlist_list():
    """
    Test that playlist list is a list.
    """
    run_list_assertions("playlist_list", playlist_list)


def test_artist_blocklist():
    """
    Test that artist blocklist is a list.
    """
    run_list_assertions("artist_blocklist", artist_blocklist)


def test_label_watchlist():
    """
    Test that label watchlist is a list.
    """
    run_list_assertions("label_watchlist", label_watchlist)


def test_platform_genre_country_tuples():
    """
    Test that platform_genre_country_tuples is a list of tuples.
    """
    assert isinstance(platform_genre_country_chart_tuples, list), "platform_genre_country_chart_tuples is not a list"
    assert all(isinstance(x, tuple) for x in
               platform_genre_country_chart_tuples), "platform_genre_country_chart_tuples contains non-tuple elements"
    assert platform_genre_country_chart_tuples, "platform_genre_country_chart_tuples is empty"

    # Check that each tuple has 3 elements
    for tup in platform_genre_country_chart_tuples:
        assert len(tup) == 3, f"Tuple {tup} does not have 3 elements"


def test_add_accurate_date_added_to_columns():
    data = {
        'song_uuid': ['1',
                      '1',
                      '2',
                      '2',
                      '3',
                      '4',
                      '4'],
        'playlist_crawl_date': [
            '2023-10-10',
            '2023-10-09',
            '2023-10-05',
            '2023-10-04',
            '2023-10-03',
            '2023-10-02',
            '2023-10-01'
        ]
    }

    df = pd.DataFrame(data)
    result_df = add_accurate_date_added_to_columns(df)

    assert all(result_df[result_df['song_uuid'] == '1']['date_added'] == pd.Timestamp('2023-10-09').date())
    assert all(result_df[result_df['song_uuid'] == '2']['date_added'] == pd.Timestamp('2023-10-04').date())
    assert all(result_df[result_df['song_uuid'] == '3']['date_added'] == pd.Timestamp('2023-10-03').date())
    assert all(result_df[result_df['song_uuid'] == '4']['date_added'] == pd.Timestamp('2023-10-01').date())

    # Additional assertion to ensure the date_added column exists
    assert 'date_added' in result_df.columns

    # Optional: Check that the number of rows remains the same
    assert len(result_df) == len(df)


def test_get_songs_added_to_playlist_in_last_day():
    data = {
        'song_uuid': ['1',
                      '1',
                      '2',
                      '2',
                      '3',
                      '4'],
        'playlist_crawl_date': [
            '2023-10-10',
            '2023-10-09',
            '2023-10-05',
            '2023-10-04',
            '2023-10-10',
            '2023-10-04'
        ]
    }

    tracklist_df = pd.DataFrame(data)

    tracklist_df = remove_songs_not_added_on_latest_crawl_date(tracklist_df)

    assert all(tracklist_df['date_added'] == pd.Timestamp('2023-10-10').date())
    assert len(tracklist_df) == 1
    assert all(tracklist_df['song_uuid'] == '3')


def test_is_english():
    """Test cases for is_english_text function"""
    # Valid cases
    assert is_english("Hello, World! 123") is True
    assert is_english("") is True
    assert is_english("@#$%^&*()") is True

    # Invalid cases
    assert is_english("café") is False  # accented e
    assert is_english("привет") is False  # Cyrillic
    assert is_english("über") is False  # German umlaut
    assert is_english("なに") is False  # Japanese
    assert is_english(None) is False  # Non-string input
    assert is_english(123) is False  # Non-string input
    assert is_english('Вор замочек открывает') is False  # Russian

    print("All tests passed!")
