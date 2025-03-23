import pandas as pd

from src.logging_config import logger


def filter_charts_by_doc(doc, uuid_toc_streams: list[tuple[str, int, int]]) -> list[tuple[str, int, int]]:
    """
    Filter out songs with more than doc days on chart
    """
    result: set[tuple[str, int, int]] = set()
    for uuid, toc, streams in uuid_toc_streams:
        if toc <= doc:
            result.add((uuid, toc, streams))

    return list(result)


def get_filtered_sluglist(slug_list: list[str], keyword_list: list[str], exclusion_list: list[str]) -> list[str]:
    result = [
        slug for slug in slug_list
        if any(keyword.lower() in slug.lower() for keyword in keyword_list) and
           all(exclusion.lower() not in slug.lower() for exclusion in exclusion_list)
    ]
    logger.debug(f"Got chart names containing {keyword_list}: {result}")
    return result


def set_extra_values_for_df(df: pd.DataFrame, country_code: str, country_name: str, platform: str, uuid: str, toc: int,
                            slug: str):
    df["country"] = country_name
    df["country_code"] = country_code
    df["platform"] = platform
    df["song_uuid"] = uuid
    df["time_on_chart"] = toc
    df["chart_slug"] = slug
    return df


def translate_input_genre_into_keywords_and_exlusion_list(genre: str) -> tuple[list[str], list[str]]:
    match genre:
        case "alternative":
            return ["alternativ"], []
        case "all-genres":
            return ["top-200"], []
        case "pop":
            return ["pop"], ["j-pop", "k-pop", "french-pop"]
        case _:
            return [genre], []


def get_uuid_toc_streams_tuples_from_items(items: list[dict]) -> list[tuple[str, int, int]]:
    if not items:
        logger.debug("No items in chart! Returning empty list")
        return []

    return [
        (item["song"]["uuid"], item["timeOnChart"], item["metric"])
        for item in items
    ]


# spotify_viral_country_name_to_code_dict = {
#     "Argentina": "AR",
#     "Australia": "AU",
#     "Austria": "AT",
#     "Belarus": "BY",
#     "Belgium": "BE",
#     "Bolivia": "BO",
#     "Brazil": "BR",
#     "Bulgaria": "BG",
#     "Canada": "CA",
#     "Chile": "CL",
#     "Colombia": "CO",
#     "Costa Rica": "CR",
#     "Cyprus": "CY",
#     "Czech Republic": "CZ",
#     "Denmark": "DK",
#     "Dominican Republic": "DO",
#     "Ecuador": "EC",
#     "Egypt": "EG",
#     "El Salvador": "SV",
#     "Estonia": "EE",
#     "Finland": "FI",
#     "France": "FR",
#     "Germany": "DE",
#     "Greece": "GR",
#     "Guatemala": "GT",
#     "Honduras": "HN",
#     "Hong Kong": "HK",
#     "Hungary": "HU",
#     "Iceland": "IS",
#     "India": "IN",
#     "Indonesia": "ID",
#     "Ireland": "IE",
#     "Israel": "IL",
#     "Italy": "IT",
#     "Japan": "JP",
#     "Latvia": "LV",
#     "Kazakhstan": "KZ",
#     "Lithuania": "LT",
#     "Luxembourg": "LU",
#     "Malaysia": "MY",
#     "Mexico": "MX",
#     "Morocco": "MA",
#     "Netherlands": "NL",
#     "New Zealand": "NZ",
#     "Nicaragua": "NI",
#     "Norway": "NO",
#     "Nigeria": "NG",
#     "Pakistan": "PK",
#     "Panama": "PA",
#     "Paraguay": "PY",
#     "Peru": "PE",
#     "Philippines": "PH",
#     "Poland": "PL",
#     "Portugal": "PT",
#     "Romania": "RO",
#     "Singapore": "SG",
#     "Slovakia": "SK",
#     "South Korea": "KR",
#     "South Africa": "ZA",
#     "Spain": "ES",
#     "Sweden": "SE",
#     "Switzerland": "CH",
#     "Taiwan": "TW",
#     "Thailand": "TH",
#     "Turkey": "TR",
#     "Ukraine": "UA",
#     "United Arab Emirates": "AE",
#     "United Kingdom": "GB",
#     "United States": "US",
#     "Uruguay": "UY",
#     "Vietnam": "VN",
#     "Venezuela": "VE"
# }
country_name_to_code_dict = {'Algeria': 'DZ',
                             'Angola': 'AO',
                             'Anguilla': 'AI', 'Antigua & Barbuda': 'AG', 'Argentina': 'AR', 'Armenia': 'AM',
                             'Australia': 'AU',
                             'Austria': 'AT', 'Azerbaijan': 'AZ', 'Bahamas': 'BS', 'Bahrain': 'BH', 'Barbados': 'BB',
                             'Belarus': 'BY', 'Belgium': 'BE', 'Belize': 'BZ', 'Benin': 'BJ', 'Bermuda': 'BM',
                             'Bhutan': 'BT',
                             'Bolivia': 'BO', 'Botswana': 'BW', 'Brazil': 'BR', 'British Virgin Islands': 'VG',
                             'Bulgaria': 'BG',
                             'Cambodia': 'KH', 'Cameroon': 'CM', 'Canada': 'CA', 'Cape Verde': 'CV',
                             'Cayman Islands': 'KY',
                             'Chad': 'TD', 'Chile': 'CL', 'China': 'CN', 'Colombia': 'CO', 'Congo - Brazzaville': 'CG',
                             'Congo - Kinshasa': 'CD', 'Costa Rica': 'CR', 'Côte d’Ivoire': 'CI', 'Croatia': 'HR',
                             'Cyprus': 'CY',
                             'Czechia': 'CZ', 'Denmark': 'DK', 'Dominica': 'DM', 'Dominican Republic': 'DO',
                             'Ecuador': 'EC',
                             'Egypt': 'EG', 'El Salvador': 'SV', 'Estonia': 'EE', 'Eswatini': 'SZ', 'Fiji': 'FJ',
                             'Finland': 'FI',
                             'France': 'FR', 'Gambia': 'GM', 'Germany': 'DE', 'Ghana': 'GH', 'Greece': 'GR',
                             'Grenada': 'GD',
                             'Guatemala': 'GT', 'Guinea-Bissau': 'GW', 'Guyana': 'GY', 'Honduras': 'HN',
                             'Hong Kong SAR China': 'HK', 'Hungary': 'HU', 'Iceland': 'IS', 'India': 'IN',
                             'Indonesia': 'ID',
                             'Ireland': 'IE', 'Israel': 'IL', 'Italy': 'IT', 'Jamaica': 'JM', 'Japan': 'JP',
                             'Jordan': 'JO',
                             'Kazakhstan': 'KZ', 'Kenya': 'KE', 'Kuwait': 'KW', 'Kyrgyzstan': 'KG', 'Laos': 'LA',
                             'Latvia': 'LV',
                             'Lebanon': 'LB', 'Liberia': 'LR', 'Libya': 'LY', 'Lithuania': 'LT', 'Luxembourg': 'LU',
                             'Morocco': 'MA',
                             'Macao SAR China': 'MO', 'Madagascar': 'MG', 'Malawi': 'MW', 'Malaysia': 'MY',
                             'Maldives': 'MV',
                             'Mali': 'ML', 'Malta': 'MT', 'Mauritius': 'MU', 'Mexico': 'MX', 'Micronesia': 'FM',
                             'Moldova': 'MD',
                             'Mongolia': 'MN', 'Montserrat': 'MS', 'Mozambique': 'MZ', 'Myanmar (Burma)': 'MM',
                             'Namibia': 'NA',
                             'Nepal': 'NP', 'Netherlands': 'NL', 'New Zealand': 'NZ', 'Nicaragua': 'NI', 'Niger': 'NE',
                             'Nigeria': 'NG', 'North Macedonia': 'MK', 'Norway': 'NO', 'Oman': 'OM', 'Panama': 'PA',
                             'Papua New Guinea': 'PG', 'Paraguay': 'PY', 'Peru': 'PE', 'Philippines': 'PH',
                             'Poland': 'PL',
                             'Portugal': 'PT', 'Puerto Rico': 'PR', 'Qatar': 'QA', 'Romania': 'RO', 'Russia': 'RU',
                             'Saudi Arabia': 'SA', 'Senegal': 'SN', 'Serbia': 'RS', 'Seychelles': 'SC',
                             'Sierra Leone': 'SL',
                             'Singapore': 'SG', 'Slovakia': 'SK', 'Slovenia': 'SI', 'Solomon Islands': 'SB',
                             'South Africa': 'ZA',
                             'South Korea': 'KR', 'Spain': 'ES', 'Sri Lanka': 'LK', 'St. Kitts & Nevis': 'KN',
                             'St. Lucia': 'LC',
                             'St. Vincent & Grenadines': 'VC', 'Suriname': 'SR', 'Sweden': 'SE', 'Switzerland': 'CH',
                             'Taiwan': 'TW', 'Tajikistan': 'TJ', 'Tanzania': 'TZ', 'Thailand': 'TH',
                             'Trinidad & Tobago': 'TT',
                             'Tunisia': 'TN', 'Turkey': 'TR', 'Turkmenistan': 'TM', 'Turks & Caicos Islands': 'TC',
                             'Uganda': 'UG', 'Ukraine': 'UA', 'United Arab Emirates': 'AE', 'United Kingdom': 'GB',
                             'United States': 'US', 'Uruguay': 'UY', 'Uzbekistan': 'UZ', 'Venezuela': 'VE',
                             'Vietnam': 'VN',
                             'Yemen': 'YE', 'Zimbabwe': 'ZW',
                             "Czech Republic": "CZ",
                             "Pakistan": "PK",
                             }
country_code_to_name_dict = {value: key for key, value in country_name_to_code_dict.items()}


def parse_chart_codes():
    from bs4 import BeautifulSoup

    html_snippet = """"""  # Replace with the actual HTML snippet
    soup = BeautifulSoup(html_snippet, 'html.parser')

    country_dict = {}
    for div in soup.find_all('div', class_='sc-csuQGl ewgYJK'):
        country_id = div.get('id').replace('id-', '')
        country_name = div.find('div', class_='sc-jDwBTQ cKAkjH').get('title')
        country_dict[country_name] = country_id

    print(country_dict)
