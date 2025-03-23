import requests_cache
import datetime

session = requests_cache.CachedSession(expire_after=datetime.timedelta(hours=12))