import os

import dotenv

dotenv.load_dotenv()
AIDAN_EMAIL = os.getenv("AIDAN_EMAIL")
JON_EMAIL = os.getenv("JON_EMAIL")
BASE_API_URL = os.getenv("API_URL")
id = os.getenv("ID")
key = os.getenv("KEY")
credentials = {
    "x-app-id": id,
    "x-api-key": key,
}
cronitor_api_key = os.getenv("CRONITOR_API_KEY")
SENDER_EMAIL=os.getenv("SENDER_EMAIL")
SMTP_KEY=os.getenv("SMTP_KEY")