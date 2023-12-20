import os
from pathlib import Path

import dotenv

dotenv.load_dotenv()


ROOT_DIR = Path(__file__).resolve().parent.parent
GEO_API_KEY = os.getenv('GEO_API_KEY')

SRC_RESTAURANT_PATH = ROOT_DIR / 'data/restaurant'
SRC_WEATHER_PATH = ROOT_DIR / 'data/weather'
DST_PATH = ROOT_DIR / 'data/weather_restaurant'
