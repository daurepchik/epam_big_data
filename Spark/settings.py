import os
from pathlib import Path

import dotenv

dotenv.load_dotenv()


ROOT_DIR = Path(__file__).resolve().parent.parent
GEO_API_KEY = os.getenv('GEO_API_KEY', None)
if not GEO_API_KEY:
    raise ValueError('GEO_API_KEY is required')

SRC_RESTAURANT_PATH = ROOT_DIR / 'data' / 'restaurant'
SRC_WEATHER_PATH = ROOT_DIR / 'data' / 'weather'
DST_PATH = ROOT_DIR / 'data' / 'weather_restaurant'

if not SRC_RESTAURANT_PATH.exists():
    raise FileNotFoundError(f'{SRC_RESTAURANT_PATH} does not exist')
elif not SRC_WEATHER_PATH.exists():
    raise FileNotFoundError(f'{SRC_WEATHER_PATH} does not exist')
