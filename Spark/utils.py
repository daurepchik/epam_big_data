import re
from pathlib import Path
from zipfile import ZipFile

from geolib import geohash
from opencage.geocoder import OpenCageGeocode
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, DoubleType, StringType

from settings import GEO_API_KEY, RAW_WEATHER_PATH, SRC_WEATHER_PATH

geocoder = OpenCageGeocode(GEO_API_KEY)


def unzip_files(src_path: Path, dst_path: Path) -> None:
    """
    Unzips files from the source path to the destination path
    :param src_path: The Path object pointing to the directory containing zip files
    :param dst_path: The Path object pointing to the directory where the files will be extracted
    :return:
    """
    # Creating the destination directory if it doesn't exist
    dst_path.mkdir(parents=True, exist_ok=True)
    # Iterating through the folder with the zip files and extracting them one by one to the destination folder
    if not src_path.exists():
        raise FileNotFoundError('Source files does not exist')
    for file in src_path.iterdir():
        zip_file = ZipFile(file)
        zip_members = zip_file.namelist()
        members_to_extract = list(filter(lambda member: re.match(r'[^._]', Path(member).name), zip_members))
        zip_file.extractall(path=dst_path, members=members_to_extract)


@udf(returnType=ArrayType(DoubleType()))
def fill_null_lat_lng(city: StringType(), country: StringType()) -> ArrayType(DoubleType()):
    """
    User Defined Function to fill null latitude and longitude values based on city and country.
    :param city: Name of the city
    :param country: Name of the country
    :return: array containing latitude and longitude coordinates obtained from the geocoding service
    """
    try:
        response = geocoder.geocode(f'{city},{country}', limit=1)
        geometry = response[0]['geometry']
        new_lat, new_lng = geometry['lat'], geometry['lng']
    except Exception as e:
        print('Something went wrong')
        raise e
    else:
        return new_lat, new_lng


@udf()
def get_geohash(lat: DoubleType(), lng: DoubleType()) -> StringType():
    """
    User Defined Function to generate a 4 character long geohash based on latitude and longitude coordinates
    :param lat: Latitude coordinate
    :param lng: Longitude coordinate
    :return: 4 character long geohash string
    """
    return geohash.encode(lat, lng, 4)


if __name__ == '__main__':
    unzip_files(RAW_WEATHER_PATH, SRC_WEATHER_PATH)
