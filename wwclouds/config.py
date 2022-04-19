import os
import multiprocessing as mp

CUR_DIR = os.path.dirname(os.path.realpath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(CUR_DIR, '..'))

DATA_PATH = f"{CUR_DIR}/data"
DATA_PATH_MAPNIK = f"{DATA_PATH}/mapnik"
DATA_PATH_WORLD_MAP = f"{DATA_PATH_MAPNIK}/world_map"
DATA_PATH_PRODUCT = f"{DATA_PATH}/product"
DATA_PATH_DATASETS = f"{DATA_PATH}/datasets"
DATA_PATH_DOWNLOADS = f"{DATA_PATH}/downloads"

DATA_PATH_SATPY = f"{DATA_PATH}/satpy"
DATA_PATH_SATPY_RESAMPLE_CACHE = f"{DATA_PATH_SATPY}/resample_cache"

METEOSAT_API_ENDPOINT = "https://api.eumetsat.int"
METEOSAT_DOWNLOAD_ENDPOINT = f"{METEOSAT_API_ENDPOINT}/data/download"
METEOSAT_BROWSE_ENDPOINT = f"{METEOSAT_API_ENDPOINT}/data/browse"

METEOSAT_CONSUMER_KEY = "W4UfXCVUrkqcio3PWn7dWputpcwa"
METEOSAT_CONSUMER_SECRET = "MJZE2lAYnvDiaSUaKH3d13Ac1Jsa"

CPU_COUNT = mp.cpu_count()
