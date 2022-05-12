import sys
import warnings

from config import ROOT_DIR
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

if not sys.warnoptions:
    warnings.simplefilter("ignore")

from wwclouds.domains.product import ProductCreator


if __name__ == '__main__':
    from datetime import datetime
    ProductCreator.from_args(
        # utctime=datetime(2022, 2, 14, 9, 25).timestamp()
        # utctime=datetime.utcnow().timestamp()
        # utctime=datetime(2020, 12, 14, 19, 00).timestamp()
        # utctime=datetime(2022, 2, 13, 14, 25).timestamp()
        # utctime=datetime(2022, 4, 30, 11, 00).timestamp()
        utctime=datetime(2022, 5, 1, 14, 00).timestamp()
    ).create_products()
