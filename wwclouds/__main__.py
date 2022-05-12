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
    ProductCreator.from_args().create_products()
