import matplotlib.pyplot as plt
import satpy
from satpy.utils import check_satpy
from glob import glob

# check_satpy(readers=["ahi_hsd"])

from satellite import noaa_goes
from satpy.writers import get_enhanced_image
from dask.diagnostics import ProgressBar
from pyresample import create_area_def

# goes_type = noaa_goes.NoaaGoesType.GOES17
# goes = noaa_goes.NoaaGoes(goes_type)

# scn = goes.get_scene()

print()

# scn = satpy.Scene(filenames=glob("/home/joachim/Downloads/HS_H08_20220110_0000_B01_FLDK_R10_*.DAT"), reader="ahi_hsd")
scn = satpy.Scene(filenames=glob("/home/joachim/Documents/BachelorProject/wwclouds/data/noaa-goes17/1641830400_0.nc"), reader="abi_l1b")
dataset = "C01"
scn.load([dataset])
print(scn[dataset].attrs["area"])


print(scn.available_dataset_names())
# dataset = "colorized_ir_clouds"
plt.figure()
plt.imshow(scn[dataset])
plt.colorbar()
plt.show()
