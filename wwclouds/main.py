import matplotlib.pyplot as plt
import satpy
from satpy.utils import check_satpy
from glob import glob

check_satpy(readers=["seviri_l1b_hrit"])

from satpy.writers import get_enhanced_image
from dask.diagnostics import ProgressBar
from pyresample import create_area_def

# goes_type = noaa_goes.NoaaGoesType.GOES17
# goes = noaa_goes.NoaaGoes(goes_type)

# scn = goes.get_scene()

print()

# scn = satpy.Scene(filenames=glob("/home/joachim/Downloads/HS_H08_20220110_0000_B01_FLDK_R10_*.DAT"), reader="ahi_hsd")
scn = satpy.Scene(filenames=["/home/joachim/Documents/BachelorProject/wwclouds/data/metosat11/EO:EUM:DAT:MSG:HRSEVIRI/MSG4-SEVI-MSG15-0100-NA-20220111221242.174000000Z-NA.nat"])
print(scn.available_dataset_names())
print(scn.available_composite_names())
print(scn.all_dataset_ids(composites=True))
dataset = "colorized_ir_clouds"
scn.load([dataset])
newscn = scn.resample(scn.min_area())
print(newscn[dataset].attrs["area"])


print(newscn.available_dataset_names())
# dataset = "colorized_ir_clouds"
plt.figure()
# plt.imshow(newscn[dataset])
# plt.colorbar()
# plt.show()

img = get_enhanced_image(newscn[dataset])
img.data.plot.imshow(vmin=0, vmax=1, rgb="bands")

plt.show()
