import distutils
from distutils.core import setup
import glob

bin_files = glob.glob("bin/*")

# The main call
setup(name='desdmfw_drp_appintg',
      version='0.0.1',
      license="GPL",
      description="Application integration codes for running the DRP pipeline in the DESDM framework",
      author="Michelle Gower",
      author_email="mgower@illinois.edu",
      scripts=bin_files,
      data_files=[('ups', ['ups/desdmfw_drp_appintg.table'])]
      )
