"""
Contains some metadata about the NASA NEX GDDP datasets
"""

from datetime import datetime

ALL_MODELS = ['ACCESS1-0',
              'BNU-ESM',
              'CCSM4',
              'CESM1-BGC',
              'CNRM-CM5',
              'CSIRO-Mk3-6-0',
              'CanESM2',
              'GFDL-CM3',
              'GFDL-ESM2G',
              'GFDL-ESM2M',
              'IPSL-CM5A-LR',
              'IPSL-CM5A-MR',
              'MIROC-ESM-CHEM',
              'MIROC-ESM',
              'MIROC5',
              'MPI-ESM-LR',
              'MPI-ESM-MR',
              'MRI-CGCM3',
              'NorESM1-M',
              'bcc-csm1-1',
              'inmcm4']

BASE_TIMES = {
    'CCSM4': datetime(2005, 1, 1),
    'CESM1-BGC': datetime(2005, 1, 1),
    'CanESM2': datetime(1850, 1, 1),
    'GFDL-CM3': datetime(2006, 1, 1),
    'GFDL-ESM2G': datetime(2006, 1, 1),
    'GFDL-ESM2M': datetime(2006, 1, 1),
    'MIROC-ESM-CHEM': datetime(1850, 1, 1),
    'MIROC-ESM': datetime(1850, 1, 1),
    'MIROC5': datetime(1850, 1, 1),
    'MPI-ESM-LR': datetime(1850, 1, 1),
    'MPI-ESM-MR': datetime(1850, 1, 1)
}

AVERAGE_MB = 796000000 / 1024 / 1024
