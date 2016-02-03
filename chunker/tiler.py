#!/usr/bin/env python

import os
import math
from datetime import datetime, timedelta

import argparse

import numpy

os.environ['GDAL_NETCDF_BOTTOMUP'] = 'NO'
import rasterio
from rasterio._io import RasterReader

from util import log

DATE_FORMAT = '%Y%m%H%M%S'


def open_netCDF(path, subds=''):
    p = 'NETCDF:' + path
    if subds:
        p += ':' + subds
    s = RasterReader(p)
    s.start()
    return s


def get_affine(affine, col, row):
    (xmin, ymax) = affine * (0, 0)
    (tx, ty) = affine * (col, row)
    ta = affine.translation(tx - xmin, ty - ymax) * affine
    (ntx, nty) = ta * (0, 0)
    return ta


def tile(input_path,
         out_dir,
         subds='',
         s3key='',
         base_time=datetime(1950, 01, 01, 0, 0),
         target_cols=512,
         target_rows=512):
    base_name = os.path.splitext(os.path.basename(s3key))[0]  # Take off the extension
    with rasterio.drivers():
        with open_netCDF(input_path, subds) as dataset:
            cols = dataset.meta['width']
            rows = dataset.meta['height']

            tile_cols = int(math.ceil(cols / float(target_cols)))
            tile_rows = int(math.ceil(rows / float(target_rows)))

            # create windows
            windows = {}
            for tile_row in range(0, tile_rows):
                windows[tile_row] = {}

                # Because of the netCDF verticle flip issue,
                # set the windows going backwards for the rows.
                start = (rows - ((tile_row+1) * target_rows))
                row_window = (max(start, 0), start + target_rows)

                for tile_col in range(0, tile_cols):
                    start = tile_col * target_cols
                    col_window = (start, start + target_cols)
                    window = (row_window, col_window)
                    windows[tile_row][tile_col] = window

            # Logic for doing extent windowing.
            affine = dataset.affine

            for i in range(1, dataset.count + 1):
                tags = dataset.tags(i)
                days_since = float(tags['NETCDF_DIM_time'])
                band_date = base_time + timedelta(days_since)
                band_date_name = band_date.strftime(DATE_FORMAT)

                for tile_row in range(0, tile_rows):
                    for tile_col in range(0, tile_cols):
                        read_window = windows[tile_row][tile_col]

                        # WEIRDNESS: The netCDF is "bottom-up" data. This causes GDAL
                        # to not be able to work with it unless this evnironment variable
                        # is exported: export GDAL_NETCDF_BOTTOMUP=NO
                        # So it reads the band upside down, and we need to flip it.
                        wrong_way_up = dataset.read_band(i, window=read_window)
                        tile_data = numpy.flipud(wrong_way_up)
                        (data_rows, data_cols) = tile_data.shape
                        name = "%s-%s_%d_%d" % (base_name, band_date_name, tile_row, tile_col)
                        log("Tile (%3d, %3d)  Window: %s  " % (tile_col, tile_row, str(read_window)))

                        # Find affine
                        tile_affine = get_affine(affine,
                                                 tile_col * target_cols,
                                                 tile_row * target_rows)

                        tile_meta = dataset.meta.copy()
                        tile_meta['height'] = data_rows
                        tile_meta['width'] = data_cols
                        tile_meta['count'] = 1
                        tile_meta['driver'] = u'GTiff'
                        tile_meta['transform'] = tile_affine
                        tile_meta['COMPRESS'] = 'DEFLATE'

                        with rasterio.open(os.path.join(out_dir, name + '.tif'), 'w', **tile_meta) as dst:
                            tile_tags = [
                                ("ISO_TIME", band_date.isoformat()),
                                ("ORIGIN", s3key),
                                ("BAND_NUMBER", i),
                                ("TILE_COL", tile_col),
                                ("TILE_ROW", tile_row)]

                            dst.update_tags(**dict(dataset.tags().items() + tags.items() + tile_tags))
                            dst.write_band(1, tile_data)


def main():
    parser = argparse.ArgumentParser(description='Converts netcdf to geotiff')
    parser.add_argument('infile', metavar='INFILE', type=str,
                        help='The input NETCDF')
    parser.add_argument('subdataset', metavar='SUBDS', type=str,
                        help='Subdataset ID')
    parser.add_argument('id', metavar='ID', type=str,
                        help='s3 ID')
    args = parser.parse_args()
    tile(args.infile, './', args.subdataset, args.id)


if __name__ == '__main__':
    main()
