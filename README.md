# nex2geotiff-ingest

Moves NETCDF data in s3 to another s3 bucket.

## Building

`docker build -t nex2geotiff .`

## Running (example)

```bash
docker run -e AWS_ACCESS_KEY_ID=YOUR_ACCESS_KEY_HERE \
  -e AWS_SECRET_ACCESS_KEY=YOUR_SECRET_ACCESS_KEY_HERE \
  nex2geotiff \
  ./download_convert_upload.py s3://nasanex/NEX-DCP30/NEX-quartile/rcp85/mon/atmos/tasmax/r1i1p1/v1.0/CONUS/tasmax_quartile75_amon_rcp85_CONUS_208101-208512.nc \
  TARGET_BUCKET_HERE
```
