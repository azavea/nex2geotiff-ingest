# nex2json-ingest

Extracts data from NASA NEX datasets and uploads them to S3 as json.

## Building

`docker build -t nex2json .`

## Running (example)

### Downloading a dataset, processing, and uploading the result

```bash
docker run -e AWS_ACCESS_KEY_ID=YOUR_ACCESS_KEY_HERE \
  -e AWS_SECRET_ACCESS_KEY=YOUR_SECRET_ACCESS_KEY_HERE \
  nex2json \
  ./process_dataset.py rcp85 tasmax MIROC5 2050 \
  TARGET_BUCKET_HERE
```

### Submitting jobs to the queue

The use of the `all` keyword below in place of a list of model names causes
the script to use a list of all models for which a base time is defined.

```bash
docker run -e AWS_ACCESS_KEY_ID=YOUR_ACCESS_KEY_HERE \
  -e AWS_SECRET_ACCESS_KEY=YOUR_SECRET_ACCESS_KEY_HERE \
  nex2json \
  ./worker.py SQS_QUEUE_NAME \
  rcp45,rcp85 all 2010,2050,2099 tasmin,tasmax,pr \
  TARGET_BUCKET_HERE
```

### Running jobs from the queue

```bash
docker run -e AWS_ACCESS_KEY_ID=YOUR_ACCESS_KEY_HERE \
  -e AWS_SECRET_ACCESS_KEY=YOUR_SECRET_ACCESS_KEY_HERE \
  -e WORKER_QUEUE=SQS_QUEUE_NAME \
  -e IS_WORKER=yes \
  nex2json ./worker.py
```
