# nex2json-ingest

Extracts data from NASA NEX datasets and uploads them to S3 as json.

## Building

`docker build -t quay.io/azavea/nex2json .`

## Running (example)

### Downloading a dataset, processing, and uploading the result

```bash
docker run -e AWS_ACCESS_KEY_ID=YOUR_ACCESS_KEY_HERE \
  -e AWS_SECRET_ACCESS_KEY=YOUR_SECRET_ACCESS_KEY_HERE \
  quay.io/azavea/nex2json \
  ./process_dataset.py rcp85 tasmax MIROC5 2050 \
  TARGET_BUCKET_HERE
```

### Submitting jobs to the queue

The use of the `all` keyword below in place of a list of model names causes
the script to use a list of all models for which a base time is defined.

```bash
docker run -e AWS_ACCESS_KEY_ID=YOUR_ACCESS_KEY_HERE \
  -e AWS_SECRET_ACCESS_KEY=YOUR_SECRET_ACCESS_KEY_HERE \
  quay.io/azavea/nex2json \
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
  quay.io/azavea/nex2json ./worker.py
```

## Running jobs in the queue in AWS

### Create queue if one does not exist

Visibility timeout: 30 minutes

### Submit jobs to the queue using above example

### Push Docker image

`docker push quay.io/azavea/nex2json`

### Create ECS cluster

### Create task definition

- Add container
  - Container name: nex2json
  - Image: quay.io/azavea/nex2json
  - Maximum memory: 2048MB
  - CPU units: 2
  - Command: `./worker.py`
  - Environment variables
    - `IS_WORKER`: `yes`
    - `WORKER_QUEUE` - name of queue
    - `AWS_ACCESS_KEY_ID` - access key id with access to queue and s3
    - `AWS_SECRET_ACCESS_KEY` - secret access key

### Create IAM role for ECS instances, if it doesn't already exist

Attach `AmazonEC2ContainerServiceforEC2Role` policy

### Start a number of EC2 spot instances

Use AMI id from http://docs.aws.amazon.com/AmazonECS/latest/developerguide/launch_container_instance.html

Use IAM role created earlier

Under "Advanced Details", enter the following:

```bash
#!/bin/bash
echo ECS_CLUSTER=your_cluster_name >> /etc/ecs/ecs.config
```

### Run task

Under the ECS cluster intance, when instances are registered, click "Run Task" under "Tasks"
and start the created task.

### Monitor queue

Monitor the queue to see if tasks are being completed
