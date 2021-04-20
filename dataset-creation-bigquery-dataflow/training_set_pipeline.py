import apache_beam as beam
from apache_beam.io.gcp.bigquery import ReadFromBigQuery
from apache_beam.io.gcp.gcsio import GcsIO
from apache_beam.options.pipeline_options import PipelineOptions
from datetime import datetime


PROJECT_ID = '[PROJECT_ID]'
BQ_SOURCE = f'{PROJECT_ID}.[DATASET_NAME].amazon_us_reviews__ratings'
GCS_DESTINATION = 'gs://[BUCKET_NAME]/datasets/amazon-us-reviews/automl'
DATAFLOW_REGION = 'europe-west1'
RUNNER = 'DataflowRunner'  # use 'DirectRunner' for local execution


class CreateLine(beam.DoFn):
    def __init__(self, gcs_path):
        self.gcs_path = gcs_path

    def process(self, bq_row):
        yield f",{self.gcs_path}/data/{bq_row['review_id']}.txt,{bq_row['class']}"


class WriteExampleFile(beam.DoFn):
    def __init__(self, gcs_path):
        self.gcsio = GcsIO
        self.gcs_path = gcs_path

    def process(self, bq_row):
        with self.gcsio().open(f"{self.gcs_path}/data/{bq_row['review_id']}.txt", 'w', mime_type='text/plain') as fp:
            fp.write(bq_row['review_body'].encode())


class WriteIndexFile(beam.DoFn):
    def __init__(self, gcs_path):
        self.gcsio = GcsIO
        self.gcs_path = gcs_path

    def process(self, lines):
        with self.gcsio().open(f'{self.gcs_path}/index.csv', 'w', mime_type='text/csv') as fp:
            fp.write(lines.encode())


job_name = f"reviewr-automl--{datetime.utcnow().strftime('%Y%m%d-%H%I%S')}"
gcs_path = f'{GCS_DESTINATION}/{job_name}'
pipeline_options = PipelineOptions(project=PROJECT_ID,
                                   region=DATAFLOW_REGION,
                                   job_name=job_name,
                                   temp_location=f'{gcs_path}/temp')

p = beam.Pipeline(runner=RUNNER, options=pipeline_options)
bq_row = p | 'ReadFromBigQuery' >> ReadFromBigQuery(query=f"SELECT * FROM `{BQ_SOURCE}`{' LIMIT 10' if RUNNER == 'DirectRunner' else ''}",
                                                    project=PROJECT_ID,
                                                    use_standard_sql=True,
                                                    gcs_location=f'{gcs_path}/temp')

bq_row | 'WriteExampleFile' >> beam.ParDo(WriteExampleFile(gcs_path))

bq_row | 'CreateLine' >> beam.ParDo(CreateLine(gcs_path))\
    | 'CombineLines' >> beam.CombineGlobally(lambda lines: '\n'.join(lines))\
    | 'WriteIndexFile' >> beam.ParDo(WriteIndexFile(gcs_path))

p.run()
