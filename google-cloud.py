import os
import luigi
from luigi.contrib import bigquery, gcs


PROJECT_ID = "myprojectid"


class BigqueryRunQuery(bigquery.BigQueryRunQueryTask):
    """Runs a query on Bigquery and saves it to a table

    client (opt): `gcs.GCSClient()` instance
    project (defaults to PROJECT_ID defined above): "my-project-id"
    dataset: "my_dataset_id" to write to
    table: "my_table_id" to write to
    query: "SELECT 'foo' AS bar"

    https://luigi.readthedocs.io/en/stable/api/luigi.contrib.bigquery.html
    """
    client = luigi.Parameter(default=bigquery.BigQueryClient())
    project = luigi.Parameter(default=PROJECT_ID)
    dataset = luigi.Parameter()
    table = luigi.Parameter()
    query = luigi.Parameter(default="""SELECT 'foo' AS bar""")

    def output(self):
        return bigquery.BigQueryTarget(self.project, self.dataset, self.table, client=self.client)


class FileToGCS(luigi.Task):
    """Uploads a file from local to Google Cloud Storage

    client (opt): `gcs.GCSClient()` instance
    source: "./path/to/my/file.csv"
    destination: "gs://bucket/my/file.csv"

    https://luigi.readthedocs.io/en/stable/api/luigi.contrib.gcs.html
    """
    client = luigi.Parameter(default=gcs.GCSClient())
    source = luigi.Parameter()
    destination = luigi.Parameter() # "gs://bi_poc/my-test.txt"

    def output(self):
        return gcs.GCSTarget(self.destination, client=self.client)

    def run(self):
        with open(self.source, 'r') as infile:
            with gcs.GCSTarget(self.destination, client=self.client).open(mode='w') as outfile:
                outfile.write(infile.read())


class GCSToBigquery(bigquery.BigQueryLoadTask):
    """Uploads a file from Google Cloud Storage to Bigquery
    
    project (defaults to PROJECT_ID defined above): "my-project-id"
    source: "gs://bucket/my/file.csv"
    dataset: "my_dataset_id" to write to
    table: "my_table_id" to write to

    https://luigi.readthedocs.io/en/stable/api/luigi.contrib.bigquery.html
    """
    project = luigi.Parameter(default=PROJECT_ID)
    source = luigi.Parameter()
    dataset = luigi.Parameter()
    table = luigi.Parameter()

    @property
    def source_format(self):
        return "CSV"

    @property
    def write_disposition(self):
        return 'WRITE_TRUNCATE'

    @property
    def schema(self):
        return [
            {'name': 'column1', 'type': 'STRING', 'mode': 'NULLABLE', },
            {'name': 'column2', 'type': 'STRING', 'mode': 'NULLABLE', },
        ]

    @property
    def skip_leading_rows(self):
        return 1

    def source_uris(self):
        return [self.source]

    def output(self):
        return bigquery.BigQueryTarget(self.project, self.dataset, self.table)


class FileToBigquery(luigi.WrapperTask):
    """Uploads a local file to GCS then GCS to Bigquery

    source: "./path/to/my/file.csv"
    dataset: "my_dataset_id" to write to
    table: "my_table_id" to write to
    """
    source = luigi.Parameter()
    dataset = luigi.Parameter()
    table = luigi.Parameter()

    @property
    def gcs_file(self):
        return f"gs://bi_poc/{self.dataset}/{self.table}.csv"

    def requires(self):
        yield FileToGCS(source=self.source, destination=self.gcs_file)
        yield GCSToBigquery(source=self.gcs_file, dataset=self.dataset, table=self.table)