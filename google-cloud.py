import os
import luigi
from luigi.contrib import bigquery, gcs


PROJECT_ID = "myprojectid"


class BigqueryRunQuery(bigquery.BigQueryRunQueryTask):
    """Runs a query on Bigquery and saves it to a table.

    Parameters
    ----------
    client: `luigi.contrib.bigquery.BigQueryClient()` instance, optional
        (default is a new instance)
    project: str, optional
        E.g. "my-project-id" (default is PROJECT_ID)
    dataset: str
        Dataset to write to e.g. "my_dataset_id"
    table: str
        Table to write to e.g. "my_table_id"
    query: str
        Query to run e.g. "SELECT 'foo' AS bar"

    References
    ----------
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
    """Uploads a file from local to Google Cloud Storage.

    Parameters
    ----------
    client: `luigi.contrib.gcs.GCSClient()` instance, optional
        (default is a new instance)
    source: str
        E.g. "./path/to/my/file.csv"
    destination: str
        E.g. "gs://bucket/my/file.csv"

    References
    ----------
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
    """Uploads a file from Google Cloud Storage to Bigquery.

    Parameters
    ----------
    project: str, optional
        E.g. "my-project-id" (default is PROJECT_ID)
    source: str
        E.g. "gs://bucket/my/file.csv"
    dataset: str
        Dataset to write to e.g. "my_dataset_id"
    table: str
        Table to write to e.g. "my_table_id"

    References
    ----------
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
    """Uploads a local file to GCS then GCS to Bigquery.

    Parameters
    ----------
    source: str
        E.g. "./path/to/my/file.csv"
    dataset: str
        Dataset to write to e.g. "my_dataset_id"
    table: str
        Table to write to e.g. "my_table_id"
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