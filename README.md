Luigi patterns
==============

Templates, snippets and task examples for Luigi pipelines

https://github.com/spotify/luigi/

Google Cloud
------------

### Usage

Runs a query on Bigquery and saves it to a table:
```python
luigi.build([BigqueryRunQuery(dataset='mydataset', table='mytable', query="""SELECT 'foo' AS bar""")])
```

Uploads a file from local to Google Cloud Storage:
```python
luigi.build([FileToGCS(source='./path/to/my/file.csv', destination='gs://bucket/my/file.csv')])
```

Uploads a file from Google Cloud Storage to Bigquery:
```python
luigi.build([GCSToBigquery(source='gs://bucket/my/file.csv', dataset='mydataset', table='mytable')])
```

Uploads a local file to GCS then GCS to Bigquery:
```python
luigi.build([FileToBigquery(source='./path/to/my/file.csv', dataset='mydataset', table='mytable')])
```