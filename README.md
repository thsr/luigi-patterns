Luigi examples
==============

Snippets or task examples for Luigi pipelines

https://github.com/spotify/luigi/

Google Cloud
------------

Usage:

```python
luigi.build([BigqueryRunQuery(dataset='mydataset', table='mytable')])
```

```python
luigi.build([FileToGCS(source='./path/to/my/file.csv', destination='gs://bucket/my/file.csv')])
```

```python
luigi.build([GCSToBigquery(source='gs://bucket/my/file.csv', dataset='mydataset', table='mytable')])
```

```python
luigi.build([FileToBigquery(source='./path/to/my/file.csv', dataset='mydataset', table='mytable')])
```