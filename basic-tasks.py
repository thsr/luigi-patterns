import os
import luigi
import pandas as pd

class WriteToFile(luigi.Task):
    """Writes "done" to a file named `WriteToFile-param.txt` (name of the task, dash, param)

    Parameters
    ----------
    param: str
        Some example parameter
    """
    param = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(
            os.path.join("%s-%s.txt" % (self.__class__.__name__, self.param))
        )

    def run(self):
        with open(self.output().path, 'w') as out:
            out.write("done")


class WriteDataFrameToFile(luigi.Task):
    """Writes a Pandas DataFrame to a CSV file named `WriteDataFrameToFile-param.csv` (name of the task, dash, param).

    Parameters
    ----------
    param: str
        Some example parameter
    """
    param = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(
            os.path.join("%s-%s.csv" % (self.__class__.__name__, self.param))
        )

    def run(self):
        df = pd.DataFrame([['123456', self.param1]], columns=['id','param'])
        df.to_csv(self.output().path, encoding='utf-8', index=False)


class TaskWithRequirements(luigi.Task):
    """Template for a task that has two parent tasks: WriteToFile & WriteDataFrameToFile.

    Parameters
    ----------
    param: str
        Some example parameter
    """
    param = luigi.Parameter()

    def requires(self):
        return [
            WriteToFile(param=self.param),
            WriteDataFrameToFile(param=self.param)
        ]

    def output(self):
        return luigi.LocalTarget(
            os.path.join("%s-%s.txt" % (self.__class__.__name__, self.param))
        )

    def run(self):
        with open(self.output().path, 'w') as out:
            out.write("done")