import os
import pandas as pd


class Reader:
    def __init__(self, path):
        self.path = path

    def read(self):
        _, file_extension = os.path.splitext(self.path)

        if file_extension == '.csv':
            return pd.read_csv(self.path)
        elif file_extension in ['.xls', '.xlsx']:
            return pd.read_excel(self.path)
        elif file_extension == '.tsv':
            return pd.read_csv(self.path, delimiter='\t')
        else:
            raise ValueError("unsupported extension {}".format(file_extension))
