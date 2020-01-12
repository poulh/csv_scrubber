import os
import pandas as pd
import csv


class Writer:
    def __init__(self, df, path):
        self.df = df
        self.path = os.path.expanduser(path)

    def write(self):
        _, file_extension = os.path.splitext(self.path)

        if file_extension == '.csv':
            return self.df.to_csv(self.path,
                                  quoting=csv.QUOTE_NONE,
                                  index=False)
        elif file_extension == '.tsv':
            return self.df.to_csv(self.path,
                                  sep='\t',
                                  quoting=csv.QUOTE_NONE,
                                  index=False)
        elif file_extension in ['.xls', '.xlsx']:
            return self.df.to_excel(self.path, index=False)
        else:
            raise ValueError("unsupported extension {}".format(file_extension))
