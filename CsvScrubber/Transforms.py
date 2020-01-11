import os
#import pandas as pd


class Transform:
    def __init__(self, df):
        self.df = df

    def transform(self):
        return self.df


class CamelCaseTransform(Transform):
    def transform(self):
        def camel_case(word):
            print(word)
            parts = str.split(word.title())
            parts[0] = parts[0].lower()
            rval = ''.join(parts)
            return rval

        return self.df.rename(columns=camel_case)
