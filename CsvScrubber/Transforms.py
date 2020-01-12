import os
#import pandas as pd


class Transform:
    def __init__(self, df, *params):
        self.df = df
        self.params = list(params)

    def transform(self):
        return self.df


class CamelCase(Transform):
    def transform(self):
        def camel_case(word):
            parts = str.split(word.title())
            parts[0] = parts[0].lower()
            rval = ''.join(parts)
            return rval

        return self.df.rename(columns=camel_case)

class NotNa(Transform):
    def transform(self):
        colname = self.params[0]
        return self.df[self.df[colname].notna()]

class IsNa(Transform):
    def transform(self):
        colname = self.params[0]
        return self.df[self.df[colname].isna()]

class NotLower(Transform):
    def transform(self):
        colname = self.params[0]
        
        return self.df[self.df[colname] != self.df[colname].str.lower()]

class Lower(Transform):
    def transform(self):
        colname = self.params[0]
        self.df[colname] = self.df[colname].map(lambda x: x.lower() if isinstance(x,str) else x)

        return self.df

class Print(Transform):
    def transform(self):
        print(self.df)
        return super().transform()
