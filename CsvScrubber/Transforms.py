import os
#import pandas as pd


def create(df, transform, params):
    t = None
    if transform == 'print':
        t = Print(df, *params)

    if transform == 'is-na':
        t = IsNa(df, *params)

    if transform == 'not-na':
        t = NotNa(df, *params)

    if transform == 'camelcase':
        t = CamelCase(df, *params)

    if transform == 'not-lower':
        t = NotLower(df, *params)

    if transform == 'lower':
        t = Lower(df, *params)

    if transform == 'strip':
        t = Strip(df, *params)

    if t is None:
        raise ValueError("unsupported transform '{}'".format(transform))

    return t.transform()


class Transform:
    def __init__(self, df, *params):
        self.df = df
        self.params = list(params)

    def transform(self):
        return self.df


class CamelCase(Transform):
    def transform(self):
        def camel_case(word):

            try:
                word.index(' ')  # check for spaces
                parts = str.split(word)
                parts[0] = parts[0][0].lower() + parts[0][1:]
                parts[0] = parts[0].lower()
                return ''.join(parts)
            except ValueError:
                return word[0].lower() + word[1:]

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
        self.df[colname] = self.df[colname].map(lambda x: x.lower()
                                                if isinstance(x, str) else x)

        return self.df


class Strip(Transform):
    def transform(self):
        colname = self.params[0]

        self.df[colname] = self.df[colname].map(lambda x: x.strip()
                                                if isinstance(x, str) else x)

        return self.df


class Print(Transform):
    def transform(self):
        print(self.df)
        return super().transform()
