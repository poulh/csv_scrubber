import os
#import pandas as pd


def create(df, transform, params):
    t = None
    if transform == 'print':
        t = Print(df, *params)

    if transform == 'filter':
        t = Filter(df, *params)

    if transform == 'is-na':
        t = IsNa(df, *params)

    if transform == 'not-na':
        t = NotNa(df, *params)

    if transform == 'camelcase':
        t = CamelCase(df, *params)

    if transform == 'lower':
        t = Lower(df, *params)

    if transform == 'strip':
        t = Strip(df, *params)

    if t is None:
        raise ValueError("unsupported transform '{}'".format(transform))

    return t


class Transform:
    def __init__(self, df, *params):
        self.df = df
        self.params = list(params)

    def transform(self):
        return self.df


class ColumnTransform(Transform):
    def __init__(self, df, *params):
        super().__init__(df, *params)
        self.column = self.params[0]


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


class NotNa(ColumnTransform):
    def transform(self):

        return self.df[self.df[self.column].notna()]


class IsNa(ColumnTransform):
    def transform(self):

        return self.df[self.df[self.column].isna()]


class Filter(Transform):
    def transform(self):

        transform_name = self.params[0]

        t = create(self.df.copy(), transform_name, self.params[1:])
        transformed_df = t.transform()

        column = t.column  # this ensures 't' is a ColumnTransform
        series1 = self.df[column]

        series2 = transformed_df[column]
        return self.df[series1 != series2]


class Lower(ColumnTransform):
    def transform(self):

        self.df[self.column] = self.df[self.column].map(
            lambda x: x.lower() if isinstance(x, str) else x)

        return self.df


class Strip(ColumnTransform):
    def transform(self):

        self.df[self.column] = self.df[self.column].map(
            lambda x: x.strip() if isinstance(x, str) else x)

        return self.df


class Print(Transform):
    def transform(self):
        print(self.df)
        return super().transform()
