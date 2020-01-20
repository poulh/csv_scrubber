#!/usr/bin/env python

import argparse
import csv
from datetime import datetime
from datetime import timezone
import pandas as pd
import numpy as np
import logging
import CsvScrubber
from CsvScrubber import Transforms
import json


def parse_args():

    parser = argparse.ArgumentParser()
    parser.add_argument("--path", help="path csv,tsv,xls,or xlsx file")
    parser.add_argument('-v', '--verbose', action='store_true')
    parser.add_argument('-c', '--config')
    parser.add_argument('-p', '--print', action='store_true')
    parser.add_argument('-s', '--save')
    args = parser.parse_args()

    return args


def tele(t):
    try:
        if t is not None:
            if not pd.isnull(t):
                t = int(t)
                t = str(t)
                # t = "+1" + t
                # t = phonenumbers.parse(t)
                # t = phonenumbers.format_number(
                #     t, phonenumbers.PhoneNumberFormat.NATIONAL)
                # print(t)
    except:
        logging.warning("could not convert '{}'".format(t))
    return t


def duplicate_emails(df):
    dup_emails = df.groupby(df.index.str.lower()).count()
    return dup_emails[dup_emails['firstName'] > 1]


def duplicate_names(df):
    def combine_name(df):
        new_df = df.copy()
        new_df['firstName'].fillna('?', inplace=True)
        new_df['lastName'].fillna('?', inplace=True)
        new_df['combinedName'] = new_df['firstName'].str.lower(
        ) + "-" + new_df['lastName'].str.lower()
        new_df['email'] = new_df.index

        return new_df.set_index('combinedName')

    combined = {}
    for k in ['first', 'last']:
        cb = combine_name(df)
        combined[k] = cb[cb.index.duplicated(keep=k)]

    return combined['first'].join(combined['last'], rsuffix='-2', how='inner')


def spaces_around_value(df, col_name):
    return df[df[df[col_name].str.strip() != df[col_name]]]


def main():
    args = parse_args()
    if args.verbose == True:
        print(args)
    r = CsvScrubber.Reader(args.path)
    df = r.read()

    if args.config:
        with open(args.config) as json_file:
            data = json.load(json_file)
            for transform in data['transforms']:
                transform_name = transform.pop('transform')
                transform = Transforms.create(df, transform_name, **transform)
                df = transform.transform()

    if args.print:
        transform = Transforms.create(df, 'print')
        df = transform.transform()

    if args.save:
        params = {'path': args.save}
        transform = Transforms.create(df, 'save', **params)
        df = transform.transform()


main()