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


def parse_args():

    parser = argparse.ArgumentParser()
    parser.add_argument("--path", help="path csv,tsv,xls,or xlsx file")
    parser.add_argument('-t',
                        '--transform',
                        dest='transforms',
                        action='append',
                        help='<Required> Set flag',
                        required=True)

    parser.add_argument("--clubhouse-path",
                        help="path to Clubhouse converted CSV")

    parser.add_argument("--out", help="path of output CSV")
    args = parser.parse_args()

    return args


def date_string_to_ymd(str):
    # print(str)

    dt = datetime.strptime(str, "%a %b %d %Y %H:%M:%S %Z%z (UTC)")
    local_dt = dt.replace(tzinfo=timezone.utc).astimezone(tz=None)

    # dt.tzinfo =
    # print(dt.tzinfo)
    # print(dt.strftime("%Y-%m-%d %H:%M:%S"))
    # print(local_dt.strftime("%Y-%m-%d %H:%M:%S"))

    local_date_str = local_dt.strftime("%Y-%m-%d %H:%M:%S")
    logging.info(local_date_str)
    return local_date_str


def camel_case(word):
    parts = str.split(word.title())
    parts[0] = parts[0].lower()
    rval = ''.join(parts)
    return rval


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


def read_clubhouse_mongo(path):
    clubhouse = pd.read_csv(path, index_col='email')

    clubhouse = clubhouse[[
        'firstName', 'lastName', 'homePhone', 'workPhone', 'otherPhone',
        'mobilePhone'
    ]]

    return clubhouse.rename(camel_case)


def read_clubhouse_download(path):
    clubhouse = pd.read_csv(path)
    clubhouse = clubhouse.rename(columns=camel_case)

    for key in ['created', 'modified', 'checkinDate']:
        clubhouse[key] = clubhouse[key].apply(date_string_to_ymd)

    for phone in ['homePhone', 'workPhone', 'mobilePhone', 'otherPhone']:
        clubhouse[phone] = clubhouse[phone].apply(tele)

    clubhouse['email'] = clubhouse['email'].apply(lambda e: e.lower())

    clubhouse.set_index('email', inplace=True)

    return clubhouse


def read_mp_repaired_xlsx(path):
    members = pd.read_excel(path)
    members = members.rename(columns=camel_case)


def read_member_planet_converted_csv(path):
    members = pd.read_csv(path)
    members = members.rename(columns=camel_case)

    members.dropna(subset=['email'], inplace=True)

    # lowercase email addresses. easier to merge
    members['email'] = members['email'].apply(lambda e: e.lower())
    print(members)

    members.set_index('email', inplace=True)
    for col in ['initiatedDate', 'paidThroughDate']:
        members[col] = members[col].apply(
            lambda s: datetime.strptime(s, "%m/%d/%y"))

    # members = members[members['Home Phone'] != '[Restricted]']

    return members


def a_not_in_b(df_a, df_b):
    return df_a.drop(df_b.index, errors='ignore')


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

    member_planet = read_member_planet_converted_csv(args.mp_path)
    clubhouse = read_clubhouse_download(args.clubhouse_path)

    new_invitees = a_not_in_b(clubhouse, member_planet)

    new_invitees.to_csv(args.out)

    exit()
    # print(member_planet[
    #     member_planet['firstName'].str.strip() != member_planet['firstName']])
    # exit()
    # member_planet = member_planet[member_planet['firstName'] == "Poul"]
    # print(member_planet)
    # exit()
    # member_planet = member_planet[['firstName', 'lastName', 'paidThroughDate']]
    # member_planet.sort_values(by=['lastName'], inplace=True)

    # member_planet.to_excel("cathy_paid_through_list_2019-11-10.xlsx")

    # clubhouse = read_clubhouse_mongo('updated_clubhouse.csv')
    # print(a_not_in_b(member_planet, clubhouse))
    # print(a_not_in_b(clubhouse, member_planet))


def main2():
    args = parse_args()
    mp = read_member_planet_converted_csv('member_planet.2019-12-02.csv')
    cols = [
        'firstName', 'lastName', 'initiatedDate', 'status', 'memberType',
        'level', 'paidThroughDate', 'memberRating', 'homePhone', 'mobilePhone',
        'workPhone', 'otherPhone'
    ]
    print(mp[cols])

    mp['level'].fillna('Unpaid', inplace=True)
    mp['joinedMonth'] = mp['initiatedDate'].apply(lambda d: d.strftime("%Y%m"))
    mp['paidMonth'] = mp['paidThroughDate'].apply(lambda d: d.strftime("%Y%m"))

    #print(mp.groupby(['level', 'paidMonth'], as_index=False).count())

    paidThroughStats = pd.pivot_table(mp,
                                      values='mpId',
                                      index=['level'],
                                      columns=['paidMonth'],
                                      aggfunc='count').fillna('--')

    print(mp[(mp['paidMonth'] != '202001')
             & (mp['paidMonth'] != '200211')
             & (mp['level'] != 'Lifetime')][cols])

    print(paidThroughStats)


def main4():
    args = parse_args()

    r = CsvScrubber.Reader(args.path)
    df = r.read()

    for transform in args.transforms:
        # this will put first value into transform and rest in a list (params)
        transform, *params = transform.split(':')

        transform = Transforms.create(df, transform, params)

        df = transform.transform()


main4()