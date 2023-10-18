#!/usr/bin/env python
# coding: utf-8


import pandas as pd
import numpy as np
import argparse 
from sqlalchemy import create_engine


def extract_data(source):
    return pd.read_csv(source)

def transform_data(data):
    df = data.copy()

    # split up sex upon outcome columns into sex and reproductive status
    df['Sex upon Outcome'] = df['Sex upon Outcome'].replace('Unknown', np.nan)
    df[['reproductive_status', 'sex']] = df['Sex upon Outcome'].str.split(' ', expand=True, n=1)
    df.loc[df['Sex upon Outcome'] == 'Unknown', 'sex'] = 'Unknown'

    # Drop MonthYear, Sex upon outcome column, and Age upon Outcome
    df.drop(columns=['MonthYear', 'Sex upon Outcome', 'Age upon Outcome'], inplace=True)

    # rename columns
    df.rename(columns={'Animal ID': 'animal_id', 'Name': 'name', 'DateTime': 'created_time',
                       'DateTime': 'processed_ts', 'Date of Birth': 'dob', 'Outcome Type': 'outcome_type',
                       'Outcome Subtype': 'outcome_subtype', 'Animal Type': 'animal_type',
                       'Breed': 'breed', 'Color': 'color'}, inplace=True)

    # convert to date time
    df['dob'] = pd.to_datetime(df['dob'])
    df['processed_ts'] = pd.to_datetime(df['processed_ts'])

    # create animal dimension table
    animal_dim = df[['animal_id', 'name', 'dob', 'animal_type', 'breed', 'color', 'sex']].copy()
    animal_dim.drop_duplicates(subset='animal_id', inplace=True)

    ###############################
    # create outcometype_dim
    outcometype_dim = df[['outcome_type']].drop_duplicates().copy()
    outcometype_dim.reset_index(inplace=True, drop=True)
    outcometype_dim.reset_index(inplace=True)

    # rename index column to id
    outcometype_dim.rename(columns={'index': 'outcome_type_id'}, inplace=True)

    # create mapping with unique outcometype id
    outcometype_map = dict(zip(outcometype_dim.outcome_type, outcometype_dim.outcome_type_id))

    ################################
    # create subtype mapping
    outcomesubtype_dim = df[['outcome_subtype']].drop_duplicates().copy()
    outcomesubtype_dim.reset_index(drop=True, inplace=True)
    outcomesubtype_dim.reset_index(inplace=True)

    # rename index column mapping to id
    outcomesubtype_dim.rename(columns={'index': 'outcome_subtype_id'}, inplace=True)

    # create subtype mapping
    outcomesubtype_map = dict(zip(outcomesubtype_dim.outcome_subtype, outcomesubtype_dim.outcome_subtype_id))

    #################################

    # Create processed date time dimension table
    processed_dim = df[['processed_ts']].copy()
    processed_dim.drop_duplicates(inplace=True)
    processed_dim.reset_index(drop=True, inplace=True)
    processed_dim.reset_index(inplace=True)

    processed_dim.rename(columns={'index': 'processed_id'}, inplace=True)

    processed_dim['year'] = processed_dim['processed_ts'].dt.year
    processed_dim['month'] = processed_dim['processed_ts'].dt.month
    processed_dim['day'] = processed_dim['processed_ts'].dt.day

    processed_map = dict(zip(processed_dim.processed_ts, processed_dim.processed_id))

    #####################################
    repro_dim = df[['reproductive_status']].drop_duplicates().copy()
    repro_dim.reset_index(drop=True, inplace=True)
    repro_dim.reset_index(inplace=True)

    repro_dim.rename(columns={'index': 'reproductive_status_id'}, inplace=True)

    repro_map = dict(zip(repro_dim.reproductive_status, repro_dim.reproductive_status_id))

    df_fact = df[['animal_id', 'outcome_type', 'outcome_subtype', 'processed_ts', 'reproductive_status']].copy()

    df_fact['outcome_type'] = df_fact['outcome_type'].map(outcometype_map)
    df_fact['outcome_subtype'] = df_fact['outcome_subtype'].map(outcomesubtype_map)
    df_fact['processed_ts'] = df_fact['processed_ts'].map(processed_map)
    df_fact['reproductive_status'] = df_fact['reproductive_status'].map(repro_map)

    df_fact.rename(columns={'outcome_type': 'outcome_type_id', 'outcome_subtype': 'outcome_subtype_id',
                            'processed_ts': 'processed_id', 'reproductive_status': 'reproductive_status_id'},
                   inplace=True)


    return df_fact, animal_dim, outcometype_dim, outcomesubtype_dim, processed_dim, repro_dim

def load_data(df_fact, animal_dim, outcometype_dim, outcomesubtype_dim, processed_dim, repro_dim):
    db_url = "postgresql+psycopg2://alex:hunter2@db:5432/shelter"  # database address
    conn = create_engine(db_url)

    processed_dim.to_sql("processed_dim", conn, if_exists="append", index = False) #what to do if my table already exists
    animal_dim.to_sql("animal_dim", conn, if_exists="append", index = False)
    outcometype_dim.to_sql("outcome_type_dim", conn, if_exists="append", index = False)
    outcomesubtype_dim.to_sql("outcome_subtype_dim", conn, if_exists="append", index = False)
    repro_dim.to_sql("repro_dim", conn, if_exists="append", index = False)
    df_fact.to_sql("visit_fact", conn, if_exists="append", index=False)

if __name__ == "__main__": 
    parser = argparse.ArgumentParser()
    parser.add_argument('source', help='source csv')
    # parser.add_argument('target', help='target csv')
    args = parser.parse_args()

    print("Starting...")
    df = extract_data(args.source)
    df_fact, animal_dim, outcometype_dim, outcomesubtype_dim, processed_dim, repro_dim = transform_data(df)
    load_data(df_fact, animal_dim, outcometype_dim, outcomesubtype_dim, processed_dim, repro_dim)
    print("Complete")