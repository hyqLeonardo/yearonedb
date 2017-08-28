import MySQLdb
import sqlalchemy
from sqlalchemy import create_engine, MetaData, Table, select

import numpy as np
import pandas as pd
import datetime

engine = create_engine('mysql+mysqldb://yearone:yearone@localhost:5432/factor_pool', echo=False)
conn = engine.connect()
metadata = MetaData(conn)


def save_factor(factor_df, factor_name, save_mode='APPEND'):
    """
    Persist factor DataFrame to database factor_pool
    :param factor_df:       DataFrame of factor to be stored
    :param factor_name:     name of the factor table
    :param save_mode:       APPEND or REPLACE
    :return: void
    """

    if save_mode not in ['APPEND', 'REPLACE']:
        print('save_mode is either \"APPEND\" or \"REPLACE\"')
        return

    if save_mode == 'APPEND':
        # check if column has changes
        try:
            t = Table(factor_name, metadata, autoload=True)
        except sqlalchemy.exc.NoSuchTableError as ex:
            print(ex)
            return

        col_db = [m.key for m in t.columns]
        col_df = factor_df.columns.values
        col_intercept = list(set(col_db) & set(col_df))

        if len(col_db) == len(col_df):
            if len(col_intercept) == len(col_db):   # no column changes
                # get rid of repeated rows by making index unique
                index_db = pd.read_sql_table(factor_name, columns=['index'], con=engine)
                index_db = list(index_db['index'])
                index_df = list(factor_df.index)
                index_intercept = list(set(index_db) & set(index_df))
                index_unique = list(set(index_df) - set(index_intercept))
                index_unique.sort()
                factor_df = factor_df.loc[index_unique, :]
                # append with index=False, cause the column named as 'index'
                # (which is auto assigned by index=True) conflict with SQL key word
                factor_df['index_col'] = factor_df.index
                factor_df.to_sql(name=factor_name, con=engine, if_exists='append', index=False)

        assert len(col_intercept) < len(col_db)
        # column changed, replace the DataFrame in Database
        db_df = pd.read_sql('SELECT * from {}'.format(factor_name),
                            con=engine, index_col=['index_col'])    # read all
        db_df.index.name = 'index'  # from 'index_col' to 'index'
        # make column of DataFrame in database consistent with factor_df
        df = db_df[col_intercept]
        new_col_name = list(set(col_df) - set(col_intercept))
        for c in new_col_name:
            df[c] = np.nan
        save_factor(df, factor_name, 'REPLACE')
        # append as usual
        save_factor(factor_df, factor_name, 'APPEND')

    if save_mode == 'REPLACE':
        factor_df['index_col'] = factor_df.index
        factor_df.to_sql(name=factor_name, con=engine, if_exists='replace', index=False)


def get_factor(factor_name, order_book_ids, start_date, end_date=None):
    """
    Retrieve factor DataFrame from database factor_pool.
    :param factor_name:     name of factor
    :param order_book_ids:  stock code list
    :param start_date:      string of start date, e.g. 2017-09-01
    :param end_date:        string of end date
    :return:    factor DataFrame
    """
    if end_date is None:    # set today as end date
        end_date = str(datetime.date.today())
    cols = 'index_col, ' + ', '.join(order_book_ids)

    sql_str = 'SELECT {} FROM {} WHERE {} BETWEEN DATE(\'{}\') AND DATE(\'{}\')'.format(
        cols, factor_name, 'index_col', start_date, end_date)
    df = pd.read_sql(sql=sql_str, con=engine, index_col='index_col')
    df.index.name = 'index'

    return df
