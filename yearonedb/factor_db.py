import MySQLdb
import sqlalchemy
from sqlalchemy import create_engine, MetaData, Table, select

import numpy as np
import pandas as pd
import datetime

engine = create_engine('mysql+mysqldb://yearone:yearone@localhost:5432/factor_pool', echo=False)


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
    # get current metadata
    conn = engine.connect()
    metadata = MetaData(conn)

    assert factor_df.index[0] <= factor_df.index[-1]
    # make a copy to modify, or the calling of method upon original df will go mad
    factor_df_copy = pd.DataFrame.copy(factor_df)

    if save_mode == 'APPEND':
        print('appending to {} ...'.format(factor_name))
        # check if column has changes
        try:
            t = Table(factor_name, metadata, autoload=True)
        except sqlalchemy.exc.NoSuchTableError as ex:
            print(ex)
            return

        col_db = [m.key for m in t.columns]
        col_db.remove('index_col')
        col_df = list(factor_df_copy.columns.values)
        col_intercept = list(set(col_db) & set(col_df))

        if len(col_intercept) == len(col_df):  # no col changed, just append
            # get rid of repeated rows by making index unique
            index_db = pd.read_sql_table(factor_name, columns=['index_col'], con=engine)
            index_db = [stamp.date() for stamp in index_db['index_col']]
            index_df = list(factor_df_copy.index)
            index_intercept = list(set(index_db) & set(index_df))
            index_unique = list(set(index_df) - set(index_intercept))
            index_unique.sort()
            factor_df_copy = factor_df_copy.loc[index_unique, :]

            # append with index=False, cause the column named as 'index'
            # (which is auto assigned by index=True) conflict with SQL key word
            factor_df_copy.loc[:, 'index_col'] = factor_df_copy.index
            # append the original factor_df
            factor_df_copy.to_sql(name=factor_name, con=engine, if_exists='append', index=False)
            return

        else:  # column changed, some removed, some append

            # replace the DataFrame in Database with new columns
            db_df = pd.read_sql('SELECT * from {}'.format(factor_name),
                                con=engine, index_col=['index_col'])  # read all
            db_df.index.name = 'index'  # from 'index_col' to 'index'
            # make column of DataFrame in database consistent with factor_df
            df = db_df.loc[:, col_intercept]
            new_col_names = list(set(col_df) - set(col_intercept))
            for c in new_col_names:
                df.loc[:, c] = np.nan
            save_factor(df, factor_name, 'REPLACE')
            # append as usual
            save_factor(factor_df, factor_name, 'APPEND')  # append the original factor_df
            return

    if save_mode == 'REPLACE':
        print('replacing {} ...'.format(factor_name))
        # factor_df_copy.columns = ['S' + c.replace('.', '') for c in factor_df_copy.columns]
        factor_df_copy['index_col'] = factor_df_copy.index
        factor_df_copy.to_sql(name=factor_name, con=engine, if_exists='replace', index=False)


def get_factor(factor_name, order_book_ids, start_date, end_date=None):
    """
    Retrieve factor DataFrame from database factor_pool.
    :param factor_name:     name of factor
    :param order_book_ids:  stock code list
    :param start_date:      string of start date, e.g. 2017-09-01
    :param end_date:        string of end date
    :return:    factor DataFrame
    """
    # get current metadata
    conn = engine.connect()
    metadata = MetaData(conn)
    # get DataFrame's column in database
    try:
        t = Table(factor_name, metadata, autoload=True)
    except sqlalchemy.exc.NoSuchTableError as ex:
        print(ex)
        return
    col_db = [m.key for m in t.columns]
    col_intercept = list(set(col_db) & set(order_book_ids))
    # use back ticks to assure sql will interpret this as column name
    col_intercept = ['`' + c + '`' for c in col_intercept]

    if end_date is None:  # set today as end date
        end_date = str(datetime.date.today())
    cols = '`index_col`, ' + ', '.join(col_intercept)

    sql_str = 'SELECT {} FROM {} WHERE {} BETWEEN DATE(\'{}\') AND DATE(\'{}\')'.format(
        cols, factor_name, 'index_col', start_date, end_date)
    df = pd.read_sql(sql=sql_str, con=engine, index_col='index_col')
    df.index.name = 'index'

    return df
