import MySQLdb
from sqlalchemy import create_engine

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

    if save_mode == 'APPEND':
        factor_df.to_sql(name=factor_name, con=engine, if_exists='append', index=True)

    if save_mode == 'REPLACE':
        factor_df.to_sql(name=factor_name, con=engine, if_exists='replace', index=True)


def get_factor(factor_name):
    """
    Retrieve factor DataFrame from database factor_pool.
    :param factor_name: name of factor
    :return: factor DataFrame
    """
    # df without index, but has a column called index
    df = pd.read_sql_query('SELECT * from {}'.format(factor_name), con=engine)
    df.index = df['index']
    del df['index']
    return df

