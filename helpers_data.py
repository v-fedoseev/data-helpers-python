import pandas as pd


# variables

nondate_types = {
    # list 'column name': type
    # to cast columns to types in bulk
    'cusip': str,
    'cik': 'Int64',
    'shares': int  
}
date_types = {
    # indicate types for date columns, 'column name': 'custom type name', e.g.
    # q - quarter
    # dt - datetime
    # ...
    'quarter': 'q',
    'date': 'dt'
}


# lambda functions

nunique = lambda x: x.nunique()  # num of unique elements
nunique_no_0 = lambda x: x.nunique() - int(any([n == 0 for n in x]))  # nun of unique elements that are not 0s
set_no_0 = lambda x: set([n != 0 for n in x])  # set of all nonzero elements
join_set = lambda x: '*'.join(sorted([str(item) for item in set(x)]))  # list all unique elements in sorted order in a string
zeros_percent = lambda x: np.round(sum([num == 0 for num in x]) / len(x), 3)  # percent of zeros in a list

date_parser = lambda x: datetime.strptime(x, '%Y%m%d')


# functions

def chunk(seq, size):
    """Splits a dataframe into chunks with set length

    Args:
        seq: the dataframe to split
        size (int): the length of chunks
    
    Returns:
        the list of chunks    
    """
    return [seq[pos:pos + size] for pos in range(0, len(seq.index), size)]


def has_duplicates(df, cols=None):
    """Checks if a dataframe has duplicates
    https://stackoverflow.com/questions/57927883
    
    Args:
        df: the dataframe to check
        cols (list): the list of columns to check, i.e. the key.
            Checks all columns by default. Optional
    
    Returns:
        True if the dataframe has duplicates, False otherwise
    """
    s = set()
    if cols is None:
        rows = df
    else:
        rows = df[cols]
    
    for ix, row in enumerate(rows):
        s.add(row)
        if len(s) < (ix + 1):
            return True  # duplicate found!
    
    return False


def left_merge_safe(df_l, df_r, cols, merge_name=''):
    """Performs a left merge only if it would not create duplciates
    
    Args:
        df_l: hte left dataframe
        df_r: the right dataframe
        cols (list): the list of columns on which to merge
        merge_name (str): name of the merge to dislay in the error. Optional
    
    Returns:
        True if the dataframe has duplicates, False otherwise
    """
    assert not has_duplicates(df_r, cols), f'{merge_name}: the right dataframe has duplicates by the key of {cols}'
    
    len_pre_merge = len(df_l.index)
    df_l = df_l.merge(
        df_r,
        on=cols,
        how='left'
    )
    assert len(df_l.index) == len_pre_merge, f'{merge_name}: duplicates from the merge'
    
    return df_l


def get_quarter_list(start='2000Q1', end='2020Q4', col_name='rdate'):
    """Gets a sorted single column dataframe of quarters between the start and the end ones
    
    Args:
        start (str): start quarter in 2000Q1 format
        end (str): end quarter in 2000Q1 format
        col_name (str): the name of the single column
    Returns:
        Single column dataframe with quarters in Pandas.Period format
    
    """
    # create a df with start and end quarters in datetime format
    dates = pd.DataFrame(
        data=[
            pd.to_datetime(start),
            pd.to_datetime(end)
        ],
        columns=[col_name]
    )
    # convert to Period format
    dates[col_name] = dates[col_name].dt.to_period('Q')
    
    # interpolate the quarters in between
    dates = (
        dates.set_index(col_name)
        .resample('Q')
        .pad()
        .reset_index()
    )
    
    return dates


def set_date_types(df, date_types=date_types, errors='coerce'):
    """Casts the specified date columns of a dataframe to specified types.
    Skips a specified column if it is not present in the dataframe.
    
    Args:
        df: dataframe to convert columns in
        date_types (dict): dictionary of columns and custom string names of desired types,
            e.g. {'date': 'dt', 'quarter': 'q'}
        errors (str): 'errors' flag to pass to pd.to_datetime() method.
            Default: 'coerce', sets not convertable values to None. Optional
    
    Returns:
        Dataframe with converted types
    """
    for col, t in date_types.items():
        # iterate columns in the dict and check if they are present
        if col in df.columns:
            # catch value and type errors for every column
            try:
                if t == 'dt':
                    # if a column to be cast to datetime
                    
                    if not ptypes.is_datetime64_any_dtype(df[col]):
                        # if not already a datetime, convert
                        df[col] = pd.to_datetime(
                            df[col].astype(str),
                            errors=errors
                        )
                    else:
                        print(f'{col} is already in datetime type')
 
                if t == 'q':
                    if not ptypes.is_period_dtype(df[col]):
                        if not ptypes.is_datetime64_any_dtype(df[col]):
                            # if not already a datetime
                            df[col] = pd.to_datetime(
                                df[col].astype(str),
                                errors=errors
                            )
                            
                        # convert to Q regardless
                        df[col] = df[col].dt.to_period('Q')
                        
                    else:
                        print(f'{col} is already in period type')
            
            except ValueError as e:
                print(f'{col} cannot be transformed to type {t}')
                raise e          
                
            except TypeError:
                print(f'{col}: type error')
                pass

    return df                    


def set_types(df, nondate_types=nondate_types, date_types=date_types):
    for col, t in nondate_types.items():
        if col in df.columns:            
            try:
                df[col] = df[col].astype(t)
            except ValueError as e:
                print(f'{col} cannot be transformed to type {t}')
                raise e
    
    if date_types:
        df = set_date_types(df)
    
    return df
