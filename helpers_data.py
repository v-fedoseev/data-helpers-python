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



