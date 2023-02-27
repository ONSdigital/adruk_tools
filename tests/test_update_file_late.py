"LEFTOVER: convert to proper unit test"

import pandas as pd

from importlib.machinery import SourceFileLoader
adr = SourceFileLoader("adr_functions",
                       "/home/cdsw/adruk_tools/adruk_tools/adr_functions.py"
                       ).load_module()

spark = adr.session_small()


# Create dataset
# --------------

# One dummy dataframe
df = pd.DataFrame({
    "Name": ['Nathan', 'Jo', 'James', 'Silvia', 'Johannes'],
    "Age": [1, 2, 2, 3, -2],
    "Sex": ['M', 'M', 'F', 'M', 'F'],
    "DOB": [True, False, None, True, False]
})

# Save in CDSW
df.to_csv('/home/cdsw/adruk_tools/test_data.csv', index=False)

# Second dummy dataframe in memory
template = pd.DataFrame({
    "Name": ['Nathan', 'Jo', 'Jon', 'James', 'Silvia'],
    "Occ": ['test1', 'test2', 'test3', 'test4', 'test5'],
    "Alive": [15, 0, -6, -5, 10],
    "Surname": ['a', 'b', None, 'c', 'd']
})


# Start tests
# -----------

# 1 Check failure if input dataset isnt csv
# ---------

adr.update_file_with_template('/home/cdsw/adruk_tools/nino.py',
                              template,
                              'Name')

adr.update_file_later_with_template('/home/cdsw/adruk_tools/nino.py',
                                    template,
                                    'Name',
                                    ['Surname'],
                                    ['Name', 'Age', 'Sex'],
                                    ['Name', 'Age', 'Sex', 'Occ', 'Alive'])

# 2 Check if file doesnt exist, it writes template
# ------------

adr.update_file_with_template('/home/cdsw/adruk_tools/test_temp.csv',
                              template,
                              'Name')

adr.update_file_later_with_template('/home/cdsw/adruk_tools/test_temp.csv',
                                    template,
                                    'Name',
                                    ['Surname'],
                                    ['Name', 'Age', 'Sex'],
                                    ['Name', 'Age', 'Sex', 'Occ', 'Alive'])


# 3 If any input only has one column (i.e. a pandas series) it should be converted
# to DF and file should be created.
# ------------

# Create new datasets with just one column
df_one_column = pd.DataFrame({
    "Name": ['Nathan', 'Jo', 'James', 'Silvia', 'Johannes'],
})

# Save in CDSW
df_one_column.to_csv('/home/cdsw/adruk_tools/test_data_one_col.csv', index=False)

template_one_column = pd.DataFrame({
    "Name": ['Nathan', 'Jo', 'Jon', 'James', 'Silvia'],
})


# Test one column in template
adr.update_file_with_template('/home/cdsw/adruk_tools/test_data.csv',
                              template_one_column,
                              'Name')

adr.update_file_later_with_template('/home/cdsw/adruk_tools/test_data.csv',
                                    template_one_column,
                                    'Name',
                                    [],
                                    ['Name', 'Age', 'Sex'],
                                    ['Name', 'Age'])


# Test one column input file
adr.update_file_with_template('/home/cdsw/adruk_tools/test_data_one_col.csv',
                              template,
                              'Name')

adr.update_file_later_with_template('/home/cdsw/adruk_tools/test_data_one_col.csv',
                                    template,
                                    'Name',
                                    ['Surname'],
                                    ['Name'],
                                    ['Name', 'Occ', 'Alive'])

# 4 General run - see github for comparision to datasets created
# using old functions.
# ------------

adr.update_file_with_template('/home/cdsw/adruk_tools/test_data.csv',
                              template,
                              'Name')

adr.update_file_later_with_template('/home/cdsw/adruk_tools/test_data.csv',
                                    template,
                                    'Name',
                                    ['Surname'],
                                    ['Name', 'Age', 'Sex'],
                                    ['Name', 'Age', 'Sex', 'Occ', 'Alive'])
