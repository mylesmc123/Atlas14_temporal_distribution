# plot the differt temporal distributions of atlas 14 vs HMS vs SCS Type II
# %%
import datetime
from io import StringIO
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

# %%
scs_type_II_temporal_distribution = {
    0: 0,
    1: 0.2,
    2: 0.3,
    3: 0.6,
    4: 0.9,
    5: 1.8,
    6: 2.9,
    7: 4.6,
    8: 7.3,
    9: 11.4,
    10: 17.6,
    11: 14.6,
    12: 12.8,
    13: 9.3,
    14: 6.1,
    15: 3.7,
    16: 2.2,
    17: 1.3,
    18: 0.8,
    19: 0.5,
    20: 0.4,
    21: 0.2,
    22: 0.2,
    23: 0.1,
    24: 0.2
}

hms_temporal_distribution = {
    1: 0.30,
    2: 0.31,
    3: 0.33,
    4: 0.34,
    5: 0.35,
    6: 0.37,
    7: 0.39,
    8: 0.41,
    9: 0.43,
    10: 0.46,
    11: 0.48,
    12: 0.52,
    13: 0.52,
    14: 0.56,
    15: 0.61,
    16: 0.67,
    17: 0.75,
    18: 0.84,
    19: 1.55,
    20: 1.81,
    21: 2.17,
    22: 3.77,
    23: 5.71,
    24: 12.88,
    25: 36.89,
    26: 7.17,
    27: 4.35,
    28: 2.43,
    29: 1.97,
    30: 1.67,
    31: 0.90,
    32: 0.79,
    33: 0.71,
    34: 0.64,
    35: 0.59,
    36: 0.54,
    37: 0.53,
    38: 0.50,
    39: 0.47,
    40: 0.44,
    41: 0.42,
    42: 0.40,
    43: 0.38,
    44: 0.36,
    45: 0.35,
    46: 0.33,
    47: 0.32,
    48: 0.31
}

# %%
# Atlas 14 temporal distributions are pulled from a csv and have multiple quartile based tables within the sheet
quartiles_wanted = ['FIRST-QUARTILE','SECOND-QUARTILE','THIRD-QUARTILE','FOURTH-QUARTILE', 'ALL']
with open(r'data\Southeast\se_1_24h_temporal.csv', "r") as f:
        data = f.readlines()
# Only use the quartiles_wanted to get the table start indexes.
table_start_indexes = [i for i,v in enumerate(data) if "CUMULATIVE PERCENTAGES OF TOTAL PRECIPITATION" in v and any(q in v for q in quartiles_wanted)]
table_titles = [v for i,v in enumerate(data) if "CUMULATIVE PERCENTAGES OF TOTAL PRECIPITATION FOR" in v and any(q in v for q in quartiles_wanted)]
table_titles = [v.split("CUMULATIVE PERCENTAGES OF TOTAL PRECIPITATION FOR ")[-1].replace(" CASES\n","") for v in table_titles]

table_titles

# %%
table_start_indexes

# %%
# create a dataframe for all the temporal distributions, including the SCS Type II and HMS, and all the quartiles with their respective percentages.
df_all = pd.DataFrame({
    "time": [],
    "SCS Type II": [],
    "HMS": [],
    "Atlas 14 Q1 90": [],
    "Atlas 14 Q1 80": [],
    "Atlas 14 Q1 70": [],
    "Atlas 14 Q1 60": [],
    "Atlas 14 Q1 50": [],
    "Atlas 14 Q1 40": [],
    "Atlas 14 Q1 30": [],
    "Atlas 14 Q1 20": [],
    "Atlas 14 Q1 10": [],
    "Atlas 14 Q2 90": [],
    "Atlas 14 Q2 80": [],
    "Atlas 14 Q2 70": [],
    "Atlas 14 Q2 60": [],
    "Atlas 14 Q2 50": [],
    "Atlas 14 Q2 40": [],
    "Atlas 14 Q2 30": [],
    "Atlas 14 Q2 20": [],
    "Atlas 14 Q2 10": [],
    "Atlas 14 Q3 90": [],
    "Atlas 14 Q3 80": [],
    "Atlas 14 Q3 70": [],
    "Atlas 14 Q3 60": [],
    "Atlas 14 Q3 50": [],
    "Atlas 14 Q3 40": [],
    "Atlas 14 Q3 30": [],
    "Atlas 14 Q3 20": [],
    "Atlas 14 Q3 10": [],
    "Atlas 14 Q4 90": [],
    "Atlas 14 Q4 80": [],
    "Atlas 14 Q4 70": [],
    "Atlas 14 Q4 60": [],
    "Atlas 14 Q4 50": [],
    "Atlas 14 Q4 40": [],
    "Atlas 14 Q4 30": [],
    "Atlas 14 Q4 20": [],
    "Atlas 14 Q4 10": [],
    "Atlas 14 ALL 90": [],
    "Atlas 14 ALL 80": [],
    "Atlas 14 ALL 70": [],
    "Atlas 14 ALL 60": [],
    "Atlas 14 ALL 50": [],
    "Atlas 14 ALL 40": [],
    "Atlas 14 ALL 30": [],
    "Atlas 14 ALL 20": [],
    "Atlas 14 ALL 10": []
})

quartile_names = {
     "ALL": "ALL",
     "FIRST-QUARTILE": "Q1",
     "SECOND-QUARTILE": "Q2",
     "THIRD-QUARTILE": "Q3",
     "FOURTH-QUARTILE": "Q4"
}

# %%
 # For each quartile table, create a dataframe, assign the temporal distribution to the grid, stack the grids to a single xarray dataset, export a netCDF.
length_tables = len(table_start_indexes)
for i,table in enumerate(table_start_indexes):
    table_title = table_titles[i]
    print(f'Processing {table_title}')
    # table_headers are +2 rows from the table_start_index row.
    table_header_index = table + 2
    # ensure not at end of table before using the next table start index.
    if i < length_tables - 1:
        table = data[table_header_index:table_start_indexes[i+1]]
        table = [v.rstrip("\n") for v in table]
        # print (*table)
        df_table = pd.read_csv(StringIO("\n".join(table)), sep=",", header=0)
    else: # last table just grabs to end of file
        table = data[table_header_index:]
        table = [v.rstrip("\n") for v in table]
        df_table = pd.read_csv(StringIO("\n".join(table)), sep=",", header=0)
    
    # collecting data arrays for each timestep to stack into a single dataset.
    list_da = []
    
    start_time = datetime.datetime.utcfromtimestamp(0)
    # lets populate the df_all with the hours column f
    df_all["time"] = df_table["hours"]
    # get the quartile from the table title
    quartile = table_title.split(" ")[-1]
    quartile_name = quartile_names[quartile]
    df_all[f"Atlas 14 {quartile_name} 90"] = df_table["90%"]
    df_all[f"Atlas 14 {quartile_name} 80"] = df_table["80%"]
    df_all[f"Atlas 14 {quartile_name} 70"] = df_table["70%"]
    df_all[f"Atlas 14 {quartile_name} 60"] = df_table["60%"]
    df_all[f"Atlas 14 {quartile_name} 50"] = df_table["50%"]
    df_all[f"Atlas 14 {quartile_name} 40"] = df_table["40%"]
    df_all[f"Atlas 14 {quartile_name} 30"] = df_table["30%"]
    df_all[f"Atlas 14 {quartile_name} 20"] = df_table["20%"]
    df_all[f"Atlas 14 {quartile_name} 10"] = df_table["10%"]

# %%
# lets populate the SCS Type II and HMS temporal distributions.
df_all["SCS Type II"] = [scs_type_II_temporal_distribution.get(i, 0) for i in df_all["time"]]
df_all["HMS"] = [hms_temporal_distribution.get(i, 0) for i in df_all["time"]]
# %%
df_all
# %%
# anywhere except the first row, if the value is 0, then replace it with np.nan
df_all.iloc[1:, 1:] = df_all.iloc[1:, 1:].replace(0, np.nan)
df_all
# %%
