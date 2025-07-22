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
    0: 0.00,
    1: 0.30,
    2: 0.62,
    3: 0.94,
    4: 1.28,
    5: 1.64,
    6: 2.01,
    7: 2.40,
    8: 2.81,
    9: 3.24,
    10: 3.70,
    11: 4.18,
    12: 4.70,
    13: 5.22,
    14: 5.78,
    15: 6.39,
    16: 7.06,
    17: 7.81,
    18: 8.65,
    19: 10.21,
    20: 12.01,
    21: 14.18,
    22: 17.96,
    23: 23.66,
    24: 36.54,
    25: 73.43,
    26: 80.60,
    27: 84.95,
    28: 87.38,
    29: 89.35,
    30: 91.02,
    31: 91.92,
    32: 92.71,
    33: 93.42,
    34: 94.06,
    35: 94.64,
    36: 95.18,
    37: 95.72,
    38: 96.22,
    39: 96.69,
    40: 97.13,
    41: 97.55,
    42: 97.95,
    43: 98.33,
    44: 98.69,
    45: 99.04,
    46: 99.37,
    47: 99.69,
    48: 100.00
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
# For HMS, we need to map the 49 time ordinates to HMS keys 0-48
df_all["HMS"] = [hms_temporal_distribution.get(idx, 0) for idx in range(len(df_all["time"]))]
# %%
df_all
# %%
# anywhere except the first row, if the value is 0, then replace it with np.nan
df_all.iloc[1:, 1:] = df_all.iloc[1:, 1:].replace(0, np.nan)
df_all
# %%
# Plot the temporal distributions all together
plt.figure(figsize=(12, 8))
for column in df_all.columns[1:]:
    plt.plot(df_all["time"], df_all[column], label=column)
plt.xlabel("Time (hours)")
plt.ylabel("Cumulative Precipitation (mm)")
plt.title("Temporal Distributions of Precipitation")
plt.legend()
plt.grid()
plt.show()

# %%
# lets use plitly and export the figure to an html file
import plotly
import plotly.express as px
fig = px.line(df_all, x="time", y=df_all.columns[1:], title="Temporal Distributions of Precipitation")
fig.update_layout(
    xaxis_title="Time (hours)",
    yaxis_title="Cumulative Precipitation (mm)",
    legend_title="Distribution Type"
)
fig.write_html("temporal_distributions.html")
plotly.io.show(fig)  # Show the plot in an interactive window
# %%
# for each column, compute the incremental change from the previous value
df_incremental = df_all.copy()
for column in df_incremental.columns[1:]:
    # print(f'Processing column: {column}')
    # drop nan values
    df_incremental_col = df_incremental[["time", column]].dropna()
    # compute the incremental change
    df_incremental[column] = df_incremental[column].diff().fillna(df_incremental[column])

# %%
df_incremental = df_incremental.round(4)
df_incremental.fillna(method='ffill', inplace=True)
df_incremental

# %%
# plotly plot the incremental changes
fig_incremental = px.line(df_incremental, x="time", y=df_incremental.columns[1:], title="Incremental Changes in Temporal Distributions of Precipitation")
fig_incremental.update_layout(
    xaxis_title="Time (hours)",
    yaxis_title="Incremental Change (%/%)",
    legend_title="Distribution Type"
)
fig_incremental.write_html("temporal_distributions_incremental.html")
plotly.io.show(fig_incremental)  # Show the plot in an interactive window

# %%
