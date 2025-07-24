# plot the differt temporal distributions of atlas 14 vs HMS vs SCS Type II
# %%
import datetime
from io import StringIO
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

# %%
region = {
    'name': 'Midwest',
    'abbrev': 'mw',
    'area': '3'
}

# Cumulative distribution tables
scs_type_II_6hr_temporal_distribution = {
    0.0: 0.0,
    0.5: 2.1,
    1.0: 7.2,
    1.5: 15.5,
    2.0: 30.1,
    2.5: 51,
    3.0: 69.9,
    3.5: 78.3,
    4.0: 84.5,
    4.5: 89.2,
    5.0: 92.8,
    5.5: 95.8,
    6.0: 100
}


hms_6hr_temporal_distribution = {
    0.0: 0.0,
    0.5: 1.89,
    1.0: 4.08,
    1.5: 6.72,
    2.0: 11.30,
    2.5: 18.23,
    3.0: 33.86,
    3.5: 78.65,
    4.0: 87.35,
    4.5: 92.63,
    5.0: 95.58,
    5.5: 97.97,
    6.0: 100.00
}

scs_type_II_24hr_temporal_distribution = {
    0.0: 0,
    0.5: 0.25,
    1.0: 0.4,
    1.5: 0.75,
    2.0: 1.1,
    2.5: 1.6,
    3.0: 2.1,
    3.5: 2.85,
    4.0: 3.6,
    4.5: 4.7,
    5.0: 5.8,
    5.5: 7.3,
    6.0: 8.8,
    6.5: 10.8,
    7.0: 12.8,
    7.5: 15.45,
    8.0: 18.1,
    8.5: 21.6,
    9.0: 25.1,
    9.5: 27.6,
    10.0: 30.1,
    10.5: 33.5,
    11.0: 36.9,
    11.5: 40.85,
    12.0: 44.8,
    12.5: 49.55,
    13.0: 54.3,
    13.5: 60.05,
    14.0: 65.8,
    14.5: 70.8,
    15.0: 75.8,
    15.5: 80.2,
    16.0: 84.6,
    16.5: 87.1,
    17.0: 89.6,
    17.5: 90.6,
    18.0: 91.6,
    18.5: 93.0,
    19.0: 94.4,
    19.5: 95.5,
    20.0: 96.6,
    20.5: 97.3,
    21.0: 98.0,
    21.5: 98.4,
    22.0: 98.8,
    22.5: 99.1,
    23.0: 99.4,
    23.5: 99.7,
    24.0: 100.0
}

hms_24hr_temporal_distribution = {
    0.0: 0.00,
    0.5: 0.30,
    1.0: 0.62,
    1.5: 0.94,
    2.0: 1.28,
    2.5: 1.64,
    3.0: 2.01,
    3.5: 2.40,
    4.0: 2.81,
    4.5: 3.24,
    5.0: 3.70,
    5.5: 4.18,
    6.0: 4.70,
    6.5: 5.22,
    7.0: 5.78,
    7.5: 6.39,
    8.0: 7.06,
    8.5: 7.81,
    9.0: 8.65,
    9.5: 10.21,
    10.0: 12.01,
    10.5: 14.18,
    11.0: 17.96,
    11.5: 23.66,
    12.0: 36.54,
    12.5: 73.43,
    13.0: 80.60,
    13.5: 84.95,
    14.0: 87.38,
    14.5: 89.35,
    15.0: 91.02,
    15.5: 91.92,
    16.0: 92.71,
    16.5: 93.42,
    17.0: 94.06,
    17.5: 94.64,
    18.0: 95.18,
    18.5: 95.72,
    19.0: 96.22,
    19.5: 96.69,
    20.0: 97.13,
    20.5: 97.55,
    21.0: 97.95,
    21.5: 98.33,
    22.0: 98.69,
    22.5: 99.04,
    23.0: 99.37,
    23.5: 99.69,
    24.0: 100.00
}

tables = {
    'SCS Type II 24hr': {
        'name':scs_type_II_24hr_temporal_distribution,
        'dur': '24h'
    },
    'HMS 24hr': {
        'name': hms_24hr_temporal_distribution,
        'dur': '24h'
    },
    'SCS Type II 6hr': {
        'name': scs_type_II_6hr_temporal_distribution,
        'dur': '06h'
    },
    'HMS 6hr': {
        'name': hms_6hr_temporal_distribution,
        'dur': '06h'
    }
}

# %%
temporal_durations_wanted = ['6h', '12h', '24h']
temporal_duration_tables = {
    f'{dur}Distribution': f'data/{region["name"]}/{region["abbrev"]}_{region["area"]}_{dur}_temporal.csv'
    for dur in temporal_durations_wanted
}

# %%
# Create a DataFrame to hold all the temporal distributions
df_all = pd.DataFrame()
# Create a time column with 49 ordinates (0-48) for half-hour increments
df_all["time"] = [i * 0.5 for i in range(49)]  # 0 to 24 hours in 0.5 hour increments
# Atlas 14 temporal distributions are pulled from a csv per duration and have multiple quartile based tables within the sheet
quartiles_wanted = ['FIRST-QUARTILE','SECOND-QUARTILE','THIRD-QUARTILE','FOURTH-QUARTILE', 'ALL']
for dur, file_path in temporal_duration_tables.items():
    print(f'Processing {dur} temporal distribution from {file_path}')
    with open(file_path, "r") as f:
        data = f.readlines()    
    # Only use the quartiles_wanted to get the table start indexes.
    table_start_indexes = [i for i,v in enumerate(data) if 'CUMULATIVE PERCENTAGES OF TOTAL PRECIPITATION' in v and any(q in v for q in quartiles_wanted)]
    table_titles = [v for i,v in enumerate(data) if 'CUMULATIVE PERCENTAGES OF TOTAL PRECIPITATION FOR' in v and any(q in v for q in quartiles_wanted)]
    table_titles = [v.split('CUMULATIVE PERCENTAGES OF TOTAL PRECIPITATION FOR ')[-1].replace(' CASES\n','') for v in table_titles]
    
    print('Table titles:', table_titles)
    print('Table start indexes:', table_start_indexes)

    # %%
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
        
        # get the quartile from the table title
        quartile = table_title.split(" ")[-1]
        quartile_name = quartile_names[quartile]
        df_all[f"Atlas 14 Storm{dur} {quartile_name} 90"] = df_table["90%"]
        df_all[f"Atlas 14 Storm{dur} {quartile_name} 80"] = df_table["80%"]
        df_all[f"Atlas 14 Storm{dur} {quartile_name} 70"] = df_table["70%"]
        df_all[f"Atlas 14 Storm{dur} {quartile_name} 60"] = df_table["60%"]
        df_all[f"Atlas 14 Storm{dur} {quartile_name} 50"] = df_table["50%"]
        df_all[f"Atlas 14 Storm{dur} {quartile_name} 40"] = df_table["40%"]
        df_all[f"Atlas 14 Storm{dur} {quartile_name} 30"] = df_table["30%"]
        df_all[f"Atlas 14 Storm{dur} {quartile_name} 20"] = df_table["20%"]
        df_all[f"Atlas 14 Storm{dur} {quartile_name} 10"] = df_table["10%"]

# %%
df_all
print('Number of rows in df_all:', len(df_all))
# %%
# lets populate the SCS Type II and HMS temporal distributions.
# Populate SCS Type II and HMS temporal distributions using the 'tables' dictionary
# For 24h distributions
scs_24hr = tables['SCS Type II 24hr']['name']
hms_24hr = tables['HMS 24hr']['name']

# %%
df_scs_24hr = pd.DataFrame(list(scs_24hr.items()), columns=['time', 'SCS Type II 24hr'])
# Reindex df_scs_24hr to match df_all["time"] (49 ordinates, half-hour increments), ffill where needed
df_scs_24hr = df_scs_24hr.set_index("time").reindex(df_all["time"]).ffill().reset_index()
df_hms_24hr = pd.DataFrame(list(hms_24hr.items()), columns=['time', 'HMS 24hr'])
# Reindex df_hms_24hr to match df_all["time"] (49 ordin
df_hms_24hr = df_hms_24hr.set_index("time").reindex(df_all["time"]).ffill().reset_index()

# %%
df_all["SCS Type II 24hr"] = df_scs_24hr["SCS Type II 24hr"]
df_all["HMS 24hr"] = df_hms_24hr["HMS 24hr"]
# for 6h distributions
scs_6hr = tables['SCS Type II 6hr']['name']
hms_6hr = tables['HMS 6hr']['name']
df_all["SCS Type II 6hr"] = [scs_6hr.get(t, 0) for t in df_all["time"]]
df_all["HMS 6hr"] = [hms_6hr.get(t, 0) for t in df_all["time"]]

# %%
df_all
# %%
# anywhere except the first row, if the value is 0, then replace it with 100
df_all.iloc[1:, 1:] = df_all.iloc[1:, 1:].replace(0, 100.0)
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
# plt.show()

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
# plotly.io.show(fig)  # Show the plot in an interactive window
# %%
# for each column, compute the incremental change from the previous value
df_incremental = df_all.copy()
len(df_incremental.columns)
print ('\nNumber of rows in df_incremental before processing:', len(df_incremental))
print("Number of rows in df_all before processing:", len(df_all))
# %%
for column in df_incremental.columns[1:]:
    # print(f'Processing column: {column}')
    # drop nan values
    df_incremental_col = df_incremental[["time", column]].dropna()
    # compute the incremental change
    df_incremental_col = df_incremental_col.diff().fillna(df_incremental_col).round(4)
    # set missing to 0
    # df_incremental_col.fillna(0, inplace=True)
    # Forward fill to backfill missing value ordinates to get 49 (0-48) ordinates
    # df_incremental_col = df_incremental_col.reindex(range(0, 49), method='ffill')
    df_incremental[column] = df_incremental_col[column]

df_incremental
# %%
# print the number of rows
print('Number of rows in df_incremental after processing:', len(df_incremental))
# %%
# plotly plot the incremental changes
fig_incremental = px.line(df_incremental, x="time", y=df_incremental.columns[1:], title="Incremental Changes in Temporal Distributions of Precipitation")
# fig_incremental = px.line(df_incremental, x="time", y=df_incremental.columns[1:], title="")
fig_incremental.update_layout(
    xaxis_title="Time (hours)",
    yaxis_title="Incremental Change (%/%)",
    # legend_title="Distribution Type"
)

fig_incremental.write_html("temporal_distributions_incremental.html")
# plotly.io.show(fig_incremental)  # Show the plot in an interactive window

# %%
len(df_incremental.columns)
# %%
# export the df_incremental to an html file that looks like the ijupyter notebook tables
df_incremental.to_html("temporal_distributions_incremental_table.html", index=False, float_format='%.4f')

# %%
# Get the max value from each column in df_incremental except the first column (time)
# This will give us the maximum incremental change for each distribution type   
df_max_values = df_incremental[1:].max()
# drop the first row
df_max_values = df_max_values[1:]
# sort the values in descending order
df_max_values = df_max_values.sort_values(ascending=False)
# keep the first 10 values
df_max_values = df_max_values.head(10)
df_max_values
# %%
# from df_incremental, drop columns not in df_max_values.index
df_incremental_top10 = df_incremental[["time"] + df_max_values.index.tolist()]
df_incremental_top10
# %%
# make a plotly plot of the top 10 incremental changes
fig_incremental_top10 = px.line(df_incremental_top10, x="time", y=df_incremental_top10.columns[1:], title="Top 10 Incremental Changes in Temporal Distributions of Precipitation")
fig_incremental_top10.update_layout(
    xaxis_title="Time (hours)",
    yaxis_title="Incremental Change (%/%)",
    legend_title="Distribution Type"
)
fig_incremental_top10.write_html("temporal_distributions_incremental_top10.html")
# %%
