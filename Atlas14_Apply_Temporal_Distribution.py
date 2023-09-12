# %%
import xarray as xr
import datetime
import rioxarray
import pandas as pd
import numpy as np
from io import StringIO
from tqdm import tqdm

temporal_value_occurrence = 0.50 # as a float
temporal_value_occurrence_name = str(int(temporal_value_occurrence *100))+'PercentOccurence'
temporal_value_occurrence_column = str(int(temporal_value_occurrence *100))+'%'

# Add optional frontend rampup time to the start_time by zilling zeroes to the data
ramp_up_time_hours = 48

region = {
    'name': 'Southeast',
    'abbrev': 'se'
}

# quartiles_wanted = ['FIRST-QUARTILE','SECOND-QUARTILE','THIRD-QUARTILE','FOURTH-QUARTILE', 'ALL']
quartiles_wanted = ['ALL']

temporal_duration_table = f'data\{region["name"]}\{region["abbrev"]}_1_24h_temporal.csv'
temporal_duration_name = '24hDistribution'

years_padded = ['002', '010', '025', '050', '100', '500']
years_int = [int(year) for year in years_padded] 

precip_durations = ['05m', '60m', '06h', '12h', '24h']

grids = {}
for i, year in enumerate(years_padded):
    for dur in precip_durations:
        grids[f'{year}yr_Partial_Duration_{dur}Precip'] = {
            'path': f'data\{region["name"]}\{region["abbrev"]}{years_int[i]}yr{dur}a\{region["abbrev"]}{years_int[i]}yr{dur}a.asc',
        }

for grid in tqdm(grids):
    grid_name = grid
    print(f'\nProcessing {grid_name}...')
    grid_file = grids[grid]['path']

    da = rioxarray.open_rasterio(grid_file, masked=True)
    # # Convert units to inches.
    da = da/1000
    # da.squeeze().plot.imshow()

    # %%
    da

    # %%

    # Open Temporal Distribution Tables downloaded from NOAA
    # CSV source: https://hdsc.nws.noaa.gov/pfds/pfds_temporal.html
    with open(temporal_duration_table, "r") as f:
        data = f.readlines()
    # data.strip('\n')

    # Only use the quartiles_wanted to get the table start indexes.
    table_start_indexes = [i for i,v in enumerate(data) if "CUMULATIVE PERCENTAGES OF TOTAL PRECIPITATION" in v and any(q in v for q in quartiles_wanted)]
    table_titles = [v for i,v in enumerate(data) if "CUMULATIVE PERCENTAGES OF TOTAL PRECIPITATION FOR" in v and any(q in v for q in quartiles_wanted)]
    table_titles = [v.split("CUMULATIVE PERCENTAGES OF TOTAL PRECIPITATION FOR ")[-1].replace(" CASES\n","") for v in table_titles]

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
        # starting data at epoch time + 0.25 hours = 01JAN1970 00:15:00. HEC-Vortex Timeshift bug workaround. DSS starTime will be 01JAN1970 00:00:00.
        start_time = datetime.datetime.utcfromtimestamp(0) +  datetime.timedelta(hours=0.25)
        
        
        ramp_up_table_rows = ramp_up_time_hours * 2 # temporal distribution table is in 30 minute increments
        # Create zeroes array over the rampup time and add that to the df_table
        table_row_hours = np.arange(0.0, 48.5, 0.5)
        # table_row_hours
        
        df_table_rampup = pd.DataFrame({
            'hours':table_row_hours, 
            f'{temporal_value_occurrence_column}':0*ramp_up_time_hours
        })

        # Append the rampup table to the df_table by adding the value of last row of the rampup table to the hours column of the df_table.
        df_table['hours'] = df_table['hours'] + df_table_rampup['hours'].iloc[-1]
        # df_table = df_table_rampup.append(df_table, ignore_index=True)
        df_table['hours']
        
        # drop final row of rampup table before appending df_table
        df_table_rampup.drop(df_table_rampup.tail(1).index,inplace=True)
        df_table = df_table_rampup.append(df_table, ignore_index=True)
        df_table.fillna(0, inplace=True)
        df_table


        # %%
        # for each timestep in the table, assign the temporal distribution to the grid.
        for index, row in df_table.iterrows():
            timestep = start_time + datetime.timedelta(hours=row['hours'])
            
            da_copy = da.copy(deep=True)
            
            # Convert Units to the {temporal_value_occurrence_column} Occurance Temporal Value increment to create a dataarray to be stacked into a dataset with a time dimension.
            da_copy = da_copy*(row[f'{temporal_value_occurrence_column}']/100)
            #  Rename data array data variable
            da_copy = da_copy.rename('PrecipCumulative')
            # Assign time coordinate  
            da_copy = da_copy.assign_coords(time = timestep)
            da_copy = da_copy.expand_dims(dim="time")
            # Append to list
            list_da.append(da_copy)

        # stack the dataarrays into a single dataset.
        # ds = xr.combine_by_coords(list_da)
        ds = xr.concat(list_da, dim="time")
        ds = ds.to_dataset(name='PrecipCumulative')
        
        # Remove band dimension.
        ds = ds.squeeze()
        ds = ds.drop_vars('band')

        # Create Precip Incremental variable
        ds['PrecipInc'] = ds['PrecipCumulative'].diff(dim='time', label='upper')

        # CF Conventions
        ds = ds.rename({
            'x':'longitude',
            'y':'latitude'
        })

        ds['latitude'].attrs['units'] = 'degrees_north'
        ds['latitude'].attrs['standard_name'] = 'latitude'
        ds['latitude'].attrs['long_name'] = 'latitude'
        ds['latitude'].attrs['axis'] = 'Y'

        ds['longitude'].attrs['units'] = 'degrees_east'
        ds['longitude'].attrs['standard_name'] = 'longitude'
        ds['longitude'].attrs['long_name'] = 'longitude'
        ds['longitude'].attrs['axis'] = 'X'

        ds['time'].attrs['standard_name'] = 'time'
        ds['time'].attrs['long_name'] = 'time'
        ds['time'].attrs['axis'] = 'T'

        ds['PrecipCumulative'].attrs['units'] = 'inches'
        ds['PrecipCumulative'].attrs['long_name'] = 'Cumulative Precipitation'
        
        ds['PrecipInc'].attrs['units'] = 'inches'
        ds['PrecipInc'].attrs['long_name'] = 'Incremental Precipitation'
        
        # Add temporal distribution to the dataset.
        ds_td = df_table[['hours',f'{temporal_value_occurrence_column}']].to_xarray()
        ds_td.expand_dims(dim="time")
        ds_td["time"] = ds.time
        ds_td[f'{temporal_value_occurrence_column}'] = ds_td[f'{temporal_value_occurrence_column}'].swap_dims({"index":"time"})
        ds_td = ds_td.drop_vars("hours")
        ds_td = ds_td.drop_vars("index")
        ds_td = ds_td.rename({f"{temporal_value_occurrence_column}":"TemporalDistribution"})
        ds = xr.merge([ds,ds_td])
        ds['TemporalDistribution'].attrs['units'] = 'percent'
        ds['TemporalDistribution'].attrs['long_name'] = 'Temporal Distribution Culuative Percentage'
        ds['TemporalDistribution'].attrs['occurence'] = f'{temporal_value_occurrence_column}'
        ds['TemporalDistribution'].attrs['temporalDuration'] = temporal_duration_name
        ds['TemporalDistribution'].attrs['source'] = f'NOAA Atlas 14: {temporal_duration_table}'

        # Export to netCDF
        output_file = rf"output\{region['name']}\nc\Atlas14_{region['name']}_{grid_name}_{temporal_duration_name}_{temporal_value_occurrence_name}_{table_title}.nc"
        ds.to_netcdf(output_file)

        # Next Step is to run the Jython script to convert the netCDF to a DSS file.

    # %%
    ds['PrecipCumulative'].isel(time=1).plot()

    # %%
    ds['PrecipCumulative'].sel(latitude=32, longitude=-88, method='nearest').plot()