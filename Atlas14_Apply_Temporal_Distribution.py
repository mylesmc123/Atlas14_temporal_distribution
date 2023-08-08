# %%
import xarray as xr
import datetime
import rioxarray
import pandas as pd
from io import StringIO
from tqdm import tqdm

temporal_value_occurrence = 0.50 # as a float
temporal_value_occurrence_name = str(int(temporal_value_occurrence *100))+'PercentOccurence'
temporal_value_occurrence_column = str(int(temporal_value_occurrence *100))+'%'

region = {
    'name': 'Texas',
    'abbrev': 'tx'
}

temporal_duration_table = f'data\{region["name"]}\{region["abbrev"]}_3_24h_temporal.csv'
temporal_duration_name = '24hDistribution'

grids = {
    '002yr_Partial_Duration_24hPrecip': {
        'path': f'data\{region["name"]}\{region["abbrev"]}2yr24ha\{region["abbrev"]}2yr24ha.asc',
    },
    '005yr_Partial_Duration_24hPrecip': {
        'path': f'data\{region["name"]}\{region["abbrev"]}5yr24ha\{region["abbrev"]}5yr24ha.asc',
    },
    '010yr_Partial_Duration_24hPrecip': {
        'path': f'data\{region["name"]}\{region["abbrev"]}10yr24ha\{region["abbrev"]}10yr24ha.asc',
    },
    '025yr_Partial_Duration_24hPrecip': {
        'path': f'data\{region["name"]}\{region["abbrev"]}25yr24ha\{region["abbrev"]}25yr24ha.asc',
    },
    '50yr_Partial_Duration_24hPrecip': {
        'path': f'data\{region["name"]}\{region["abbrev"]}50yr24ha\{region["abbrev"]}50yr24ha.asc',
    },
    '100yr_Partial_Duration_24hPrecip': {
        'path': f'data\{region["name"]}\{region["abbrev"]}100yr24ha\{region["abbrev"]}100yr24ha.asc',
    },
    '500yr_Partial_Duration_24hPrecip': {
        'path': f'data\{region["name"]}\{region["abbrev"]}500yr24ha\{region["abbrev"]}500yr24ha.asc',
    },
    '100yr_Partial_Duration_05mPrecip' : {
        'path': f'data\{region["name"]}\{region["abbrev"]}100yr05ma\{region["abbrev"]}100yr05ma.asc',
    },   
    '100yr_Partial_Duration_15mPrecip': {
        'path': f'data\{region["name"]}\{region["abbrev"]}100yr15ma\{region["abbrev"]}100yr15ma.asc',
    },
    '100yr_Partial_Duration_30mPrecip': {
        'path': f'data\{region["name"]}\{region["abbrev"]}100yr30ma\{region["abbrev"]}100yr30ma.asc',
    },
    '100yr_Partial_Duration_02hPrecip': {
        'path': f'data\{region["name"]}\{region["abbrev"]}100yr02ha\{region["abbrev"]}100yr02ha.asc',
    },
    '100yr_Partial_Duration_06hPrecip': {
        'path': f'data\{region["name"]}\{region["abbrev"]}100yr06ha\{region["abbrev"]}100yr06ha.asc',
    },
    '100yr_Partial_Duration_12hPrecip': {
        'path': f'data\{region["name"]}\{region["abbrev"]}100yr12ha\{region["abbrev"]}100yr12ha.asc',
    },
    '100yr_Partial_Duration_60mPrecip': {
        'path': f'data\{region["name"]}\{region["abbrev"]}100yr60ma\{region["abbrev"]}100yr60ma.asc',
    },
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

    # %%
    table_start_indexes = [i for i,v in enumerate(data) if "CUMULATIVE PERCENTAGES OF TOTAL PRECIPITATION" in v]
    table_start_indexes

    # %%
    table_titles = [v for i,v in enumerate(data) if "CUMULATIVE PERCENTAGES OF TOTAL PRECIPITATION FOR" in v]
    table_titles = [v.split("CUMULATIVE PERCENTAGES OF TOTAL PRECIPITATION FOR ")[-1].replace(" CASES\n","") for v in table_titles]
    table_titles  

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



