# %%
import xarray as xr
import datetime
import rioxarray
import pandas as pd
import numpy as np
from io import StringIO
from tqdm import tqdm
import os
import geopandas as gpd

# setup project dir data if custom merged diffrent from the Atlas 14 grid extent by region
project_name = 'Lawton'
clip_shp = "boundaries/Lawton_BB_StatePlaneOK.shp"
# merged will be a merge of multiple Atlas 14 grids and clipped to the project extent already.
merged = False

region = {
    'name': 'Midwest',
    'abbrev': 'mw',
}

years_padded = ['001', '002', '005', '010', '025', '050', '100', '200', '500']
years_int = [int(year) for year in years_padded] 

precip_durations = ['06h', '24h']
temporal_durations = ['06h', '24h']


# Cumulative distribution tables, the keys are hours
scs_type_II_6hr_temporal_distribution = {
    0.00: 0.00,
    0.25: 1.00,
    0.50: 2.20,
    0.75: 4.10,
    1.00: 7.00,
    1.25: 10.80,
    1.50: 15.50,
    1.75: 21.00,
    2.00: 27.60,
    2.25: 35.10,
    2.50: 43.30,
    2.75: 51.90,
    3.00: 60.70,
    3.25: 69.10,
    3.50: 76.80,
    3.75: 83.10,
    4.00: 88.00,
    4.25: 91.80,
    4.50: 94.70,
    4.75: 96.70,
    5.00: 98.00,
    5.25: 98.90,
    5.50: 99.50,
    5.75: 99.80,
    6.00: 100.00
}

hms_6hr_temporal_distribution = {
    0.00: 0.00,
    0.25: 0.93,
    0.50: 1.92,
    0.75: 3.00,
    1.00: 4.16,
    1.25: 5.45,
    1.50: 6.88,
    1.75: 9.10,
    2.00: 11.63,
    2.25: 14.93,
    2.50: 18.99,
    2.75: 26.05,
    3.00: 39.71,
    3.25: 70.84,
    3.50: 79.41,
    3.75: 84.06,
    4.00: 87.69,
    4.25: 90.43,
    4.50: 92.79,
    4.75: 94.31,
    5.00: 95.67,
    5.25: 96.89,
    5.50: 98.01,
    5.75: 99.04,
    6.00: 100.00
}

scs_type_II_24hr_temporal_distribution = {
    0: 0,
    1: 0.2,
    2: 0.5,
    3: 1.1,
    4: 2,
    5: 3.8,
    6: 6.7,
    7: 11.3,
    8: 18.6,
    9: 30,
    10: 47.6,
    11: 62.2,
    12: 75,
    13: 84.3,
    14: 90.4,
    15: 94.1,
    16: 96.3,
    17: 97.6,
    18: 98.4,
    19: 98.9,
    20: 99.3,
    21: 99.5,
    22: 99.7,
    23: 99.8,
    24: 100
}

# Updated HMS 24hr temporal distribution values
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

grids = {}
for i, year in enumerate(years_padded):
    for dur in precip_durations:
        if not merged:
            grids[f'{year}yr_Partial_Duration_{dur}Precip'] = {
                'path': f'data/{region["name"]}/{region["abbrev"]}{years_int[i]}yr{dur}a/{region["abbrev"]}{years_int[i]}yr{dur}a.asc',
                # 'path': f'data/I57.tif',
                'year': year,
                'year_int': years_int[i],
                'duration': dur
            }
        else:
            grids[f'{year}yr_Partial_Duration_{dur}Precip'] = {
                'path': f'data/Merged/{project_name}/{project_name}{year}yr{dur}a.asc',
                'year': year,
                'year_int': years_int[i],
                'duration': dur
            }

# %%
for dur in precip_durations:
    temporal_duration_name = f'{dur}Distribution'
    print(f'\nProcessing {temporal_duration_name} temporal distribution...')
    for grid in grids:
        # Check if the grid duration matches the precip duration
        if grids[grid]['duration'] != dur:
            print(f'Skipping {grid} with duration {grids[grid]["duration"]} for {temporal_duration_name}.')
            continue
        grid_name = grid
        print(f'\nProcessing {grids[grid]['path']}...')
        grid_file = grids[grid]['path']
        da = rioxarray.open_rasterio(grid_file, masked=True)
        # # Convert units to inches.
        da = da/1000
        # da.squeeze().plot()  # Show the raster in interactive environments
        if not merged:
            # clip the raster using clip_shp
            # open the shapefile using geopandas
            clip_gdf = gpd.read_file(clip_shp)
            # set the crs to 4326
            clip_gdf = clip_gdf.to_crs("EPSG:4326")
            # set raster to crs EPSG:4326
            da = da.rio.set_crs("EPSG:4326", inplace=True)
            # clip the raster using the shapefile
            from shapely.geometry import mapping
            da = da.rio.clip(clip_gdf.geometry.apply(mapping),
                            crs=clip_gdf.crs, drop=True, all_touched=True)
            
        for tables_name, table_info in tables.items():
            # check if the temporal distribution duration matches the storm duration
            print (f"temporal_dur = {table_info['dur']}, storm_dur = {grids[grid]['duration']}")
            if table_info['dur'] != grids[grid]['duration']:
                print(f'Skipping {tables_name} with duration {table_info["dur"]} for {grid_name} with duration {grids[grid]["duration"]}.\n')
                continue
            table = table_info['name']
            print(f'Applying {tables_name} temporal distribution...')
            # put the temporal distribution table into a pandas DataFrame
            df_table = pd.DataFrame.from_dict(table, orient='index', columns=['value'])
            df_table.index.name = 'hours'
            df_table.reset_index(inplace=True)

            # collecting data arrays for each timestep to stack into a single dataset.
            list_da = []
            start_time = datetime.datetime(2000, 1, 1, 0, 0, 0)
            
            for index, row in df_table.iterrows():
                # calculate the time for each row
                timestep = start_time + datetime.timedelta(hours=row['hours'])
                da_copy = da.copy(deep=True)
            
                # Convert Units to the {temporal_value_occurrence_column} Occurance Temporal Value increment to create a dataarray to be stacked into a dataset with a time dimension.
                da_copy = da_copy*(df_table['value'].iloc[index]/100.0)  # Convert percentage to decimal
                #  Rename data array data variable
                da_copy = da_copy.rename('PrecipCumulative')
                # Assign time coordinate  
                da_copy = da_copy.assign_coords(time = timestep)
                da_copy = da_copy.expand_dims(dim="time")
                # Append to list
                list_da.append(da_copy)

            # stack the dataarrays into a single dataset.
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
            ds['PrecipCumulative'].attrs['cell_methods'] = 'time: sum'
            
            ds['PrecipInc'].attrs['units'] = 'inches'
            ds['PrecipInc'].attrs['long_name'] = 'Incremental Precipitation'
            ds['PrecipInc'].attrs['cell_methods'] = 'time: sum'
            
            # Add temporal distribution to the dataset.
            ds_td = df_table.to_xarray()
            ds_td.expand_dims(dim="time")
            ds_td["time"] = ds.time
            ds_td["value"] = ds_td["value"].swap_dims({"index":"time"})
            ds_td = ds_td.drop_vars("hours")
            ds_td = ds_td.drop_vars("index")
            ds_td = ds_td.rename({"value":"TemporalDistribution"})
            ds = xr.merge([ds,ds_td])
            ds['TemporalDistribution'].attrs['units'] = 'percent'
            ds['TemporalDistribution'].attrs['long_name'] = 'Temporal Distribution Cumulative Percentage'
            ds['TemporalDistribution'].attrs['temporalDuration'] = temporal_duration_name
            
            # add time bounds
            ds['time'].attrs['bounds'] = 'time_bnds'
            # the time bounds are time1, time2, time2, time3, time3, time4, ...
            time_bnds = np.empty((len(ds['time'])-1, 2), dtype='datetime64[ns]')
            time_bnds[:, 0] = ds['time'].values[:-1]
            time_bnds[:, 1] = ds['time'].values[1:]
            time_bnds = xr.DataArray(time_bnds, dims=['time', 'bnds'], coords={'time': ds['time'].values[:-1], 'bnds': [0, 1]})
            time_bnds.name = 'time_bnds'
            ds['time_bnds'] = time_bnds
            ds['time_bnds'].attrs['standard_name'] = 'time_bnds'
            ds['time_bnds'].attrs['long_name'] = 'Time Bounds'
            ds['time_bnds'].attrs['description'] = 'Start and end of each time period'

            # Export to netCDF
            output_file = rf"output\{project_name}\nc\Atlas14_{project_name}{grids[grid]['year_int']}yr_{grids[grid]['duration']}Storm_{temporal_duration_name}_{tables_name}.nc"
            # create output directory if it does not exist
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            print(f'Exporting to {output_file}')
            ds.to_netcdf(output_file)

                
# %%
# Next Step is to run the Jython script to convert the netCDF to a DSS file.
