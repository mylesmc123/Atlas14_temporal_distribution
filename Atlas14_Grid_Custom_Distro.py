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
clip_shp = "boundaries\Lawton_BB_StatePlaneOK.shp"
# merged will be a merge of multiple Atlas 14 grids and clipped to the project extent already.
merged = False

region = {
    'name': 'Midwest',
    'abbrev': 'mw',
}

years_padded = ['001', '002', '005', '010', '025', '050', '100', '200', '500']
years_int = [int(year) for year in years_padded] 

precip_durations = ['24h']
temporal_duration_name = '24hDistribution'

scs_type_II_temporal_distribution = {
    0: 0.00,
    1: 0.20,
    2: 0.50,
    3: 1.10,
    4: 2.00,
    5: 3.80,
    6: 6.70,
    7: 11.30,
    8: 18.60,
    9: 30.00,
    10: 47.60,
    11: 62.20,
    12: 75.00,
    13: 84.30,
    14: 90.40,
    15: 94.10,
    16: 96.30,
    17: 97.60,
    18: 98.40,
    19: 98.90,
    20: 99.30,
    21: 99.50,
    22: 99.70,
    23: 99.80,
    24: 100.00
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

tables = {
    'SCS Type II': scs_type_II_temporal_distribution,   
    'HMS': hms_temporal_distribution
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
for grid in grids:
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
        
        for tables_name, table in tables.items():
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
            
            ds['PrecipInc'].attrs['units'] = 'inches'
            ds['PrecipInc'].attrs['long_name'] = 'Incremental Precipitation'
            
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
            ds['TemporalDistribution'].attrs['source'] = f'{tables_name} Temporal Distribution Table'

            # Export to netCDF
            output_file = rf"output\{project_name}\nc\Atlas14_{project_name}{grids[grid]['year_int']}yr_{temporal_duration_name}_{tables_name}.nc"
            # create output directory if it does not exist
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            print(f'Exporting to {output_file}')
            ds.to_netcdf(output_file)

                
# %%
# Next Step is to run the Jython script to convert the netCDF to a DSS file.
