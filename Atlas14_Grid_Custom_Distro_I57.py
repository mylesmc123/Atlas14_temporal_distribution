# %%
import xarray as xr
import datetime
import rioxarray
import pandas as pd
import numpy as np
import os
import geopandas as gpd

# setup project dir data if custom merged diffrent from the Atlas 14 grid extent by region
project_name = 'I57_'
clip_shp = "boundaries/I57_BB_3857.shp"
# merged will be a merge of multiple Atlas 14 grids and clipped to the project extent already.
merged = True

region = {
    'name': 'Midwest',
    'abbrev': 'mw',
}

years_padded = ['001', '002', '005', '010', '025', '050', '100', '200', '500']
years_int = [int(year) for year in years_padded] 

precip_durations = ['24h']

temporal_distribution_csv = r'data/HydroCad Rainfall Temporal Distribtions.csv'
temporal_distribution_hours_column = 'Time (hours)'
# Match normalized CSV headers to output filename aliases.
temporal_distribution_header_aliases = {
    'MSE1 24-hr (depth)': 'MSE1_24hr',
    'MSE2 24-hr (depth)': 'MSE2_24hr',
    'MSE3 24-hr (depth)': 'MSE3_24hr',
    'MSE4 24-hr (depth)': 'MSE4_24hr',
    'MSE5 24-hr (depth)': 'MSE5_24hr',
    'MSE6 24-hr (depth)': 'MSE6_24hr',
    'Type I 24-hr (depth)': 'TypeI_24hr',
    'Type IA 24-hr (depth)': 'TypeIA_24hr',
    'Type II 6-hr (depth)': 'TypeII_6hr',
    'Type II 12-hr (depth)': 'TypeII_12hr',
    'Type II 24-hr (depth)': 'TypeII_24hr',
    'Type III 6-hr (depth)': 'TypeIII_6hr',
    'Type III 12-hr (depth)': 'TypeIII_12hr',
    'Type III 24-hr (depth)': 'TypeIII_24hr',
}

# Set to None to use all distributions, or provide a list of aliases to run a subset.
selected_temporal_distribution_aliases = [
    'TypeII_24hr',
    'MSE5_24hr',
]


def normalize_csv_headers(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = df.columns.str.replace(r'\s+', ' ', regex=True).str.strip()
    return df

temporal_distribution_df = normalize_csv_headers(pd.read_csv(temporal_distribution_csv))

if selected_temporal_distribution_aliases is None:
    selected_temporal_distribution_header_aliases = temporal_distribution_header_aliases
else:
    available_aliases = set(temporal_distribution_header_aliases.values())
    selected_aliases = set(selected_temporal_distribution_aliases)
    missing_aliases = sorted(selected_aliases - available_aliases)
    if missing_aliases:
        raise KeyError(f'Missing temporal distribution aliases: {missing_aliases}')

    selected_temporal_distribution_header_aliases = {
        column_name: alias
        for column_name, alias in temporal_distribution_header_aliases.items()
        if alias in selected_aliases
    }

if temporal_distribution_hours_column not in temporal_distribution_df.columns:
    raise KeyError(f'Missing hours column: {temporal_distribution_hours_column}')

missing_distribution_columns = [
    column_name for column_name in selected_temporal_distribution_header_aliases
    if column_name not in temporal_distribution_df.columns
]

if missing_distribution_columns:
    raise KeyError(f'Missing temporal distribution columns: {missing_distribution_columns}')


def sanitize_output_name(name: str) -> str:
    return (
        name.replace('/', '_')
            .replace('\\', '_')
            .replace('(', '')
            .replace(')', '')
            .replace('-', '_')
            .replace(' ', '_')
    )


tables = {
    column_name: {
        'alias': alias,
        'table': temporal_distribution_df[
            [temporal_distribution_hours_column, column_name]
        ].rename(columns={
            temporal_distribution_hours_column: 'hours',
            column_name: 'value',
        })
    }
    for column_name, alias in selected_temporal_distribution_header_aliases.items()
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
            # grids must be previously  merged using grid_merge_clip.py
            grids[f'{year}yr_Partial_Duration_{dur}Precip'] = {
                'path': f'data/Merged/{project_name}/{project_name}{year}yr{dur}a.asc',
                'year': year,
                'year_int': years_int[i],
                'duration': dur
            }

# %%
for grid in grids:
    grid_name = grid
    print(f"\nProcessing {grids[grid]['path']}...")
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
        
    for tables_name, table_entry in tables.items():
        table_alias = table_entry['alias']
        table = table_entry['table']
        print(f'Applying {tables_name} temporal distribution...')
        df_table = table.copy()
        df_table = df_table.sort_values('hours').reset_index(drop=True)
        df_table['value'] = pd.to_numeric(df_table['value'], errors='coerce')
        df_table['increment'] = df_table['value'].diff().fillna(df_table['value'])

        if df_table['value'].isna().any():
            raise ValueError(f'{tables_name} contains non-numeric distribution values.')

        # collecting data arrays for each timestep to stack into a single dataset.
        list_da = []
        start_time = datetime.datetime(1970, 1, 1, 0, 0, 0)
        
        for index, row in df_table.iterrows():
            # calculate the time for each row
            timestep = start_time + datetime.timedelta(hours=row['hours'])
            da_copy = da.copy(deep=True)
        
            # The CSV values are cumulative fractions, so use them directly for the cumulative field.
            da_copy = da_copy * row['value']
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
        ds['PrecipInc'] = ds['PrecipCumulative'].diff(dim='time', label='upper').reindex(time=ds.time, fill_value=0)

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
        ds_td = xr.Dataset(
            data_vars={
                'TemporalDistribution': ('time', df_table['value'].to_numpy())
            },
            coords={
                'time': ds.time,
                'hours': ('time', df_table['hours'].to_numpy())
            }
        )
        ds = xr.merge([ds,ds_td])
        ds['TemporalDistribution'].attrs['units'] = 'fraction'
        ds['TemporalDistribution'].attrs['long_name'] = 'Temporal Distribution Cumulative Fraction'
        ds['TemporalDistribution'].attrs['temporalDuration'] = table_alias
        ds['TemporalDistribution'].attrs['source'] = f'{tables_name} Temporal Distribution Table (alias: {table_alias})'

        # Export to netCDF
        table_alias_safe = sanitize_output_name(table_alias)
        output_file = rf"output\{project_name}\nc\Atlas14_{project_name}{grids[grid]['year_int']}yr_{table_alias_safe}.nc"
        # create output directory if it does not exist
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        print(f'Exporting to {output_file}\n')
        ds.to_netcdf(output_file)

                
# %%
# Next Step is to run the Jython script to convert the netCDF to a DSS file.
