# %%
import json
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

def make_custom_distro_grids(
    project_name,
    clip_shp,
    merged,
    region,
    years_wanted,
    precip_durations,
    tables_path
):
    years_int = [int(year) for year in years_padded]

    # Load temporal distribution tables from JSON file
    with open(tables_path, 'r') as f:
        tables = json.load(f)

    grids = {}
    for i, year in enumerate(years_padded):
        for dur in precip_durations:
            if not merged:
                grids[f'{year}yr_Partial_Duration_{dur}Precip'] = {
                    'path': f'data/{region["name"]}/{region["abbrev"]}{years_int[i]}yr{dur}a/{region["abbrev"]}{years_int[i]}yr{dur}a.asc',
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

    for dur in precip_durations:
        temporal_duration_name = f'{dur}Distribution'
        print(f'\nProcessing {temporal_duration_name} temporal distribution...')
        for grid in grids:
            if grids[grid]['duration'] != dur:
                print(f'Skipping {grid} with duration {grids[grid]["duration"]} for {temporal_duration_name}.')
                continue
            grid_name = grid
            print(f'\nProcessing {grids[grid]["path"]}...')
            grid_file = grids[grid]['path']
            da = rioxarray.open_rasterio(grid_file, masked=True)
            da = da / 1000
            if not merged:
                clip_gdf = gpd.read_file(clip_shp)
                clip_gdf = clip_gdf.to_crs("EPSG:4326")
                da = da.rio.set_crs("EPSG:4326", inplace=True)
                from shapely.geometry import mapping
                da = da.rio.clip(clip_gdf.geometry.apply(mapping),
                                 crs=clip_gdf.crs, drop=True, all_touched=True)

            for table_name in tables.keys():
                table_info = tables[table_name]
                print(f"temporal_dur = {table_info['dur']}, storm_dur = {grids[grid]['duration']}")
                if table_info['dur'] != grids[grid]['duration']:
                    print(f'Skipping {table_name} with duration {table_info["dur"]} for {grid_name} with duration {grids[grid]["duration"]}.\n')
                    continue
                table = {float(k): v for k, v in table_info['values'].items()}
                print(f'Applying {table_name} temporal distribution...')
                df_table = pd.DataFrame.from_dict(table, orient='index', columns=['value'])
                df_table.index.name = 'hours'
                df_table.reset_index(inplace=True)

                list_da = []
                start_time = datetime.datetime(2000, 1, 1, 0, 0, 0)

                for index, row in df_table.iterrows():
                    timestep = start_time + datetime.timedelta(hours=row['hours'])
                    da_copy = da.copy(deep=True)
                    da_copy = da_copy * (df_table['value'].iloc[index] / 100.0)
                    da_copy = da_copy.rename('PrecipCumulative')
                    da_copy = da_copy.assign_coords(time=timestep)
                    da_copy = da_copy.expand_dims(dim="time")
                    list_da.append(da_copy)

                ds = xr.concat(list_da, dim="time")
                ds = ds.to_dataset(name='PrecipCumulative')
                ds = ds.squeeze()
                ds = ds.drop_vars('band')
                ds['PrecipInc'] = ds['PrecipCumulative'].diff(dim='time', label='upper')

                ds = ds.rename({
                    'x': 'longitude',
                    'y': 'latitude'
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

                ds_td = df_table.to_xarray()
                ds_td.expand_dims(dim="time")
                ds_td["time"] = ds.time
                ds_td["value"] = ds_td["value"].swap_dims({"index": "time"})
                ds_td = ds_td.drop_vars("hours")
                ds_td = ds_td.drop_vars("index")
                ds_td = ds_td.rename({"value": "TemporalDistribution"})
                ds = xr.merge([ds, ds_td])
                ds['TemporalDistribution'].attrs['units'] = 'percent'
                ds['TemporalDistribution'].attrs['long_name'] = 'Temporal Distribution Cumulative Percentage'
                ds['TemporalDistribution'].attrs['temporalDuration'] = temporal_duration_name

                ds['time'].attrs['bounds'] = 'time_bnds'
                time_bnds = np.empty((len(ds['time']) - 1, 2), dtype='datetime64[ns]')
                time_bnds[:, 0] = ds['time'].values[:-1]
                time_bnds[:, 1] = ds['time'].values[1:]
                time_bnds = xr.DataArray(time_bnds, dims=['time', 'bnds'],
                                        coords={'time': ds['time'].values[:-1], 'bnds': [0, 1]})
                time_bnds.name = 'time_bnds'
                ds['time_bnds'] = time_bnds
                ds['time_bnds'].attrs['standard_name'] = 'time_bnds'
                ds['time_bnds'].attrs['long_name'] = 'Time Bounds'
                ds['time_bnds'].attrs['description'] = 'Start and end of each time period'

                output_file = rf"output\{project_name}\nc\Atlas14_{project_name}{grids[grid]['year_int']}yr_{grids[grid]['duration']}Storm_{temporal_duration_name}_{table_name}.nc"
                os.makedirs(os.path.dirname(output_file), exist_ok=True)
                print(f'Exporting to {output_file}')
                ds.to_netcdf(output_file)

if __name__ == "__main__":
    # get command line arguments using argpare
    import argparse
    import sys
    parser = argparse.ArgumentParser(description="Generate custom temporal distribution grids.")
    parser.add_argument('--project_name', type=str, required=True, help='Project name for output files.')
    parser.add_argument('--merged', action='store_true', help='Indicate if the grids are previously merged and therefore should not be clipped.')
    parser.add_argument('--region_name', type=str, required=True, help='Region name for the grids.')
    parser.add_argument('--years_wanted_list', type=lambda s: s.strip('[]').replace('"', '').replace("'", '').split(','), required=True, help='List of years padded for the grids Ex: ["001", "002", "005"].')
    parser.add_argument('--precip_durations_list', type=lambda s: s.strip('[]').replace('"', '').replace("'", '').split(','), required=True, help='List of precipitation durations for the grids EX: ["06h", "12h", "24h"].')
    parser.add_argument('--dist_table', type=str, required=True, help='Path to the custom distribution tables (JSON file).')
    parser.add_argument('--clip_shp', type=str, required=False, default='boundaries/Lawton_BB_StatePlaneOK.shp', help='Path to the clipping shapefile.')
    args = parser.parse_args()
    
    project_name = args.project_name
    merged = args.merged
    if not merged:
        clip_shp = args.clip_shp
        merged = False
    else:
        clip_shp = None
    regions = {
        'midwest': 'mw',
        'southeast': 'se',
        'southwest': 'sw',
        'northeast': 'ne',
        'texas': 'tx',
    }
    region = {
        'name': args.region_name,
        'abbrev': regions[args.region_name.lower()]
    }
    years_wanted_list = args.years_wanted_list
    precip_durations_list = args.precip_durations_list
    # remove any leading or trailing whitespace from the list items
    years_wanted_list = [year.strip() for year in years_wanted_list]
    precip_durations_list = [duration.strip() for duration in precip_durations_list]
    tables_path = args.dist_table

    make_custom_distro_grids(
        project_name=project_name,
        clip_shp=clip_shp,
        merged=merged,
        region=region,
        years_wanted=years_wanted_list,
        precip_durations=precip_durations_list,
        tables_path=tables_path
    )

