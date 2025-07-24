# This script will merge multiple Atlas 14 grids and clip them based on a bounding box shapefile.
# it is meant to be used where a project extent overlaps multiple Atlas 14 grids.
# the script Atlas14_Apply_Temporal_Distribution.py can then be used to apply the temporal distribution to the merged grids.
# %%
import os
import glob
import geopandas as gpd
import rasterio
from rasterio.merge import merge

# %%
#project name includes a trailing underscore to better align with the naming convention of the grids and Atlas14_Apply_Temporal_Distribution.py
# Ex: project_name = 'I57_'
project_name = 'I57_'
clip_shp = "boundaries\I57_BB_3857.shp"

# the 2 grids needing merged are midwest and southeast
merged_out_dir = f"data/merged/{project_name}"
if not os.path.exists(merged_out_dir):
    os.makedirs(merged_out_dir)

regions = {
    'region1': {
        "abbrev": 'mw',
        'name': 'Midwest'
    },
    'region2': {
        'abbrev': 'se',
        'name': 'Southeast'
    }
}


years_padded = ['001', '002', '005', '010', '025', '050', '100', '200', '500']
years_int = [int(year) for year in years_padded] 
precip_durations = ['06h', '24h']

grids = {}
for i, year in enumerate(years_padded):
    for dur in precip_durations:
            # get the ids for region1
            region1_grid_dir = f'data/{regions["region1"]["name"]}/{regions["region1"]["abbrev"]}{years_int[i]}yr{dur}a'
            region2_grid_dir = f'data/{regions["region2"]["name"]}/{regions["region2"]["abbrev"]}{years_int[i]}yr{dur}a'
            region1_grid_path = f'{region1_grid_dir}/{regions["region1"]["abbrev"]}{years_int[i]}yr{dur}a.asc'
            region2_grid_path = f'{region2_grid_dir}/{regions["region2"]["abbrev"]}{years_int[i]}yr{dur}a.asc'
            
            # check if the grid files exist
            if not os.path.exists(region1_grid_path):
                raise FileNotFoundError(f"Grid file for {regions['region1']['name']} {year} year {dur} duration not found: {region1_grid_path}")
            if not os.path.exists(region2_grid_path):
                raise FileNotFoundError(f"Grid file for {regions['region2']['name']} {year} year {dur} duration not found: {region2_grid_path}")
            
            print(f'\nMerging grids for {regions["region1"]["name"]} and {regions["region2"]["name"]} for {year} year {dur} duration...')
            merged_output_path = f'{merged_out_dir}/{project_name}{year}yr{dur}a.asc'

            # if output file already exists, delete the existing file
            if os.path.exists(merged_output_path):
                os.remove(merged_output_path)
                print(f'Removed existing merged grid: {merged_output_path}')
            
            # Clip and Merge the grids using rasterio
            with rasterio.open(region1_grid_path) as src1, rasterio.open(region2_grid_path) as src2:
                # Clip the grids to the bounding box shape file
                clip_gdf = gpd.read_file(clip_shp)

                # Print the CRS of the grids and clip_gdf
                print(f"CRS of {regions['region1']['name']} grid: {src1.crs}")
                print(f"CRS of {regions['region2']['name']} grid: {src2.crs}")
                print(f"CRS of clip_gdf: {clip_gdf.crs}") 

                
                
                # Import mask and reproject functions
                from rasterio import mask
                from rasterio.warp import calculate_default_transform, reproject, Resampling
                from rasterio.io import MemoryFile

                # Set the CRS of the grids and clip_gdf explicitly to EPSG:4326
                src1_crs = 'EPSG:4326'
                src2_crs = 'EPSG:4326'
                clip_gdf = clip_gdf.to_crs('EPSG:4326')
                

                # Merge the two grids first
                merged, merged_transform = merge([src1, src2])

                # Prepare metadata for the merged raster
                merged_meta = src1.meta.copy()
                merged_meta.update({
                    'driver': 'GTiff',
                    'height': merged.shape[1],
                    'width': merged.shape[2],
                    'count': 1,
                    'dtype': merged.dtype,
                    'crs': src1.crs,
                    'transform': merged_transform
                })

                # Write merged raster to memory
                memfile_merged = MemoryFile()
                with memfile_merged.open(**merged_meta) as merged_ds:
                    merged_ds.write(merged)

                # Clip the merged raster to the bounding box shapefile
                with memfile_merged.open() as merged_ds:
                    clipped_data, clipped_transform = mask.mask(merged_ds, clip_gdf.geometry, crop=True)
                    clipped_meta = merged_ds.meta.copy()
                    clipped_meta.update({
                        'driver': 'AAIGrid',
                        'height': clipped_data.shape[1],
                        'width': clipped_data.shape[2],
                        'count': 1,
                        'dtype': clipped_data.dtype,
                        'crs': merged_ds.crs,
                        'transform': clipped_transform
                    })
                    # Write the clipped, merged raster to disk as .asc (AAIGrid)
                    with rasterio.open(merged_output_path, 'w', **clipped_meta) as dst:
                        dst.write(clipped_data)
                print(f'Merged and clipped grid saved to: {merged_output_path}\n')
