# Use a hybrid pythonscript to show an output netcdf file
# %%
import pandas as pd
import xarray as xr
import matplotlib.pyplot as plt
import numpy as np

# %%
# Load the netCDF file
file_path = 'output/I57_/nc/Atlas14_I57_100yr_MSE5_24hr.nc'
ds = xr.open_dataset(file_path)
ds

# %%
# Plot the cumulative precipitation
plt.figure(figsize=(10, 6))
ds['PrecipCumulative'].isel(latitude=10, longitude=10).plot()
plt.title('Cumulative Precipitation at Latitude=10, Longitude=10')
plt.xlabel('Time')
plt.ylabel('Precipitation (inches)')
plt.grid()
plt.show()

# %%
# Show the final cumulative precipitation on a folium map as a viridis raster overlay
import folium
import matplotlib.colors as mcolors
import matplotlib.cm as mcm
import io, base64, webbrowser, tempfile, os as _os

last_time_step = ds['PrecipCumulative'].isel(time=-1)
data = last_time_step.values  # shape (lat, lon)

# Normalise and apply viridis colormap (NaN -> transparent)
vmin = np.nanmin(data)
vmax = np.nanmax(data)
norm = mcolors.Normalize(vmin=vmin, vmax=vmax)
rgba = mcm.viridis(norm(np.ma.masked_invalid(data)))
rgba[np.isnan(data), 3] = 0  # transparent where nodata

# Encode to PNG in memory
buf = io.BytesIO()
plt.imsave(buf, rgba, format='png', origin='upper')
buf.seek(0)
img_b64 = base64.b64encode(buf.read()).decode('utf-8')
img_url = f'data:image/png;base64,{img_b64}'

# Bounds: [[south, west], [north, east]]
lats = ds['latitude'].values
lons = ds['longitude'].values
bounds = [[lats.min(), lons.min()], [lats.max(), lons.max()]]

m = folium.Map(location=[lats.mean(), lons.mean()], zoom_start=8, tiles='CartoDB positron')
folium.raster_layers.ImageOverlay(
    image=img_url,
    bounds=bounds,
    opacity=0.8,
    name='Cumulative Precipitation',
).add_to(m)

# Colorbar as a legend
cmap_img_buf = io.BytesIO()
fig_cb, ax_cb = plt.subplots(figsize=(4, 0.4))
fig_cb.subplots_adjust(bottom=0.5)
cb = plt.colorbar(mcm.ScalarMappable(norm=norm, cmap='viridis'), cax=ax_cb, orientation='horizontal')
cb.set_label('Cumulative Precipitation (inches)', fontsize=8)
fig_cb.savefig(cmap_img_buf, format='png', bbox_inches='tight', dpi=100)
plt.close(fig_cb)
cmap_img_buf.seek(0)
cb_b64 = base64.b64encode(cmap_img_buf.read()).decode('utf-8')
folium.Marker(
    location=[lats.min(), lons.mean()],
    icon=folium.DivIcon(html=f'<img src="data:image/png;base64,{cb_b64}" style="width:300px">')
).add_to(m)

folium.LayerControl().add_to(m)

_map_path = _os.path.join(tempfile.gettempdir(), 'qaqc_map.html')
m.save(_map_path)
webbrowser.open(f'file:///{_map_path}')

# %%
