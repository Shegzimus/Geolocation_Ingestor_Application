import osmnx as ox
import folium
from shapely.geometry import mapping

# Geocode Dublin to get the boundary polygon
gdf = ox.geocode_to_gdf("Dublin, Ireland")
polygon = gdf.geometry.iloc[0]  # This is a shapely Polygon

# Get the center of the polygon to center the map
center = [gdf.geometry.centroid.y.values[0], gdf.geometry.centroid.x.values[0]]

# Create the map
m = folium.Map(location=center, zoom_start=12, tiles="CartoDB positron")

# Add the polygon to the map with styling
folium.GeoJson(
    data=mapping(polygon),
    style_function=lambda x: {
        'fillColor': '#3388ff',
        'color': '#3388ff',
        'weight': 2,
        'fillOpacity': 0.2
    }
).add_to(m)

# Add a marker at the center
folium.Marker(
    location=center,
    popup="Dublin, Ireland",
    icon=folium.Icon(color="red", icon="info-sign")
).add_to(m)

# Display the map
m.save("dublin_map.html")  # Save to HTML file to view
m  # This will display the map in a Jupyter notebook