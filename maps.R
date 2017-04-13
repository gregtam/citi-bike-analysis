library(ggmap)
library(mapproj)
library(rgdal)
library(rworldmap)

# Maps data taken from http://www1.nyc.gov/site/planning/data-maps/open-data/districts-download-metadata.page
counties <- readOGR('nybb_16d/nybb.shp', layer='nybb')

# Plot NYC
ggplot() + geom_polygon(data=counties, aes(x=long, y=lat, group=group))

project_coordinates <- function(input_df, lat, long)
{
  # Projects coordinates into the same space as the NYC map
  form <- paste0("~", long, " + ", lat)
  coordinates(input_df) <- as.formula(form)
  proj4string(input_df) <- CRS("+proj=longlat +datum=NAD83")

  input_df <- spTransform(input_df, CRS(proj4string(counties))) %>% data.frame()
  
  return(input_df)
}

start_df <- project_coordinates(start_df, 'start_station_latitude', 'start_station_longitude')
end_df <- project_coordinates(end_df, 'end_station_latitude', 'end_station_longitude')

path_coord_freq_df <- project_coordinates(path_coord_freq_df, 'lat_1', 'lon_1') %>%
  project_coordinates('lat_2', 'lon_2')

path_dir_freq_df <- project_coordinates(path_dir_freq_df, 'start_station_latitude', 'start_station_longitude') %>%
  project_coordinates('end_station_latitude', 'end_station_longitude')


nyc_map_plot <- ggplot() +
  geom_polygon(data = counties, aes(x = long, y = lat, group = group))

blank_axes <- theme(axis.text.x = element_blank(),
                    axis.text.y = element_blank(),
                    axis.ticks.x = element_blank(),
                    axis.ticks.y = element_blank()
                   )

fill_ocean <- theme(panel.background = element_rect(fill = 'dodgerblue3', colour = 'dodgerblue3'),
                    panel.grid.major = element_blank(),
                    panel.grid.minor = element_blank(),
                    panel.border = element_blank()
                   )

title_format <- theme(plot.title = element_text(size = 26, hjust = 0.5),
                      axis.title.x = element_text(size = 22),
                      axis.title.y = element_text(size = 22),
                      legend.title = element_text(size = 18),
                      legend.text = element_text(size = 12)
                     )


plot_window <- coord_fixed(ratio = 1, xlim = c(975000, 1025000), ylim = c(175000, 232000))

#########################
# Plot the start points #
#########################
nyc_map_plot +
  geom_point(data = start_df, shape = 21, colour = 'black', fill = 'white', stroke = 0.2,
             aes(x = start_station_longitude, y = start_station_latitude, size = count)
            ) +
  plot_window +
  blank_axes +
  fill_ocean +
  scale_size_area(name = 'Frequency', max_size = 5) +
  xlab('Longitude') +
  ylab('Latitude')
  # coord_fixed(ratio = 1, xlim = c(975000, 1025000), ylim = c(150000, 250000))
  
#######################
# Plot the end points #
#######################
nyc_map_plot +
  geom_point(data = end_df, shape = 21, colour = 'black', fill = 'white', stroke = 0.2,
             aes(x = end_station_longitude, y = end_station_latitude, size = count)
            ) +
  plot_window +
  blank_axes +
  fill_ocean +
  scale_size_area(name = 'Frequency', max_size = 5) +
  xlab('Longitude') +
  ylab('Latitude')
  # coord_fixed(ratio = 1, xlim = c(975000, 1025000), ylim = c(150000, 250000))

#################
# Plot of paths #
#################
# Here, we plot the most frequent paths overlaid on the NYC map.
# Each path will have slight transparency. The path is shown
# irrespective of direction.

path_plot <- nyc_map_plot +
  plot_window +
  blank_axes +
  fill_ocean + 
  labs(title = 'Citi Bike Paths', x = 'Longitude', y = 'Latitude') + 
  title_format

for (i in 1:100) {
  temp_df <- data.frame(latitude = c(path_coord_freq_df[i, 'lat_1'], path_coord_freq_df[i, 'lat_2']),
                        longitude = c(path_coord_freq_df[i, 'lon_1'], path_coord_freq_df[i, 'lon_2']),
                        count = path_coord_freq_df[i, 'count'])
  
  # path_plot <- path_plot + geom_line(data = temp_df, aes(x = longitude, y = latitude, alpha = count), size = 3, colour = 'white')
  path_plot <- path_plot + geom_line(data = temp_df, aes(x = longitude, y = latitude, size = count), alpha = 0.25, colour = 'white')
}

path_plot

###########################
# Plot of Path Directions #
###########################
# Here, we wish to show a similar plot as above, but instead show
# the path directionality. We do this by making each end of the path
# a different colour.

path_dir_plot <- nyc_map_plot +
  plot_window +
  blank_axes +
  fill_ocean + 
  labs(title = 'Citi Bike Paths', x = 'Longitude', y = 'Latitude') + 
  title_format +
  scale_colour_gradient(low = 'white', high = 'red') +
  theme(legend.position = 'none')

for (i in 1:100) {
  if (path_dir_freq_df[i, 'start_station_name'] == path_dir_freq_df[i, 'end_station_name']) {
    temp_df <- data.frame(latitude = path_dir_freq_df[i, 'start_station_latitude'],
                          longitude = path_dir_freq_df[i, 'start_station_longitude'])
    
    path_dir_plot <- path_dir_plot +
      geom_point(data = temp_df, aes(x = longitude, y = latitude), size = 4, colour = 'red', alpha = 0.7)
    
  } else {
    lat_interp <- seq(path_dir_freq_df[i, 'start_station_latitude'], path_dir_freq_df[i, 'end_station_latitude'], length.out = 100)
    lon_interp <- seq(path_dir_freq_df[i, 'start_station_longitude'], path_dir_freq_df[i, 'end_station_longitude'], length.out = 100)
    
    temp_df <- data.frame(latitude = lat_interp,
                          longitude = lon_interp,
                          position = 1:length(lat_interp),
                          count = path_dir_freq_df[i, 'count'])
      
    path_dir_plot <- path_dir_plot +
      geom_line(data = temp_df, aes(x = longitude, y = latitude, colour = position, size = count), alpha = 0.3)
  }
}

path_dir_plot
