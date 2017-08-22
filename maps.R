library(ggmap)
library(magrittr)
library(mapproj)
library(rgdal)
library(rworldmap)
library(scales)

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

start_df <- project_coordinates(start_df,
                                'start_station_latitude',
                                'start_station_longitude')
end_df <- project_coordinates(end_df,
                              'end_station_latitude',
                              'end_station_longitude')

most_common_station_df <- project_coordinates(most_common_station_df,
                                              'station_latitude',
                                              'station_longitude')

path_coord_freq_df <- path_coord_freq_df %>%
  project_coordinates('lat_1', 'lon_1') %>%
  project_coordinates('lat_2', 'lon_2')

path_dir_freq_df <- path_dir_freq_df %>% 
  project_coordinates('start_station_latitude', 'start_station_longitude') %>%
  project_coordinates('end_station_latitude', 'end_station_longitude')

path_dir_freq_am_df <- path_dir_freq_am_df %>%
  project_coordinates('start_station_latitude', 'start_station_longitude') %>%
  project_coordinates('end_station_latitude', 'end_station_longitude')

path_dir_freq_pm_df <- path_dir_freq_pm_df %>%
  project_coordinates('start_station_latitude', 'start_station_longitude') %>%
  project_coordinates('end_station_latitude', 'end_station_longitude')

nyc_map_plot <- ggplot() +
  geom_polygon(data = counties, aes(x = long, y = lat, group = group))

blank_axes <- theme(axis.text.x = element_blank(),
                    axis.text.y = element_blank(),
                    axis.ticks.x = element_blank(),
                    axis.ticks.y = element_blank())

fill_ocean <- theme(panel.background = element_rect(fill = 'dodgerblue3',
                                                    colour = 'dodgerblue3'),
                    panel.grid.major = element_blank(),
                    panel.grid.minor = element_blank(),
                    panel.border = element_blank())

title_format <- theme(plot.title = element_text(size = 26, hjust = 0.5),
                      axis.title.x = element_text(size = 22),
                      axis.title.y = element_text(size = 22),
                      legend.title = element_text(size = 18),
                      legend.text = element_text(size = 12))

plot_window <- coord_fixed(ratio = 1,
                           xlim = c(975000, 1025000),
                           ylim = c(175000, 232000))

#########################
# Plot the start points #
#########################
png(filename = 'plots/start_points.png', width = 700, height = 700)

nyc_map_plot +
  geom_point(data = start_df,
             aes(x = start_station_longitude,
                 y = start_station_latitude,
                 size = count),
             shape = 21,
             colour = 'black',
             fill = 'white',
             stroke = 0.2) +
  title_format + 
  plot_window +
  blank_axes +
  fill_ocean +
  scale_size_area(name = 'Frequency', max_size = 4, labels = comma) +
  labs(title = 'Start Points', x = 'Longitude', y = 'Latitude')

dev.off()
  
#######################
# Plot the end points #
#######################
png(filename = 'plots/end_points.png', width = 700, height = 700)

nyc_map_plot +
  geom_point(data = end_df,
             aes(x = end_station_longitude,
                 y = end_station_latitude,
                 size = count),
             shape = 21,
             colour = 'black',
             fill = 'white',
             stroke = 0.2) +
  title_format +
  plot_window +
  blank_axes +
  fill_ocean +
  scale_size_area(name = 'Frequency', max_size = 4, labels = comma) +
  labs(title = 'End Points', x = 'Longitude', y = 'Latitude')

dev.off()

#########################
# Plot the station uses #
#########################
png(filename = 'plots/station_uses.png', width = 700, height = 700)

nyc_map_plot +
  geom_point(data = most_common_station_df,
             aes(x = station_longitude,
                 y = station_latitude,
                 size = count),
             shape = 21,
             colour = 'black',
             fill = 'white',
             stroke = 0.2) +
  title_format +
  plot_window +
  blank_axes +
  fill_ocean +
  scale_size_area(name = 'Frequency', max_size = 4, labels = comma) +
  labs(title = 'Station Uses', x = 'Longitude', y = 'Latitude')

dev.off()

#################
# Plot of paths #
#################
# Here, we plot the most frequent paths overlaid on the NYC map. Each path will
# have slight transparency. The path is shown irrespective of direction.
png(filename = 'plots/citi_bike_paths.png', width = 700, height = 700)

path_plot <- nyc_map_plot +
  title_format + 
  plot_window +
  blank_axes +
  fill_ocean + 
  scale_size_area(name = 'Frequency', max_size = 6, labels = comma) + 
  labs(title = 'Bike Paths', x = 'Longitude', y = 'Latitude')

for (i in 1:100) {
  # Create a temporary data frame which includes the start and end latitude and
  # longitude coordinates for a given path.
  if (path_coord_freq_df[i, 'station_1'] == path_coord_freq_df[i, 'station_2']) {
    # If start and end point are the same, do a geom_point
    temp_df <- data.frame(latitude = path_coord_freq_df[i, 'lat_1'],
                          longitude = path_coord_freq_df[i, 'lon_1'],
                          count = path_coord_freq_df[i, 'count'])
    
    path_plot <- path_plot + geom_point(data = temp_df,
                                        aes(x = longitude,
                                            y = latitude,
                                            size = count),
                                        colour = 'white', alpha = 0.25)
  } else {
    # Otherwise, connect the two points by a line
    temp_df <- data.frame(latitude = c(path_coord_freq_df[i, 'lat_1'],
                                       path_coord_freq_df[i, 'lat_2']),
                          longitude = c(path_coord_freq_df[i, 'lon_1'],
                                        path_coord_freq_df[i, 'lon_2']),
                          count = path_coord_freq_df[i, 'count'])
    
    path_plot <- path_plot + geom_line(data = temp_df,
                                       aes(x = longitude,
                                           y = latitude,
                                           size = count),
                                       colour = 'white', alpha = 0.25)
  }
}

path_plot

dev.off()

###########################
# Plot of Path Directions #
###########################
# Here, we wish to show a similar plot as above, but instead show the path
# directionality. We do this by making each end of the path a different colour.

plot_paths <- function(df, title_str, n = 100) {
  plot_object <- nyc_map_plot +
    plot_window +
    blank_axes +
    fill_ocean + 
    scale_colour_gradient(low = 'white', high = 'red') +
    scale_size_area(name = 'Frequency', max_size = 6, labels = comma) +
    title_format +
    labs(title = title_str,
         x = 'Longitude', y = 'Latitude') +
    guides(colour = F)
  
  for (i in 1:n) {
    if (df[i, 'start_station_name'] == df[i, 'end_station_name']) {
      # If the start and end point are the same, then plot a point.
      temp_df <- data.frame(latitude = df[i, 'start_station_latitude'],
                            longitude = df[i, 'start_station_longitude'],
                            count = df[i, 'count'])
      
      # Add point to ggplot object.
      plot_object <- plot_object +
        geom_point(data = temp_df,
                   aes(x = longitude, y = latitude, size = count),
                   colour = 'red', alpha = 0.7, show.legend = F)
    } else {
      # If the start and end points are different, then draw a line between the
      # two to indicate the path
      
      # Here, we interpolate points between the start and end point. This is
      # done so that we can have a colour gradient throughout the path.
      lat_interp <- seq(df[i, 'start_station_latitude'],
                        df[i, 'end_station_latitude'],
                        length.out = 100)
      lon_interp <- seq(df[i, 'start_station_longitude'],
                        df[i, 'end_station_longitude'],
                        length.out = 100)
      
      # Create a data frame with the interpolated values. Position is simply an
      # increasing array to determine the colour.
      temp_df <- data.frame(latitude = lat_interp,
                            longitude = lon_interp,
                            position = 1:length(lat_interp),
                            count = df[i, 'count'])
      # Add line to the ggplot object.
      plot_object <- plot_object +
        geom_line(data = temp_df,
                  aes(x = longitude,
                      y = latitude,
                      colour = position,
                      size = count),
                  alpha = 0.25)
    }
  }
  plot_object
}

png(filename = 'plots/citi_bike_paths_dir.png', width = 700, height = 700)
plot_paths(path_dir_freq_df,
           title_str = 'Bike Path Directions (Start: White, End: Red)')
dev.off()

png(filename = 'plots/citi_bike_paths_dir_am.png', width = 700, height = 700)
plot_paths(path_dir_freq_am_df,
           title_str = 'Bike Path Directions - AM (Start: White, End: Red)',
           n = 150)
dev.off()

png(filename = 'plots/citi_bike_paths_dir_pm.png', width = 700, height = 700)
plot_paths(path_dir_freq_pm_df,
           title_str = 'Bike Path Directions - PM (Start: White, End: Red)',
           n = 150)
dev.off()
