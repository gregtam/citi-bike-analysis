# Load Libraries
library(ggplot2)
library(magrittr)
library(mapproj)
library(rgdal)
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

  input_df <- spTransform(input_df, CRS(proj4string(counties))) %>%
    data.frame()

  return(input_df)
}

start_proj_df <- project_coordinates(start_df,
                                     'start_station_latitude',
                                     'start_station_longitude')
end_proj_df <- project_coordinates(end_df,
                                   'end_station_latitude',
                                   'end_station_longitude')

most_common_station_proj_df <- project_coordinates(most_common_station_df,
                                                   'station_latitude',
                                                   'station_longitude')

path_coord_freq_proj_df <- path_coord_freq_df %>%
  project_coordinates('lat_1', 'lon_1') %>%
  project_coordinates('lat_2', 'lon_2')

path_dir_freq_proj_df <- path_dir_freq_df %>%
  project_coordinates('start_station_latitude', 'start_station_longitude') %>%
  project_coordinates('end_station_latitude', 'end_station_longitude')

path_dir_freq_wkdy_am_proj_df <- path_dir_freq_wkdy_am_df %>%
  project_coordinates('start_station_latitude', 'start_station_longitude') %>%
  project_coordinates('end_station_latitude', 'end_station_longitude')

path_dir_freq_wkdy_pm_proj_df <- path_dir_freq_wkdy_pm_df %>%
  project_coordinates('start_station_latitude', 'start_station_longitude') %>%
  project_coordinates('end_station_latitude', 'end_station_longitude')

path_dir_freq_wknd_proj_df <- path_dir_freq_wknd_df %>%
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

title_format <- theme(plot.title = element_text(size = 22, hjust = 0.5),
                      axis.title.x = element_text(size = 20),
                      axis.title.y = element_text(size = 20),
                      legend.title = element_text(size = 18),
                      legend.text = element_text(size = 12))

remove_axis_labels <- theme(axis.title.x = element_blank(),
                            axis.title.y = element_blank())

plot_window <- coord_fixed(ratio = 1,
                           xlim = c(975000, 1025000),
                           ylim = c(175000, 232000))

#########################
# Plot the start points #
#########################
png(filename = 'plots/start_points.png', width = 700, height = 700)

nyc_map_plot +
  geom_point(data = start_proj_df,
             aes(x = start_station_longitude,
                 y = start_station_latitude,
                 size = count),
             shape = 21,
             colour = 'black',
             fill = 'white',
             stroke = 0.2) +
  title_format +
  remove_axis_labels +
  plot_window +
  blank_axes +
  fill_ocean +
  scale_size_area(name = 'Frequency', max_size = 4, labels = comma) +
  labs(title = 'Start Points')

dev.off()

#######################
# Plot the end points #
#######################
png(filename = 'plots/end_points.png', width = 700, height = 700)

nyc_map_plot +
  geom_point(data = end_proj_df,
             aes(x = end_station_longitude,
                 y = end_station_latitude,
                 size = count),
             shape = 21,
             colour = 'black',
             fill = 'white',
             stroke = 0.2) +
  title_format +
  remove_axis_labels +
  plot_window +
  blank_axes +
  fill_ocean +
  scale_size_area(name = 'Frequency', max_size = 4, labels = comma) +
  labs(title = 'End Points')

dev.off()

#########################
# Plot the station uses #
#########################
png(filename = 'plots/station_uses.png', width = 700, height = 700)

nyc_map_plot +
  geom_point(data = most_common_station_proj_df,
             aes(x = station_longitude,
                 y = station_latitude,
                 size = count),
             shape = 21,
             colour = 'black',
             fill = 'white',
             stroke = 0.2) +
  title_format +
  remove_axis_labels +
  plot_window +
  blank_axes +
  fill_ocean +
  scale_size_area(name = 'Frequency', max_size = 4, labels = comma) +
  labs(title = 'Station Uses')

dev.off()

#################
# Plot of paths #
#################
# Here, we plot the most frequent paths overlaid on the NYC map. Each path will
# have slight transparency. The path is shown irrespective of direction.
png(filename = 'plots/citi_bike_paths.png', width = 700, height = 700)

path_plot <- nyc_map_plot +
  title_format +
  remove_axis_labels +
  plot_window +
  blank_axes +
  fill_ocean +
  scale_size_area(name = 'Frequency', max_size = 6, labels = comma) +
  labs(title = 'Bike Paths')

for (i in 1:100) {
  # Create a temporary data frame which includes the start and end latitude and
  # longitude coordinates for a given path.
  if (path_coord_freq_proj_df[i, 'station_1'] == path_coord_freq_proj_df[i, 'station_2']) {
    # If start and end point are the same, do a geom_point
    temp_df <- data.frame(latitude = path_coord_freq_proj_df[i, 'lat_1'],
                          longitude = path_coord_freq_proj_df[i, 'lon_1'],
                          count = path_coord_freq_proj_df[i, 'count'])

    path_plot <- path_plot + geom_point(data = temp_df,
                                        aes(x = longitude,
                                            y = latitude,
                                            size = count),
                                        colour = 'white', alpha = 0.25)
  } else {
    # Otherwise, connect the two points by a line
    temp_df <- data.frame(latitude = c(path_coord_freq_proj_df[i, 'lat_1'],
                                       path_coord_freq_proj_df[i, 'lat_2']),
                          longitude = c(path_coord_freq_proj_df[i, 'lon_1'],
                                        path_coord_freq_proj_df[i, 'lon_2']),
                          count = path_coord_freq_proj_df[i, 'count'])

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

plot_paths <- function(df, title_str, n = 100, max_limit = NA, max_size = 6) {
  # Plots Citi Bike paths on the New York City map.
  #
  # Inputs:
  #   df: a DataFrame of the paths. It should have projected coordinate
  #       and counts
  #   title_str: A string representing the title of the plot
  #   n: The n most frequent paths will be plotted. (Default: 100)
  #   max_limit: The maximum scale size. If NA, then limits will not be
  #              specified (Default: NA)
  #   max_size: The maximum size of the plotted lines. (Default: 6)

  plot_object <- nyc_map_plot +
    title_format +
    remove_axis_labels +
    plot_window +
    blank_axes +
    fill_ocean +
    scale_colour_gradient(low = 'white', high = 'red') +
    labs(title = title_str) +
    guides(colour = F)

  # If max limit is specified, then set limits
  if (is.na(max_limit)) {
    plot_object <- plot_object +
      scale_size_area(name = 'Frequency', max_size = max_size, labels = comma)
  } else {
    plot_object <- plot_object +
      scale_size_area(name = 'Frequency', max_size = max_size, labels = comma,
                      limits = c(1, max_limit))
  }

  # For each distinct trip
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
plot_paths(path_dir_freq_proj_df,
           title_str = 'Bike Path Directions (Start: White, End: Red)',
           n = 150)
dev.off()

# We want AM, PM, and weekend plots to be on the same scale. We will get the max
# count from all three tables.
# Get the max count of AM, PM, and weekend paths.
max_count <- max(path_dir_freq_wkdy_am_proj_df$count,
                 path_dir_freq_wkdy_pm_proj_df$count,
                 path_dir_freq_wknd_proj_df$count)
# Round up to the nearest multiple of 500.
max_limit <- ceiling(max_count/500) * 500

png(filename = 'plots/citi_bike_paths_dir_wkdy_am.png', width = 700, height = 700)
plot_paths(path_dir_freq_wkdy_am_proj_df,
           title_str = 'Bike Paths - Weekday AM (Start: White, End: Red)',
           max_limit = max_limit,
           n = 150)
dev.off()

png(filename = 'plots/citi_bike_paths_dir_wkdy_pm.png', width = 700, height = 700)
plot_paths(path_dir_freq_wkdy_pm_proj_df,
           title_str = 'Bike Paths - Weekday PM (Start: White, End: Red)',
           max_limit = max_limit,
           n = 150)
dev.off()

png(filename = 'plots/citi_bike_paths_dir_wknd.png', width = 700, height = 700)
plot_paths(path_dir_freq_wknd_proj_df,
           title_str = 'Bike Paths - Weekend (Start: White, End: Red)',
           max_limit = max_limit,
           n = 150)
dev.off()