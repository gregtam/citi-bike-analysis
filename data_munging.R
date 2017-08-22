# Load Libraries
library(SparkR)
sc <- sparkR.session(master = 'yarn',
                     sparkConfig = list(spark.driver.memory='8g',
                                        spark.executor.memory='8g'))
library(ggplot2)
library(magrittr)

# install.spark() needed for first time

citi_bike_trips_sdf <- tableToDF('citi_bike_trips')

# Create a lookup table from station name to coordinates.
lut_sdf <-
  union(select(citi_bike_trips_sdf,
               'start_station_name',
               'start_station_longitude',
               'start_station_latitude') %>% 
          distinct(),
        select(citi_bike_trips_sdf,
               'end_station_name',
               'end_station_longitude',
               'end_station_latitude') %>%
          distinct()) %>%
  distinct() %>%
  orderBy(column('start_station_name'))

#####################
# Group by stations #
#####################

start_sdf <- citi_bike_trips_sdf %>%
  groupBy('start_station_name',
          'start_station_longitude',
          'start_station_latitude') %>%
  count() %>%
  orderBy(desc(column('count')))

start_time <- Sys.time()
start_df <- collect(start_sdf)
time_start_sdf <- Sys.time() - start_time

end_sdf <- citi_bike_trips_sdf %>%
  groupBy('end_station_name',
          'end_station_longitude',
          'end_station_latitude') %>%
  count() %>%
  orderBy(desc(column('count')))

start_time <- Sys.time()
end_df <- collect(end_sdf)
time_end_sdf <- Sys.time() - start_time

# Because start and end points have roughly the same distribution, let's combine
# them together

start_station_sdf <- citi_bike_trips_sdf %>%
  select(column('starttime') %>% alias('use_time'),
         column('start_station_name') %>% alias('station_name'),
         column('start_station_latitude') %>% alias('station_latitude'),
         column('start_station_longitude') %>% alias('station_longitude'))

end_station_sdf <- citi_bike_trips_sdf %>%
  select(column('stoptime') %>% alias('use_time'),
         column('end_station_name') %>% alias('station_name'),
         column('end_station_latitude') %>% alias('station_latitude'),
         column('end_station_longitude') %>% alias('station_longitude'))

station_uses_sdf <- union(start_station_sdf, end_station_sdf)

start_time <- Sys.time()
most_common_station_df <- station_uses_sdf %>%
  groupBy('station_name',
          'station_longitude',
          'station_latitude') %>%
  count() %>%
  collect()
time_most_common_station_sdf <- Sys.time() - start_time

################
# Hourly usage #
################

# Get hourly counts
start_hour_group_df <- citi_bike_trips_sdf %>%
  select(hour(column('starttime')) %>% alias('start_hour')) %>%
  groupBy('start_hour') %>%
  count() %>%
  orderBy('start_hour') %>%
  collect()

end_hour_group_df <- citi_bike_trips_sdf %>%
  select(hour(column('stoptime')) %>% alias('stop_hour')) %>%
  groupBy('stop_hour') %>%
  count() %>%
  orderBy('stop_hour') %>%
  collect()

###################
# Plot Histograms #
###################

# Set theme of plot
hist_theme <- theme(plot.title = element_text(size = 22, hjust = 0.5),
                    axis.title.x = element_text(size = 18),
                    axis.title.y = element_text(size = 18),
                    axis.text.x = element_text(size = 12),
                    axis.text.y = element_text(size = 12),
                    legend.title = element_blank(),
                    legend.text = element_text(size = 12))

# Plot hourly usage
png(filename = 'plots/hourly_usage.png', width = 700, height = 400)
ggplot() +
  geom_bar(data = start_hour_group_df,
           aes(x = start_hour - 0.2,
               y = count,
               fill = 'Start'),
           stat = 'identity',
           width = 0.4) +
  geom_bar(data = end_hour_group_df,
           aes(x = stop_hour + 0.2,
               y = count,
               fill = 'Stop'),
           stat = 'identity',
           width = 0.4) +
  geom_vline(xintercept = 8.5, size = 1, linetype = 2) +
  geom_vline(xintercept = 17.5, size = 1, linetype = 2) +
  scale_fill_manual(values = c('royalblue', 'orangered3')) +
  hist_theme +
  labs(title = 'Hourly Usage', x = 'Hour of Day', y = 'Frequency')
dev.off()

# Look at day of the week
day_of_week_count_df <- citi_bike_trips_sdf %>%
  select(date_format(column('starttime'), 'E') %>% 
           alias('day_of_week')) %>%
  select('day_of_week') %>%
  groupBy('day_of_week') %>%
  count() %>%
  orderBy('day_of_week') %>%
  collect()

day_of_week_count_df$ordinal_number = c(6, 2, 7, 1, 5, 3, 4)

day_of_week_count_df <- day_of_week_count_df %>%
  dplyr::arrange(ordinal_number)

ggplot(day_of_week_count_df, aes(x = day_of_week, y = count)) +
  geom_bar(stat = 'identity') +
  labs(x = 'Day of Week', y = 'Frequency')
    
##################
# Group by paths #
##################
# Here, we take a look at the most common paths irrespective of direction. We
# can do this by sorting the two station names alphabetically, then 
# concatenating the two together.

# Separate station names. station_1 takes the one that comes first
# alphabetically. station_2 takes teh one that comes later alphabetically.
station_1 <- ifelse(column('start_station_name') < column('end_station_name'), 
                    column('start_station_name'), 
                    column('end_station_name'))
station_2 <- ifelse(column('start_station_name') < column('end_station_name'),
                    column('end_station_name'),
                    column('start_station_name'))
is_subscriber <- ifelse(column('usertype') == 'Subscriber', 1, 0)

# Concatenate station names into a string and count occurrences.
most_common_path_sdf <- citi_bike_trips_sdf %>%
  select(column('*'),
         concat(station_1, lit(' - '), station_2) %>% alias('station_names'),
         alias(is_subscriber, 'is_subscriber')) %>%
  groupBy('station_names') %>%
  agg(count = count(column('*')) %>% cast('int'),
      pct_subscriber = avg(column('is_subscriber'))) %>%
  orderBy(column('count') %>% desc())

most_common_path_df <- take(most_common_path_sdf, 100)

# Next, we can split the concatenated station names and count the occurrences.
path_freq_sdf <- most_common_path_sdf %>%
  selectExpr('*',
             "split(station_names, ' - ')[0]",
             "split(station_names, ' - ')[1]") %>%
  select(c('count', 
           alias(column('split(station_names,  - )[0]'), 'station_1'),
           alias(column('split(station_names,  - )[1]'), 'station_2')))

# We will use the look up table (lut_sdf) to append the coordinates to the path
# start and end points.
path_coord_freq_sdf <- path_freq_sdf %>%
  join(lut_sdf,
       column('station_1') == column('start_station_name')) %>%
  select(c('station_1', 
           'station_2',
           'count',
           alias(column('start_station_longitude'), 'lon_1'),
           alias(column('start_station_latitude'), 'lat_1'))) %>%
  join(lut_sdf, column('station_2') == column('start_station_name')) %>%
  select(c('station_1',
           'station_2',
           'count',
           'lat_1',
           'lon_1',
           alias(column('start_station_longitude'), 'lon_2'),
           alias(column('start_station_latitude'), 'lat_2'))) %>%
  orderBy(column('count') %>% desc())

path_coord_freq_df <- take(path_coord_freq_sdf, 1000)

###############################
# Most Common Path Directions #
###############################

path_dir_freq_sdf <- citi_bike_trips_sdf %>%
  groupBy('start_station_name',
          'start_station_latitude',
          'start_station_longitude',
          'end_station_name',
          'end_station_latitude',
          'end_station_longitude') %>%
  count() %>%
  orderBy(column('count') %>% desc())

path_dir_freq_df <- take(path_dir_freq_sdf, 1000)

# Split between morning and evening commutes
path_dir_freq_am_sdf <- citi_bike_trips_sdf %>%
  where(hour(column('starttime')) == 8 | hour(column('starttime')) == 9) %>%
  groupBy('start_station_name',
          'start_station_latitude',
          'start_station_longitude',
          'end_station_name',
          'end_station_latitude',
          'end_station_longitude') %>%
  count() %>%
  orderBy(column('count') %>% desc())

path_dir_freq_am_df <- take(path_dir_freq_am_sdf, 1000)

path_dir_freq_pm_sdf <- citi_bike_trips_sdf %>%
  where(hour(column('starttime')) == 17 | hour(column('starttime')) == 18) %>%
  groupBy('start_station_name',
          'start_station_latitude',
          'start_station_longitude',
          'end_station_name',
          'end_station_latitude',
          'end_station_longitude') %>%
  count() %>%
  orderBy(column('count') %>% desc())

path_dir_freq_pm_df <- take(path_dir_freq_pm_sdf, 1000)
