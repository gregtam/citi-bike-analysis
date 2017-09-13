# Load Libraries
library(magrittr)
library(SparkR)

sc <- sparkR.session(master = 'yarn',
                     sparkConfig = list(spark.driver.memory='8g',
                                        spark.executor.memory='8g'))

# Define DataFrame based off table in Hive
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

start_df <- collect(start_sdf)

end_sdf <- citi_bike_trips_sdf %>%
  groupBy('end_station_name',
          'end_station_longitude',
          'end_station_latitude') %>%
  count() %>%
  orderBy(desc(column('count')))

end_df <- collect(end_sdf)

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

most_common_station_df <- station_uses_sdf %>%
  groupBy('station_name',
          'station_longitude',
          'station_latitude') %>%
  count() %>%
  collect()

###############################
# Separate by Weekday/Weekend #
###############################

# Trips that are less than 24 hours and start and end on a weekday
weekday_bike_trips_sdf <- citi_bike_trips_sdf %>%
  withColumn('start_dayofweek', date_format(column('starttime'), 'E')) %>%
  withColumn('stop_dayofweek', date_format(column('stoptime'), 'E')) %>%
  withColumn('starttime_unix', unix_timestamp(column('starttime'))) %>%
  withColumn('stoptime_unix', unix_timestamp(column('stoptime'))) %>%
  withColumn('trip_length_sec',
             (column('stoptime_unix') - column('starttime_unix'))) %>%
  where(column('trip_length_sec') <= 60*60*24) %>%
  where(column('start_dayofweek') != 'Sat') %>%
  where(column('start_dayofweek') != 'Sun') %>%
  where(column('stop_dayofweek') != 'Sat') %>%
  where(column('stop_dayofweek') != 'Sun')

# Trips that are less than 24 hours and start and end on a Saturday or Sunday
weekend_bike_trips_sdf <- citi_bike_trips_sdf %>%
  withColumn('start_dayofweek', date_format(column('starttime'), 'E')) %>%
  withColumn('stop_dayofweek', date_format(column('stoptime'), 'E')) %>%
  withColumn('starttime_unix', unix_timestamp(column('starttime'))) %>%
  withColumn('stoptime_unix', unix_timestamp(column('stoptime'))) %>%
  withColumn('trip_length_sec',
             (column('stoptime_unix') - column('starttime_unix'))) %>%
  where(column('trip_length_sec') <= 60*60*24) %>%
  where((column('start_dayofweek') == 'Sat')
        | (column('start_dayofweek') == 'Sun')) %>%
  where((column('stop_dayofweek') == 'Sat')
        | (column('stop_dayofweek') == 'Sun'))

################
# Hourly Usage #
################

# Get hourly counts for weekday trips
start_hour_weekday_group_df <- weekday_bike_trips_sdf %>%
  select(hour(column('starttime')) %>% alias('start_hour')) %>%
  groupBy('start_hour') %>%
  count() %>%
  orderBy('start_hour') %>%
  collect()

end_hour_weekday_group_df <- weekday_bike_trips_sdf %>%
  select(hour(column('stoptime')) %>% alias('stop_hour')) %>%
  groupBy('stop_hour') %>%
  count() %>%
  orderBy('stop_hour') %>%
  collect()

# Get hourly counts for weekend trips
start_hour_weekend_group_df <- weekend_bike_trips_sdf %>%
  select(hour(column('starttime')) %>% alias('start_hour')) %>%
  groupBy('start_hour') %>%
  count() %>%
  orderBy('start_hour') %>%
  collect()

end_hour_weekend_group_df <- weekend_bike_trips_sdf %>%
  select(hour(column('stoptime')) %>% alias('stop_hour')) %>%
  groupBy('stop_hour') %>%
  count() %>%
  orderBy('stop_hour') %>%
  collect()
    
#####################
# Most Common Paths #
#####################

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
  select(column('count'),
         alias(column('split(station_names,  - )[0]'), 'station_1'),
         alias(column('split(station_names,  - )[1]'), 'station_2'))

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

#####################################
# Most Common Path (With Direction) #
#####################################

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

# Split between morning and evening commutes on weekdays
  
path_dir_freq_wkdy_am_sdf <- weekday_bike_trips_sdf %>%
  where(hour(column('starttime')) == 8 | hour(column('starttime')) == 9) %>%
  groupBy('start_station_name',
          'start_station_latitude',
          'start_station_longitude',
          'end_station_name',
          'end_station_latitude',
          'end_station_longitude') %>%
  count() %>%
  orderBy(column('count') %>% desc())

path_dir_freq_wkdy_am_df <- take(path_dir_freq_wkdy_am_sdf, 1000)

path_dir_freq_wkdy_pm_sdf <- weekday_bike_trips_sdf %>%
  where(hour(column('starttime')) == 17 | hour(column('starttime')) == 18) %>%
  groupBy('start_station_name',
          'start_station_latitude',
          'start_station_longitude',
          'end_station_name',
          'end_station_latitude',
          'end_station_longitude') %>%
  count() %>%
  orderBy(column('count') %>% desc())

path_dir_freq_wkdy_pm_df <- take(path_dir_freq_wkdy_pm_sdf, 1000)

# Weekend trips
path_dir_freq_wknd_sdf <- weekend_bike_trips_sdf %>%
  groupBy('start_station_name',
          'start_station_latitude',
          'start_station_longitude',
          'end_station_name',
          'end_station_latitude',
          'end_station_longitude') %>%
  count() %>%
  orderBy(column('count') %>% desc())

path_dir_freq_wknd_df <- take(path_dir_freq_wknd_sdf, 1000)