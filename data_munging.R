# Load Libraries
library(ggplot2)
library(magrittr)

# Connect to spark
if (nchar(Sys.getenv("SPARK_HOME")) < 1) {
  Sys.setenv(SPARK_HOME = paste0(Sys.getenv("HOME"),
                                 "/spark-2.1.1-bin-hadoop2.7"))
}
library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
sc <- sparkR.session(master = 'local[4]',
                     sparkConfig = list(spark.driver.memory='8g',
                                        spark.executor.memory='8g'))
# install.spark() needed for first time

# Select only relevant columns
df_cols <- c('starttime', 'start.station.name', 'start.station.latitude',
             'start.station.longitude', 'stoptime', 'end.station.name',
             'end.station.latitude', 'end.station.longitude', 'usertype',
             'birth.year')
july_citi_sdf <- as.DataFrame(read.csv('citibike/201607-citibike-tripdata.csv')[, df_cols])
jan_citi_sdf <- as.DataFrame(read.csv('citibike/201601-citibike-tripdata.csv')[, df_cols])

############################
# Extract date information #
############################

# Since timestamps are represented as strings in this data and UDFs are not very
# feasible in SparkR, we extracted each piece of the date (e.g., year, month,
# day, etc.)

july_citi_date_sdf <- july_citi_sdf %>%
  selectExpr('*',
             "split(starttime, ' ')[0] AS start_date",
             "split(starttime, ' ')[1] AS start_timeofday",
             "split(stoptime, ' ')[0] AS stop_date",
             "split(stoptime, ' ')[1] AS stop_timeofday") %>%
  selectExpr('*',
             "split(start_date, '/')[0] AS start_month",
             "split(start_date, '/')[1] AS start_day",
             "split(start_date, '/')[2] AS start_year",
             "split(start_timeofday, ':')[0] AS start_hour",
             "split(start_timeofday, ':')[1] AS start_minute",
             "split(start_timeofday, ':')[2] AS start_second",
             "split(stop_date, '/')[0] AS stop_month",
             "split(stop_date, '/')[1] AS stop_day",
             "split(stop_date, '/')[2] AS stop_year",
             "split(stop_timeofday, ':')[0] AS stop_hour",
             "split(stop_timeofday, ':')[1] AS stop_minute",
             "split(stop_timeofday, ':')[2] AS stop_second") %>%
  select(column('start_station_name'),
         column('start_station_latitude'),
         column('start_station_longitude'),
         column('end_station_name'),
         column('end_station_latitude'),
         column('end_station_longitude'),
         cast(column('start_month'), 'integer'),
         cast(column('start_day'), 'integer'),
         cast(column('start_year'), 'integer'),
         cast(column('start_hour'), 'integer'),
         cast(column('start_minute'), 'integer'),
         cast(column('start_second'), 'integer'),
         cast(column('stop_month'), 'integer'),
         cast(column('stop_day'), 'integer'),
         cast(column('stop_year'), 'integer'),
         cast(column('stop_hour'), 'integer'),
         cast(column('stop_minute'), 'integer'),
         cast(column('stop_second'), 'integer'),
         column('usertype'),
         column('birth_year')) %>%
  select(column('start_station_name'), column('start_station_latitude'),
         column('start_station_longitude'), column('end_station_name'),
         column('end_station_latitude'), column('end_station_longitude'),
         concat(column('start_year'), lit('-'),
                column('start_month'), lit('-'),
                column('start_day'), lit(' '),
                column('start_hour'), lit(':'),
                column('start_minute'), lit(':'),
                column('start_second')) %>%
           alias('start_time') %>%
           cast('timestamp'),
         concat(column('stop_year'), lit('-'),
                column('stop_month'), lit('-'),
                column('stop_day'), lit(' '),
                column('stop_hour'), lit(':'),
                column('stop_minute'), lit(':'),
                column('stop_second')) %>%
           alias('stop_time') %>%
           cast('timestamp'),
         column('usertype'),
         column('birth_year'))

# Write to disk the first time, then load it every other time
# write.df(july_citi_date_sdf, 'july_citi_date', mode = 'overwrite')
july_citi_date_sdf <- read.df('july_citi_date')

# Create a lookup table from station name to coordinates.
lut_sdf <-
  union(select(july_citi_date_sdf,
               'start_station_name',
               'start_station_longitude',
               'start_station_latitude') %>% 
          distinct(),
        select(july_citi_date_sdf,
               'end_station_name',
               'end_station_longitude',
               'end_station_latitude') %>%
          distinct()) %>%
  distinct() %>%
  orderBy(column('start_station_name'))

#####################
# Group by stations #
#####################

start_sdf <- july_citi_sdf %>%
  groupBy('start_station_name',
          'start_station_longitude',
          'start_station_latitude') %>%
  count() %>%
  orderBy(desc(column('count')))

start_time <- Sys.time()
start_df <- collect(start_sdf)
time_start_sdf <- Sys.time() - start_time

end_sdf <- july_citi_sdf %>%
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

start_station_sdf <- july_citi_sdf %>%
  select(column('starttime') %>% alias('use_time'),
         column('start_station_name') %>% alias('station_name'),
         column('start_station_latitude') %>% alias('station_latitude'),
         column('start_station_longitude') %>% alias('station_longitude'))

end_station_sdf <- july_citi_sdf %>%
  select(column('stoptime') %>% alias('use_time'),
         column('end_station_name') %>% alias('station_name'),
         column('end_station_latitude') %>% alias('station_latitude'),
         column('end_station_longitude') %>% alias('station_longitude'))

all_station_sdf <- union(start_station_sdf, end_station_sdf)

start_time <- Sys.time()
station_use_df <- all_station_sdf %>%
  groupBy('station_name',
          'station_longitude',
          'station_latitude') %>%
  count() %>%
  collect()
time_station_use_sdf <- Sys.time() - start_time

# Include hourly data
start_hour_group_df <- july_citi_date_sdf %>%
  select(hour(column('start_time')) %>% alias('start_hour')) %>%
  groupBy('start_hour') %>%
  count() %>%
  orderBy('start_hour') %>%
  collect()

end_hour_group_df <- july_citi_date_sdf %>%
  select(hour(column('stop_time')) %>% alias('stop_hour')) %>%
  groupBy('stop_hour') %>%
  count() %>%
  orderBy('stop_hour') %>%
  collect()

hist_theme <- theme(plot.title = element_text(size = 22, hjust = 0.5),
                    axis.title.x = element_text(size = 18),
                    axis.title.y = element_text(size = 18),
                    axis.text.x = element_text(size = 12),
                    axis.text.y = element_text(size = 12),
                    legend.title = element_blank(),
                    legend.text = element_text(size = 12))

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
  scale_fill_manual(values = c('royalblue', 'orangered3')) +
  hist_theme +
  labs(x = 'Hour of Day', y = 'Frequency')

# Look at day of the week
day_of_week_count_df <- july_citi_date_sdf %>%
  select(date_format(column('start_time'), 'E') %>% 
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
# alphabetically.
station_1 <- ifelse(column('start_station_name') < column('end_station_name'), 
                    column('start_station_name'), 
                    column('end_station_name'))
station_2 <- ifelse(column('start_station_name') < column('end_station_name'),
                    column('end_station_name'),
                    column('start_station_name'))
is_subscriber <- ifelse(column('usertype') == 'Subscriber', 1, 0)

# Concatenate station names into a string and count occurrences.
most_common_path_sdf <- july_citi_date_sdf %>%
  select(column('*'),
         concat(station_1, lit(' - '), station_2) %>% alias('station_names'),
         alias(is_subscriber, 'is_subscriber')) %>%
  groupBy('station_names') %>%
  agg(count = count(column('*')) %>% cast('int'),
      pct_subscriber = avg(column('is_subscriber'))) %>%
  orderBy(column('count') %>% desc())

most_common_path_df <- take(most_common_path_sdf, 50)

# Next, we can split the concatenated station names and count the occurrences.
path_freq_sdf <-
  selectExpr(most_common_path_sdf,
             '*',
             "split(station_names, ' - ')[0]",
             "split(station_names, ' - ')[1]") %>%
  select(c('count', 
           alias(column('split(station_names,  - )[0]'), 'station_1'),
           alias(column('split(station_names,  - )[1]'), 'station_2')))

path_coord_freq_sdf <-
  join(path_freq_sdf,
       lut_sdf,
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

path_coord_freq_df <- take(path_coord_freq_sdf, 300)

###############################
# Most Common Path Directions #
###############################

path_dir_freq_sdf <- july_citi_date_sdf %>%
  groupBy('start_station_name', 'start_station_latitude',
          'start_station_longitude', 'end_station_name', 'end_station_latitude',
          'end_station_longitude') %>%
  count() %>%
  orderBy(column('count') %>% desc())

path_dir_freq_df <- take(path_dir_freq_sdf, 300)

# Split between morning and evening commutes
path_dir_freq_am_sdf <- july_citi_date_sdf %>%
  where(hour(column('start_time')) == 8 | hour(column('start_time')) == 9) %>%
  groupBy('start_station_name', 'start_station_latitude',
          'start_station_longitude', 'end_station_name', 'end_station_latitude',
          'end_station_longitude') %>%
  count() %>%
  orderBy(column('count') %>% desc())

path_dir_freq_am_df <- take(path_dir_freq_am_sdf, 300)

path_dir_freq_pm_sdf <- july_citi_date_sdf %>%
  where(hour(column('start_time')) == 17 | hour(column('start_time')) == 18) %>%
  groupBy('start_station_name', 'start_station_latitude',
          'start_station_longitude', 'end_station_name', 'end_station_latitude',
          'end_station_longitude') %>%
  count() %>%
  orderBy(column('count') %>% desc())

path_dir_freq_pm_df <- take(path_dir_freq_pm_sdf, 300)
