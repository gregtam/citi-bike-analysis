# Load Libraries
library(magrittr)
library(SparkR)

sc <- sparkR.session(master = 'yarn',
                     sparkConfig = list(spark.driver.memory='8g',
                                        spark.executor.memory='8g'))

table_name_list <- c('2013-07 - Citi Bike trip data.csv',
                     '2013-08 - Citi Bike trip data.csv',
                     '2013-09 - Citi Bike trip data.csv',
                     '2013-10 - Citi Bike trip data.csv',
                     '2013-11 - Citi Bike trip data.csv',
                     '2013-12 - Citi Bike trip data.csv', 
                     '2014-01 - Citi Bike trip data.csv',
                     '2014-02 - Citi Bike trip data.csv',
                     '2014-03 - Citi Bike trip data.csv',
                     '2014-04 - Citi Bike trip data.csv',
                     '2014-05 - Citi Bike trip data.csv',
                     '2014-06 - Citi Bike trip data.csv',
                     '2014-07 - Citi Bike trip data.csv',
                     '2014-08 - Citi Bike trip data.csv',
                     '201409-citibike-tripdata.csv', 
                     '201410-citibike-tripdata.csv',
                     '201411-citibike-tripdata.csv',
                     '201412-citibike-tripdata.csv',
                     '201501-citibike-tripdata.csv',
                     '201502-citibike-tripdata.csv',
                     '201503-citibike-tripdata.csv',
                     '201504-citibike-tripdata.csv',
                     '201505-citibike-tripdata.csv',
                     '201506-citibike-tripdata.csv',
                     '201507-citibike-tripdata.csv',
                     '201508-citibike-tripdata.csv',
                     '201509-citibike-tripdata.csv',
                     '201510-citibike-tripdata.csv',
                     '201511-citibike-tripdata.csv',
                     '201512-citibike-tripdata.csv',
                     '201601-citibike-tripdata.csv',
                     '201602-citibike-tripdata.csv',
                     '201603-citibike-tripdata.csv',
                     '201604-citibike-tripdata.csv',
                     '201605-citibike-tripdata.csv',
                     '201606-citibike-tripdata.csv',
                     '201607-citibike-tripdata.csv',
                     '201608-citibike-tripdata.csv',
                     '201609-citibike-tripdata.csv',
                     '201610-citibike-tripdata.csv',
                     '201611-citibike-tripdata.csv',
                     '201612-citibike-tripdata.csv',
                     '201701-citibike-tripdata.csv',
                     '201702-citibike-tripdata.csv',
                     '201703-citibike-tripdata.csv',
                     '201704-citibike-tripdata.csv',
                     '201705-citibike-tripdata.csv',
                     '201706-citibike-tripdata.csv',
                     '201707-citibike-tripdata.csv')

#####################
# Load Data from S3 #
#####################
# Here, we load our data from S3 into Spark as Spark DataFrames. The timestamps
# associated with the csv files are not all in the same format. We must account
# for this so that we can union all of the tables together into one master table
# and have the proper timestamps.

# A list of the Citi Bike Spark DataFrames from S3
source_sdf_list <- list()
# A list of the Citi Bike DataFrames with timestamps properly formatted
timestamp_edit_sdf_list <- list()
for (i in 1:length(table_name_list)) {
  # Load DataFrame from CSV file in S3
  source_sdf_list[[i]] <- read.df(paste0('s3://gt-citi-bike/',
                                         table_name_list[i]),
                                  source = 'csv',
                                  header = 'true')
  
  if (i %in% 1:14) {
    # Date is in yyyy-MM-dd HH:mm:ss format
    date_format_str <- 'yyyy-MM-dd HH:mm:ss'
    
  } else if (i %in% c(15:18, 22:23, 25:length(table_name_list))) {
    # Date is in MM/dd/yyyy HH:mm:ss format
    date_format_str <- 'MM/dd/yyyy HH:mm:ss'
    
  } else if (i %in% c(19:21, 24)){
    # Date is in MM/dd/yyyy HH:mm format
    date_format_str <- 'MM/dd/yyyy HH:mm'
  } else {
    print('This statement should not be reached.')
  }
  
  # Replace column names that have spaces with underscores and set to lower case
  timestamp_edit_sdf_list[[i]] <- source_sdf_list[[i]] %>%
    select(lapply(columns(source_sdf_list[[i]]),
                  function(x) column(x) %>% alias(tolower(gsub(' ', '_', x)))))
  
  
  if (i < 40 | (i >= 46 & i <= 49)) {
    timestamp_edit_sdf_list[[i]] <- timestamp_edit_sdf_list[[i]] %>%
      withColumn('starttime_unix',
                 unix_timestamp(column('starttime'), date_format_str)) %>%
      withColumn('stoptime_unix',
                 unix_timestamp(column('stoptime'), date_format_str)) %>%
      withColumn('starttime_correct',
                 from_unixtime(column('starttime_unix'),
                               'yyyy-MM-dd HH:mm:ss') %>%
                   cast('timestamp')) %>%
      withColumn('stoptime_correct',
                 from_unixtime(column('stoptime_unix'),
                               'yyyy-MM-dd HH:mm:ss') %>%
                   cast('timestamp'))
  } else if (i >= 40 & i <= 45) {
    timestamp_edit_sdf_list[[i]] <- timestamp_edit_sdf_list[[i]] %>%
      withColumn('starttime_unix',
                 unix_timestamp(column('start_time'), date_format_str)) %>%
      withColumn('stoptime_unix',
                 unix_timestamp(column('stop_time'), date_format_str)) %>%
      withColumn('starttime_correct',
                 from_unixtime(column('starttime_unix'),
                               'yyyy-MM-dd HH:mm:ss') %>%
                   cast('timestamp')) %>%
      withColumn('stoptime_correct',
                 from_unixtime(column('stoptime_unix'),
                               'yyyy-MM-dd HH:mm:ss') %>%
                   cast('timestamp'))
  } else {
    # print(paste('This statement should not be reached. i =', i))
  }
}

# Union all of the timestamp edited tables together to form our master table
for (i in 1:length(source_sdf_list)) {
  # Replace original start and stop times with the corrected ones and drop
  # intermediate columns
  if (i < 40 | (i >= 46 & i <= 49)) {
    temp_sdf <- timestamp_edit_sdf_list[[i]] %>%
      withColumn('starttime', column('starttime_correct')) %>%
      withColumn('stoptime', column('stoptime_correct')) %>%
      withColumn('start_station_latitude',
                 column('start_station_latitude') %>% cast('double')) %>%
      withColumn('start_station_longitude',
                 column('start_station_longitude') %>% cast('double')) %>%
      withColumn('end_station_latitude',
                 column('end_station_latitude') %>% cast('double')) %>%
      withColumn('end_station_longitude',
                 column('end_station_longitude') %>% cast('double')) %>%
      drop(c('starttime_unix', 'stoptime_unix',
             'starttime_correct', 'stoptime_correct'))
  } else if (i >= 40 & i <= 45) {
    temp_sdf <- timestamp_edit_sdf_list[[i]] %>%
      withColumn('start_time', column('starttime_correct')) %>%
      withColumn('stop_time', column('stoptime_correct')) %>%
      withColumn('start_station_latitude',
                 column('start_station_latitude') %>% cast('double')) %>%
      withColumn('start_station_longitude',
                 column('start_station_longitude') %>% cast('double')) %>%
      withColumn('end_station_latitude',
                 column('end_station_latitude') %>% cast('double')) %>%
      withColumn('end_station_longitude',
                 column('end_station_longitude') %>% cast('double')) %>%
      drop(c('starttime_unix', 'stoptime_unix',
             'starttime_correct', 'stoptime_correct')) %>%
      withColumnRenamed('start_time', 'starttime') %>%
      withColumnRenamed('stop_time', 'stopttime')
  }
  
  if (i == 1) {
    citi_bike_trips_sdf <- temp_sdf
  } else {
    citi_bike_trips_sdf <- union(citi_bike_trips_sdf, temp_sdf)
  }
}

# Save the table to Hive
sql('DROP TABLE IF EXISTS citi_bike_trips')
saveAsTable(citi_bike_trips_sdf, 'citi_bike_trips')