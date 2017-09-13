library(dplyr)
library(ggplot2)
library(magrittr)
library(scales)
library(SparkR)

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

# Plot hourly usage for weekday
png(filename = 'plots/hourly_usage_weekday.png', width = 700, height = 400)
ggplot() +
  geom_bar(data = start_hour_weekday_group_df,
           aes(x = start_hour - 0.2,
               y = count,
               fill = 'Start'),
           stat = 'identity',
           width = 0.4) +
  geom_bar(data = end_hour_weekday_group_df,
           aes(x = stop_hour + 0.2,
               y = count,
               fill = 'Stop'),
           stat = 'identity',
           width = 0.4) +
  geom_vline(xintercept = 8.5, size = 1, linetype = 2) +
  geom_vline(xintercept = 17.5, size = 1, linetype = 2) +
  scale_fill_manual(values = c('royalblue', 'orangered3')) +
  scale_y_continuous(labels = comma) +
  hist_theme +
  labs(title = 'Hourly Usage For Weekdays', x = 'Hour of Day', y = 'Frequency')
dev.off()

# Plot hourly usage for weekend
png(filename = 'plots/hourly_usage_weekend.png', width = 700, height = 400)
ggplot() +
  geom_bar(data = start_hour_weekend_group_df,
           aes(x = start_hour - 0.2,
               y = count,
               fill = 'Start'),
           stat = 'identity',
           width = 0.4) +
  geom_bar(data = end_hour_weekend_group_df,
           aes(x = stop_hour + 0.2,
               y = count,
               fill = 'Stop'),
           stat = 'identity',
           width = 0.4) +
  scale_fill_manual(values = c('royalblue', 'orangered3')) +
  scale_y_continuous(labels = comma) +
  hist_theme +
  labs(title = 'Hourly Usage for Weekends', x = 'Hour of Day', y = 'Frequency')
dev.off()


# Look at day of the week
weekday_count_df <- citi_bike_trips_sdf %>%
  select(date_format(column('starttime'), 'E') %>% 
           alias('day_of_week')) %>%
  select('day_of_week') %>%
  groupBy('day_of_week') %>%
  count() %>%
  orderBy('day_of_week') %>%
  collect()

# Create a mapping from day of week to number
weekday_vec <- 1:7
names(weekday_vec) <- c('Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat')

weekday_count_df$ordinal_number <- factor(weekday_vec[weekday_count_df$day_of_week])

# Sort by day of week
weekday_count_df <- weekday_count_df %>%
  dplyr::arrange(ordinal_number)

ggplot(weekday_count_df, aes(x = ordinal_number, y = count)) +
  geom_bar(stat = 'identity') +
  labs(x = 'Day of Week', y = 'Frequency') +
  scale_x_discrete(labels = names(weekday_vec))