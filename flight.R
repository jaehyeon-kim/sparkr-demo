#
# Machine Learning - _session initialization_
#

Sys.setenv('JAVA_HOME'='/usr/lib/jvm/java-8-openjdk-amd64')
Sys.setenv('HADOOP_HOME'='/usr/local/hadoop-2.8.2')
Sys.setenv('SPARK_HOME'='/usr/local/spark-2.2.1')

library(ggplot2)
library(magrittr)
library(tibble)
library(dplyr)
library(SparkR, lib.loc=file.path(Sys.getenv('SPARK_HOME'),'R', 'lib'))

source('utils.R')
seed <- 1237
ext_opts <- '-Dhttp.proxyHost=10.74.1.25 -Dhttp.proxyPort=8080 -Dhttps.proxyHost=10.74.1.25 -Dhttps.proxyPort=8080'

sparkR.session(master = "spark://master:7077",
               appName = 'ml demo',
               sparkConfig = list(spark.driver.memory = '2g'), 
               sparkPackages = 'org.apache.hadoop:hadoop-aws:2.8.2',
               spark.driver.extraJavaOptions = ext_opts)


#
# Machine Learning - _load data from S3_
#

dat <- read.df('s3n://sparkr-demo/public-data/flight_2007.csv', 
               header = 'true',
               source = 'csv', 
               inferSchema = 'true')

#
# Machine Learning - _data exploration_
#

# dat_s <- randomSplit(dat, weights = c(0.1, 0.9), seed)[[1]] %>%
#   collect() %>% as.tibble()
# readr::write_csv(dat_s, './flight-present/flight_2007_10p.csv')
# dat_s <- readr::read_csv('./flight-present/flight_2007_10p.csv')
dat_s <- dat_s %>% 
  dplyr::filter(!is.na(arr_delay) & !is.na(dep_delay)) %>%
  dplyr::mutate(
    month = as.integer(format(as.Date(date, format('%Y/%m/%d')), '%m')),
    weekday = weekdays(as.Date(date, format('%Y/%m/%d')), TRUE),
    weekday = factor(weekday, levels = c('Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun')),
    is_weekend = case_when(
      weekday %in% c('Fri', 'Sat', 'Sun') ~ 1,
      TRUE ~ 0),
    dep_hour = floor(dep_time/100),
    arr_hour = floor(arr_time/100),
    is_delay = if_else(arr_delay > 15, 'yes', 'no')
  ) %>%
  dplyr::filter(cancelled == 0) %>%
  dplyr::select(-date, -cancelled, -dep_time, -arr_time)

dat_s %>% select(-unique_carrier, -origin, -dest) %>% head()


#
# Machine Learning - _data exploration ctd_
#

# use as is
get_multiplot('weekday')

# not using
get_multiplot('is_weekend')

# regrouping
# '1' if month is 12 or 1-3
# '2' if month is 6-8
# '3' if month is 4-5 or 9-11
get_multiplot('month')

# regrouping
# '1' if dep_hour is 4-12
# '2' if dep_hour is 13-19
# '3' if dep_hour is 0-3 or 20+
get_multiplot('dep_hour')

## only use dep_delay
bind_rows(
  summarise_cont(dat_s, 'dep_delay'),
  summarise_cont(dat_s, 'distance'),
  summarise_cont(dat_s, 'air_time')
)

## dep_delay is highly correlated with arr_delay
dat_s %>% dplyr::select(arr_delay, dep_delay) %>% cor()


#
# Machine Learning - _feature generation_
#

`%++%` <- function(a, b) paste(a, b)
month_c_expr <- 
  "case when split(date, '/')[1] in ('6', '7', '8') then '2'" %++%
    "when split(date, '/')[1] in ('1', '2', '3', '12') then '1'" %++%
  "else '3' end"
weekday_expr <- 
  "case date_format(to_date(date, 'yyyy/mm/dd'), 'E')" %++%
    "when 'Mon' then '1' when 'Tue' then '2'" %++%
    "when 'Wed' then '3' when 'Thu' then '4'" %++%
    "when 'Fri' then '5' when 'Sat' then '6'" %++% 
  "else '7' end"
dep_hour_c_expr <- 
  "case when cast(floor(dep_time/100) AS integer) <= 3 then '3'" %++%
    "when cast(floor(dep_time/100) AS integer) <= 12 then '1'" %++%
    "when cast(floor(dep_time/100) AS integer) <= 19 then '2'" %++%
  "else '3' end"

dat <- dat %>% dropna(cols = c('arr_delay', 'dep_delay')) %>%
  mutate(
    month_c = expr(month_c_expr),
    weekday = expr(weekday_expr),
    dep_hour_c = expr(dep_hour_c_expr),
    is_delay = ifelse(expr('arr_delay') > 15, 'yes', 'no')
) %>% 
  filter(expr('cancelled == 0'))

sel_cols <- c('is_delay', 'dep_delay', 'month_c', 'dep_hour_c', 'weekday')
dat %>% select(sel_cols) %>% head()

## verify variables
# dat %>% mutate(
#   month = expr("cast(split(date, '/')[1] as integer)")
# ) %>% select(c('month', 'month_c')) %>%
#   distinct() %>% arrange(expr('month')) %>%
#   collect()
# 
# dat %>% mutate(
#   dep_hour = expr("cast(floor(dep_time/100) AS integer)")
# ) %>% select(c('dep_hour', 'dep_hour_c')) %>%
#   distinct() %>% arrange(expr('dep_hour')) %>%
#   collect()


#
# Machine Learning - _model fitting_
#

dat_split <- randomSplit(dat, weights = c(0.7, 0.3), seed)
train <- dat_split[[1]]
test <- dat_split[[2]]

formula <- 'is_delay ~ dep_delay + month_c + dep_hour_c + weekday' %>% 
  as.formula()

model <- spark.randomForest(train, formula, 'classification')
## writing/loading model
# write.ml(model, 's3n://sparkr-demo/model/flight_2007_rf.model')
# model <- read.ml('s3n://sparkr-demo/model/flight_2007_rf.model')


#
# Machine Learning - _model evaluation_
#

preds <- predict(model, test)

hpreds <- preds %>% head(50) %>%
  dplyr::select(is_delay, prediction, probability, rawPrediction) %>%
  dplyr::rename(prob = probability, raw_pred = rawPrediction)

hpreds %>% head()


#
# Machine Learning - _model evaluation ctd_
#

# readr::write_csv(hpreds_up, './flight-present/hpreds_up.csv')
# hpreds_up <- readr::read_csv('./flight-present/hpreds_up.csv')
hpreds_up <- bind_cols(hpreds,
                       extract_from_jmethod(hpreds, 'prob'),
                       extract_from_jmethod(hpreds, 'raw_pred')
) %>% dplyr::select(-prob, -raw_pred)

hpreds_up %>% head()


#
# Machine Learning - _model evaluation ctd_
#

# readr::write_csv(cmat, './flight-present/cmat.csv')
# cmat <- readr::read_csv('./flight-present/cmat.csv')
cmat <- preds %>% crosstab('is_delay', 'prediction')
cmat

# accuracy
1 - sum(cmat$no[1], cmat$yes[2])/sum(cmat$no, cmat$yes)
# 0.9096646


#
# Machine Learning - _model evaluation ctd_
#

## feature importance
s <- summary(model)

importance <- get_feat_importance(model)

# readr::write_csv(importance, './flight-present/importance.csv')
# importance <- readr::read_csv('./flight-present/importance.csv')
ggplot(importance, aes(x=feature, y=importance)) +
  geom_bar(stat = 'identity', fill = 'steel blue') +
  ggtitle('Feature importance') +
  theme(plot.title = element_text(hjust = 0.5), 
        axis.text.x = element_text(angle = 45, hjust = 1))
