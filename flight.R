Sys.setenv('JAVA_HOME'='/usr/lib/jvm/java-8-openjdk-amd64')
Sys.setenv('HADOOP_HOME'='/usr/local/hadoop-2.8.2')
Sys.setenv('SPARK_HOME'='/usr/local/spark-2.2.1')

library(magrittr)
library(tibble)
library(dplyr)
library(SparkR, lib.loc=file.path(Sys.getenv('SPARK_HOME'),'R', 'lib'))

ext_opts <- '-Dhttp.proxyHost=10.74.1.25 -Dhttp.proxyPort=8080 -Dhttps.proxyHost=10.74.1.25 -Dhttps.proxyPort=8080'

sparkR.session(master = "spark://master:7077",
               appName = 'ml demo',
               sparkConfig = list(spark.driver.memory = '2g'), 
               sparkPackages = 'org.apache.hadoop:hadoop-aws:2.8.2',
               spark.driver.extraJavaOptions = ext_opts)

dat <- read.df('s3n://sparkr-demo/public-data/flight.csv', 
               header = 'true',
               source = 'csv', 
               inferSchema = 'true')

`%++%` <- function(a, b) paste(a, b)
is_weekend_expr <- "case when date_format(to_date(date, 'yyyy/mm/dd'), 'E')" %++%
  "in ('Fri', 'Sat', 'Sun') then 1 else 0 end"

weekday_expr <- "case date_format(to_date(date, 'yyyy/mm/dd'), 'E')" %++%
                  "when 'Mon' then '1' when 'Tue' then '2'" %++%
                  "when 'Wed' then '3' when 'Thu' then '4'" %++%
                  "when 'Fri' then '5' when 'Sat' then '6' else '7' end"

dat <- dat %>% mutate(
  year = expr("cast(split(date, '/')[0] AS integer)"),
  month = expr("cast(split(date, '/')[1] AS integer)"),
  is_weekend = expr(is_weekend_expr),
  weekday = expr(weekday_expr),
  dep_hour = expr('cast(floor(dep_time/100) AS integer)')
) %>% drop('date') %>%
  dropna(cols = c('arr_delay')) %>%
  #filter(expr('arr_delay > 5')) %>%
  filter(expr('cancelled') == 0)

dat %>% group_by('month') %>%
  summarize(avg_delay = expr('mean(arr_delay)'),
            max_delay = expr('max(arr_delay)')) %>%
  arrange('month') %>%
  head(100)

dat %>% group_by('weekday') %>%
  summarize(avg_delay = expr('mean(arr_delay)'),
            max_delay = expr('max(arr_delay)')) %>%
  arrange('weekday') %>%
  head(100)

dat %>% group_by('is_weekend') %>%
  summarize(avg_delay = expr('mean(arr_delay)'),
            max_delay = expr('max(arr_delay)')) %>%
  arrange('is_weekend') %>%
  head(100)

dat %>% group_by('dep_hour') %>%
  summarize(avg_delay = expr('mean(arr_delay)'),
            max_delay = expr('max(arr_delay)')) %>%
  arrange('dep_hour') %>%
  head(100)

dat %>% corr('arr_delay', 'distance')
dat %>% corr('arr_delay', 'dep_delay')

dat_split <- randomSplit(dat, weights = c(0.7, 0.3), 1237)
train <- dat_split[[1]]
test <- dat_split[[2]]

formula <- 'arr_delay ~ dep_delay + is_weekend + dep_hour + month' %>% as.formula()

model <- spark.glm(train, formula, family = 'gaussian')

model <- spark.randomForest(train, formula, 'regression', numTrees = 200)

model <- spark.gbt(train, formula, 'regression')

# Model summary
s <- summary(model)

# Prediction
preds <- predict(model, test)
preds %>% head()

preds %>%
  summarize(
    rmse = expr('sqrt(mean((label-prediction)*(label-prediction)))'),
    mae = expr('mean(abs(label-prediction))')) %>%
  head()

stringr::str_extract_all(s$featureImportances, '\\d+\\.*\\d*')

predictions %>%
  group_by('is_delay') %>%
  pivot('prediction') %>%
  agg(count(column('prediction'))) %>%
  head(10)



