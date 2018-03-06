Sys.setenv('JAVA_HOME'='/usr/lib/jvm/java-8-openjdk-amd64')
Sys.setenv('HADOOP_HOME'='/usr/local/hadoop-2.8.2')
Sys.setenv('SPARK_HOME'='/usr/local/spark-2.2.1')

library(magrittr)
library(tibble)
library(dplyr)
library(SparkR, lib.loc=file.path(Sys.getenv('SPARK_HOME'),'R', 'lib'))

# https://spark.apache.org/docs/2.2.1/configuration.html
ext_opts <- '-Dhttp.proxyHost=10.74.1.25 -Dhttp.proxyPort=8080 -Dhttps.proxyHost=10.74.1.25 -Dhttps.proxyPort=8080'

sparkR.session(master = "spark://master:7077",
               appName = 'sparkr_demo_flight',
               sparkConfig = list(spark.driver.memory = '2g', spark.network.timeout = '12000s'), 
               sparkPackages = 'org.apache.hadoop:hadoop-aws:2.8.2',
               spark.driver.extraJavaOptions = ext_opts)

dat <- read.df('s3n://sparkr-demo/public-data/flight.csv', 
               header = 'true',
               source = 'csv', 
               inferSchema = 'true')

`%++%` <- function(a, b) paste(a, b)
weekend_expr <- "case when date_format(to_date(date, 'yyyy/mm/dd'), 'E')" %++%
                      "in ('Fri', 'Sat', 'Sun') then 1 else 0 end"
dat <- dat %>% mutate(
  year = expr("cast(split(date, '/')[0] AS integer)"),
  month = expr("cast(split(date, '/')[1] AS integer)"),
  day = expr("cast(split(date, '/')[2] AS integer)"),
  weekend = expr(weekend_expr),
  dep_hour = floor(expr('dep_time')/100),
  is_delay = cast(ifelse(expr('arr_delay > 15'), 1, 0), 'integer')
) %>% drop('date') %>%
  filter(expr('cancelled') == 0) %>%
  dropna(cols = c('arr_delay'))

dat_split <- randomSplit(dat, weights = c(0.7, 0.3), 1237)
train <- dat_split[[1]]
test <- dat_split[[2]]








