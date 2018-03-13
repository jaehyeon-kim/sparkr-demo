#
# Intro to data manipulation - _load data_
#

Sys.setenv('JAVA_HOME'='/usr/lib/jvm/java-8-openjdk-amd64')
Sys.setenv('HADOOP_HOME'='/usr/local/hadoop-2.8.2')
Sys.setenv('SPARK_HOME'='/usr/local/spark-2.2.1')

library(magrittr); library(tibble); library(dplyr)
library(SparkR, lib.loc=file.path(Sys.getenv('SPARK_HOME'),'R', 'lib'))
sparkR.session(master = 'spark://master:7077', appName = 'titanic demo',
               sparkConfig = list(spark.driver.memory = '2g'))

tdf <- read.csv('titanic.csv', stringsAsFactors = FALSE) %>%
  dplyr::sample_frac(1, replace = FALSE) %>% as.tibble()
rec <- nrow(tdf)
df <- as.DataFrame(tdf)

df %>% head(2)

printSchema(df)


#
# Data manipulation - _check data_
#

## more functions
str(df)

summary(df) %>% collect()

df %>% collect() # SparkDataFrame to data.frame

## check classes
df %>% class() # SparkDataFrame
df %>% head() %>% class() # data.frame


#
# Data manipulation - _select, filter..._
#

## column expressions
df$survived # Column survived
column('survived') # Column survived
'survived' # string
expr('survived') # Column survived

## selecting rows, columns
df %>% select(df$survived) %>% head()
df %>% select(column('survived')) %>% head()
df %>% select(expr('survived')) %>% head()
df %>% select('class', 'survived') %>% head()

tdf %>% dplyr::select(class, survived) %>% head()

df %>% filter('survived == "yes" and age == "child"') %>% head()
df %>% filter(df$survived == 'yes' & df$age == 'child') %>% head()

tdf %>% dplyr::filter(survived == 'yes' & age == 'child') %>% head()


#
# Data manipulation - _group_by, mutate ..._
#

## creating variable
df %>% mutate(age_c = ifelse(expr('age') == 'adult', '1', '0')) %>% 
  head(2)

## grouping, aggregation
df %>% group_by('class', 'age') %>%
  summarize(count = n(expr('survived'))) %>%
  arrange('class', 'age') %>% collect()

tdf %>% dplyr::group_by(class, age) %>%
  dplyr::summarise(count = n())


#
# Data manipulation - _join_
#

rdf <- data.frame(age = c('adult', 'child'), lvl = c('0', '1'), stringsAsFactors = FALSE)
rDF <- as.DataFrame(rdf)

df %>% join(rDF, df$age == rDF$age, 'inner') %>%
  group_by('class', 'lvl') %>%
  summarize(count = n(expr('survived'))) %>%
  arrange('class', 'lvl') %>% collect()

tdf %>% dplyr::inner_join(rdf, by = 'age') %>%
  dplyr::group_by(class, lvl) %>%
  dplyr::summarise(count = n())


#
# Data manipulation case study - _intro_
#

#
# Data manipulation case study - _multiple transformations_
#

tmp <- df %>% group_by('class', 'age') %>%
  summarize(count = n(expr('survived')))
tmp %>% mutate(prop = expr('count') / rec) %>% 
  arrange('class', 'age') %>% collect()


tdf %>% dplyr::group_by(class, age) %>%
  dplyr::summarise(count = n()) %>% 
  dplyr::mutate(prop = count / rec)


#
# Data manipulation - _dapply_
#

## dapply, dapplyCollect
schema <- structType(
  structField('class', 'string'),
  structField('age', 'string'),
  structField('count', 'double'), # not integer
  structField('prop', 'double')
)

fn <- function(x) {
  cbind(x, x$count / rec) # expr() not working
}

# may take more time but no temporary DF
df %>% group_by('class', 'age') %>%
  summarize(count = n(expr('survived'))) %>%
  dapply(fn, schema) %>%
  arrange('class', 'age') %>% collect()


#
# Data manipulation - _gapply_
#

## gapply, gapplyCollect
schema <- structType(
  structField('class', 'string'),
  structField('age', 'string'),
  structField('count', 'integer'),
  structField('prop', 'double')
)

fn <- function(key, x) {
  data.frame(key, nrow(x), nrow(x)/rec, stringsAsFactors = FALSE)
}

df %>% gapply(cols = c('class', 'age'), func = fn, schema = schema) %>%
  arrange('class', 'age') %>% collect()


#
# Data manipulation - _SQL_
#

## sql queries
createOrReplaceTempView(df, 'titanic_tbl')

`%++%` <- function(a, b) paste(a, b)
qry <- '
  SELECT class, age, count(*) as count, count(*) /' %++% 
  format(round(rec, 1), nsmall = 1) %++% 'as prop' %++%
  'FROM titanic_tbl' %++%
  'group by class, age' %++%
  'order by class, age'

sql(qry) %>% collect()


#  
# Data manipulation - _spark.lapply_
# 
 
## spark.lapply
discnt <- tdf %>% dplyr::distinct(class, age)
lst <- lapply(1:nrow(discnt), function(i) {
  cls <- discnt[i, 1] %>% unlist()
  ag <- discnt[i, 2] %>% unlist()
  list(dat = tdf %>% dplyr::filter(class == cls & age == ag),
       rec = rec)
})

fn <- function(elem) {
  library(magrittr)
  elem$dat %>% dplyr::group_by(class, age) %>%
    dplyr::summarise(count = n(), prop = count / elem$rec)
}

spark.lapply(lst, fn) %>%
  bind_rows() %>%
  dplyr::arrange(class, age)

