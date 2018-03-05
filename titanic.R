Sys.setenv('JAVA_HOME'='/usr/lib/jvm/java-8-openjdk-amd64')
Sys.setenv('HADOOP_HOME'='/usr/local/hadoop-2.8.2')
Sys.setenv('SPARK_HOME'='/usr/local/spark-2.2.1')

library(magrittr)
library(tibble)
library(dplyr)
library(SparkR, lib.loc=file.path(Sys.getenv('SPARK_HOME'),'R', 'lib'))
sparkR.session(master = 'spark://master:7077', 
               appName = 'titanic demo',
               sparkConfig = list(spark.driver.memory = '2g'))

## load data
titanic <- read.csv('titanic.csv', stringsAsFactors = FALSE)
rec <- nrow(titanic)
df <- as.DataFrame(titanic)
tdf <- as.tibble(titanic)

## same outputs to base R
str(df)
head(df)

## check classes
df %>% class() # SparkDataFrame
df %>% head() %>% class() # data.frame

#### selecting rows, columns
df %>% select(df$survived) %>% head()
df %>% select(expr('survived')) %>% head()
df %>% select('class', 'survived') %>% head()

tdf %>% dplyr::select(class, survived) %>% head()

df %>% filter('survived == "yes" and age == "child"') %>% head()
df %>% filter(df$survived == 'yes' & df$age == 'child') %>% head()

tdf %>% dplyr::filter(survived == 'yes' & age == 'child') %>% head()

## expressions
df$survived # Column survived
expr('survived') # Column survived
'survived' # string

#### grouping, aggregation
df %>% group_by('class', 'age') %>%
  summarize(count = n(expr('survived'))) %>%
  arrange('class', 'age') %>% head(7)

tdf %>% dplyr::group_by(class, age) %>%
  dplyr::summarise(count = n()) %>% head(7)

#### operating on columns
tmp <- df %>% group_by('class', 'age') %>%
  summarize(count = n(expr('survived')))
tmp %>% mutate(prop = expr('count') / rec) %>% 
  arrange('class', 'age') %>% head(10)

tdf %>% dplyr::group_by(class, age) %>%
  dplyr::summarise(count = n()) %>% 
  dplyr::mutate(prop = count / rec) %>%
  head(10)

#### user defined functions
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
  arrange('class', 'age') %>% head(10)

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
  arrange('class', 'age') %>% head(10)

#### sql queries
createOrReplaceTempView(df, 'titanic_tbl')

`%+%` <- function(a, b) paste(a, b)
qry <- '
  SELECT class, age, count(*) as count, count(*) /' %+% 
  format(round(rec, 1), nsmall = 1) %+% 'as prop' %+%
  'FROM titanic_tbl' %+%
  'group by class, age' %+%
  'order by class, age'

sql(qry) %>% head(10)

#### spark.lapply
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
  dplyr::arrange(class, age) %>%
  head(10)
