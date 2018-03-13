# Intro to data manipulation - _load data_

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



# Data manipulation - _check data_

## more functions
str(df)

summary(df) %>% collect()

df %>% collect() # SparkDataFrame to data.frame

## check classes
df %>% class() # SparkDataFrame
df %>% head() %>% class() # data.frame



# Data manipulation - _select, filter..._

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

# - many function names are same to _dplyr_
#     + use `::` for calling them
# - expressions are interchangeable but not always - see _dapply_ section
# - `expr()` is more expressive - see ML section



# Data manipulation - _group_by, mutate ..._

## creating variable
df %>% mutate(age_c = ifelse(expr('age') == 'adult', '1', '0')) %>% 
  head(2)

## grouping, aggregation
df %>% group_by('class', 'age') %>%
  summarize(count = n(expr('survived'))) %>%
  arrange('class', 'age') %>% collect()

tdf %>% dplyr::group_by(class, age) %>%
  dplyr::summarise(count = n())



# Data manipulation - _join_

rdf <- data.frame(age = c('adult', 'child'), lvl = c('0', '1'), stringsAsFactors = FALSE)
rDF <- as.DataFrame(rdf)

df %>% join(rDF, df$age == rDF$age, 'inner') %>%
  group_by('class', 'lvl') %>%
  summarize(count = n(expr('survived'))) %>%
  arrange('class', 'lvl') %>% collect()

tdf %>% dplyr::inner_join(rdf, by = 'age') %>%
  dplyr::group_by(class, lvl) %>%
  dplyr::summarise(count = n())

# * _joinType_
# + default - inner
# + inner, cross, outer, full, full_outer, left, left_outer, right, right_outer, left_semi, or left_anti



# Data manipulation - _example_

tmp <- df %>% group_by('class', 'age') %>%
  summarize(count = n(expr('survived')))
tmp %>% mutate(prop = expr('count') / rec) %>% 
  arrange('class', 'age') %>% collect()

#    class   age count        prop                                                
# 1   crew adult   885 0.402089959
# 2  first adult   319 0.144934121
# 3  first child     6 0.002726034
# 4 second adult   261 0.118582463
# 5 second child    24 0.010904134
# 6  third adult   627 0.284870513
# 7  third child    79 0.035892776

tdf %>% dplyr::group_by(class, age) %>%
  dplyr::summarise(count = n()) %>% 
  dplyr::mutate(prop = count / rec)

# - want to obtain _count_ and _prop_ by _class_ and _age_
# - unlike _dplyr_, not possible to refer to a column that's created in a chain
#    + temporary DF is created
# - 4 ways to achieve without a temp DF



# Data manipulation - _dapply_

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

# - `dapply()` - apply a function to each partition of a _SparkDataFrame_
# - note `expr()`/_string_ don't work in the function
# - will be more efficient if applied to a grouped data



# Data manipulation - _gapply_

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

# - `dapply()` - apply a function to each partition of a grouped _SparkDataFrame_
# - note `nrow()` is not base R function



# Data manipulation - _SQL_

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

# - SQL can be applied after creating/replacing a temporary view
# - [window functions](https://databricks.com/blog/2015/07/15/introducing-window-functions-in-spark-sql.html) introduced in Spark 2
# - do we need [HiveQL](https://cwiki.apache.org/confluence/display/Hive/LanguageManual)?


  
# Data manipulation - _spark.lapply_
  
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

# - run non-SparkR functions over a list of elements and distributes the computations with Spark
# - limitation - results of all the computations should fit in a single machine
