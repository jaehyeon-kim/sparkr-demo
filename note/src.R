Sys.setenv('JAVA_HOME'='/usr/lib/jvm/java-8-openjdk-amd64')
Sys.setenv('HADOOP_HOME'='/usr/local/hadoop-2.8.2')
Sys.setenv('SPARK_HOME'='/usr/local/spark-2.2.1')

library(magrittr)
library(tibble)
library(dplyr)
library(SparkR, lib.loc=file.path(Sys.getenv('SPARK_HOME'),'R', 'lib'))
sparkR.session(master = "spark://master:7077", sparkConfig = list(spark.driver.memory = "2g"))

## from local data frames
df <- as.DataFrame(faithful)
tdf <- as.tibble(faithful)

## selecting rows, columns
df %>% select(df$eruptions) %>% head()
df %>% select('eruptions') %>% head()
tdf %>% dplyr::select(eruptions) %>% head()

df %>% filter('waiting < 50') %>% head()
df %>% filter(df$waiting < 50) %>% head()
tdf %>% dplyr::filter(waiting < 50) %>% head()

## grouping, aggregation
df %>% group_by('waiting') %>%
  summarize(count = n(df$waiting)) %>% 
  arrange('waiting') %>%
  head()
tdf %>% dplyr::group_by(waiting) %>%
  dplyr::summarise(count = n()) %>% head()

## operating on columns
df %>% mutate(waiting = df$waiting * 60) %>%
  head()
tdf %>% dplyr::mutate(waiting = waiting * 60) %>%
  head()

## user defined functions
# dapply, dapplyCollect
schema <- structType(structField('eruptions', 'double'),
                     structField('waiting', 'double'),
                     structField('waiting_secs', 'double'))
fun <- function(x) {
  cbind(x, x$waiting * 60)
}

df %>% dapply(fun, schema) %>% head()

tdf %>% bind_cols(list(waiting_secs = tdf$waiting * 60)) %>%
  head()

# gapply, gapplyCollect <- multiple keys?
schema <- structType(structField('waiting', 'double'),
                     structField('max_eruption', 'double'))
fun <- function(key, x) {
  data.frame(key, max(x$eruptions))
}

df %>% gapply('waiting', fun, schema) %>% 
  arrange('waiting') %>%
  head()

tdf %>% dplyr::group_by(waiting) %>% 
  dplyr::summarise(max_eruption = max(eruptions)) %>%
  head()

# spark.lapply
families <- c('gaussian', 'poisson')
train <- function(fmly) {
  model <- glm(Sepal.Length ~ Sepal.Width + Species, iris, family = fmly)
  summary(model)
}

spark.lapply(families, train)

##??

