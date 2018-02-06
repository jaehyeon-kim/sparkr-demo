FROM rocker/tidyverse:3.4.3
MAINTAINER Jaehyeon Kim <dottami@gmail.com>

ARG SPARK_VERSION
ENV SPARK_VERSION=${SPARK_VERSION:-2.2.0} \
    # $(readlink -f /usr/bin/javac | sed "s:/bin/javac::")
    JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64

RUN apt-get update \
    && apt-get install -y openjdk-8-jdk

## build spark with hive - http://spark.apache.org/docs/latest/building-spark.html
# install necessary packages for testing SparkR
RUN R -e "install.packages(c('knitr', 'rmarkdown', 'testthat', 'e1071', 'survival'))"

# make distribution
RUN wget -q http://d3kbcqa49mib13.cloudfront.net/spark-${SPARK_VERSION}.tgz  \
    && tar xf spark-${SPARK_VERSION}.tgz \
    && cd spark-${SPARK_VERSION} \
    && ./dev/make-distribution.sh --name sparkr-hive-yarn2.6 --pip --r --tgz -Psparkr -Pyarn -Phive -Phive-thriftserver -DskipTests clean package
