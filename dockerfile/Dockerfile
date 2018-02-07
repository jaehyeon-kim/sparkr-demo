FROM rocker/tidyverse:3.4.3
MAINTAINER Jaehyeon Kim <dottami@gmail.com>

ARG SPARK_VERSION
ENV SPARK_VERSION=${SPARK_VERSION:-2.2.0} \
    # $(readlink -f /usr/bin/javac | sed "s:/bin/javac::")
    JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64

#### build spark with hive - http://spark.apache.org/docs/latest/building-spark.html
RUN apt-get update \
    && apt-get install -y openjdk-8-jdk

RUN wget -q http://d3kbcqa49mib13.cloudfront.net/spark-${SPARK_VERSION}.tgz  \
    && tar xf spark-${SPARK_VERSION}.tgz

# force R CMD CHECK with --no-tests, --no-manual, --no-vignettes
ADD ./check-cran.sh /spark-${SPARK_VERSION}/R/check-cran.sh

# make distribution
RUN cd spark-${SPARK_VERSION} \
    && ./dev/make-distribution.sh --name sparkr-hive-yarn2.6 --pip --r --tgz -Psparkr -Pyarn -Phive -Phive-thriftserver -DskipTests clean package
