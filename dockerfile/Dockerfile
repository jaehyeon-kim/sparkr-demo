FROM rocker/tidyverse:3.4.3
MAINTAINER Jaehyeon Kim <dottami@gmail.com>

ARG SPARK_VERSION
ENV SPARK_VERSION=${SPARK_VERSION:-2.2.0} \
    # $(readlink -f /usr/bin/javac | sed "s:/bin/javac::")
    JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64 \
    MAVEN_OPTS='-Xmx2g -XX:ReservedCodeCacheSize=512m'

RUN apt-get update \
    && apt-get install -y openjdk-8-jdk

RUN wget -q http://d3kbcqa49mib13.cloudfront.net/spark-${SPARK_VERSION}.tgz  \
    && tar xf spark-${SPARK_VERSION}.tgz \
    && cd spark-${SPARK_VERSION} \
    && ./dev/make-distribution.sh --name sparkr-hive-yarn2.6 --pip --r --tgz -Psparkr -Phive-thriftserver -Pyarn -DskipTests clean package
