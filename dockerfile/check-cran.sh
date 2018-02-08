#!/bin/bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#
# Updated CRAN_BUILD_OPTIONS and CRAN_CHECK_OPTIONS
#

set -o pipefail
set -e

FWDIR="$(cd "`dirname "${BASH_SOURCE[0]}"`"; pwd)"
pushd "$FWDIR" > /dev/null

. "$FWDIR/find-r.sh"

# Install the package (this is required for code in vignettes to run when building it later)
# Build the latest docs, but not vignettes, which is built with the package next
. "$FWDIR/install-dev.sh"

# Build source package with vignettes
SPARK_HOME="$(cd "${FWDIR}"/..; pwd)"
. "${SPARK_HOME}/bin/load-spark-env.sh"
if [ -f "${SPARK_HOME}/RELEASE" ]; then
  SPARK_JARS_DIR="${SPARK_HOME}/jars"
else
  SPARK_JARS_DIR="${SPARK_HOME}/assembly/target/scala-$SPARK_SCALA_VERSION/jars"
fi

if [ -d "$SPARK_JARS_DIR" ]; then
  # Build a zip file containing the source package with vignettes
  CRAN_BUILD_OPTIONS="--no-manual --no-build-vignettes"
  echo "Running CRAN build with $CRAN_BUILD_OPTIONS options"
  SPARK_HOME="${SPARK_HOME}" "$R_SCRIPT_PATH/R" CMD build $CRAN_BUILD_OPTIONS "$FWDIR/pkg"

  find pkg/vignettes/. -not -name '.' -not -name '*.Rmd' -not -name '*.md' -not -name '*.pdf' -not -name '*.html' -delete
else
  echo "Error Spark JARs not found in '$SPARK_HOME'"
  exit 1
fi

# Run check as-cran.
VERSION=`grep Version "$FWDIR/pkg/DESCRIPTION" | awk '{print $NF}'`

CRAN_CHECK_OPTIONS="--no-tests --no-manual --no-vignettes --no-build-vignettes"
echo "Running CRAN check with $CRAN_CHECK_OPTIONS options"
"$R_SCRIPT_PATH/R" CMD check $CRAN_CHECK_OPTIONS "SparkR_$VERSION.tar.gz"

popd > /dev/null