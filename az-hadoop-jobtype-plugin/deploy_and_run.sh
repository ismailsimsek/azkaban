#!/bin/bash
set -e

# clean 
rm -rf /Users/isimsek/development/azkaban/az-hadoop-jobtype-plugin/build/libs/*jar
echo "## Building the app..."
cd /Users/isimsek/development/azkaban ; ./gradlew build -x test ;
echo "## Deploy to server..."
rm -rf /opt/azkaban/plugins/jobtypes/jdbcSql
cp -r /Users/isimsek/development/azkaban/az-hadoop-jobtype-plugin/src/jobtypes/jdbcSql /opt/azkaban/plugins/jobtypes/jdbcSql
cp -f -r /Users/isimsek/development/azkaban/az-hadoop-jobtype-plugin/build/libs/*jar /opt/azkaban/plugins/jobtypes/jdbcSql/

rm -rf /opt/azkaban/plugins/jobtypes/pentaho
cp -r /Users/isimsek/development/azkaban/az-hadoop-jobtype-plugin/src/jobtypes/pentaho /opt/azkaban/plugins/jobtypes/pentaho
cp -f -r /Users/isimsek/development/azkaban/az-hadoop-jobtype-plugin/build/libs/*jar /opt/azkaban/plugins/jobtypes/pentaho/

echo "## Restart..."
cd /opt/azkaban ; bash bin/shutdown-solo.sh ; bash bin/start-solo.sh
cd /Users/isimsek/development/azkaban/az-hadoop-jobtype-plugin/
echo "## Done"
