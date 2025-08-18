#!/bin/sh

export HADOOP_VERSION=3.3.1
export HADOOP_HOME=/opt/hadoop-${HADOOP_VERSION}
export HADOOP_CLASSPATH=$HADOOP_HOME/share/hadoop/tools/lib/aws-java-sdk-bundle-1.11.901.jar:$HADOOP_HOME/share/hadoop/tools/lib/hadoop-aws-${HADOOP_VERSION}.jar
export HIVE_AUX_JARS_PATH=$HADOOP_CLASSPATH
export JAVA_HOME=/usr/local/openjdk-8
export DB_HOSTNAME=${DB_HOSTNAME:-localhost}

echo "Waiting for MySQL database on ${DB_HOSTNAME} to launch..."
while ! nc -z $DB_HOSTNAME 3306; do
    sleep 1
done


# Check if schema exists
/opt/apache-hive-metastore-3.0.0-bin/bin/schematool -dbType mysql -info

if [ $? -eq 1 ]; then
  echo "Initializing schema..."
  /opt/apache-hive-metastore-3.0.0-bin/bin/schematool -initSchema -dbType mysql
fi

echo "Starting Hive metastore service on $DB_HOSTNAME:3306"
/opt/apache-hive-metastore-3.0.0-bin/bin/start-metastore
