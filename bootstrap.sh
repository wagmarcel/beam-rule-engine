echo bootstrap.sh called ...
echo export VERSION=0.1 > version.properties;
source ./version.properties;
export RULE_ENGINE_PACKAGE_NAME=rule-engine-bundled-${VERSION}.jar
/app/local-deploy.sh
#/app/wait-for-it.sh localhost:7077 -t 300 -- /app/local-deploy.sh &
#/usr/local/spark*/bin/spark-class org.apache.spark.deploy.master.Master -h $(hostname)
#spark master will be needed in a cluster deployment. currently, with local execution no daemon is needed
