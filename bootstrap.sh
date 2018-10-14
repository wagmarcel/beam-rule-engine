echo bootstrap.sh called ...
echo export VERSION=0.1 > version.properties;
source ./version.properties;
export RULE_ENGINE_PACKAGE_NAME=rule-engine-bundled-${VERSION}.jar
echo "now deploying Rule Engine App"
/app/wait-for-it.sh localhost:8081 -t 300 -- /app/local-deploy.sh &
echo "now starting flink master service"
/docker-entrypoint.sh jobmanager


