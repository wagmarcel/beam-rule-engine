echo bootstrap.sh called ...
echo export VERSION=0.1 > version.properties;
source ./version.properties;
export RULE_ENGINE_PACKAGE_NAME=rule-engine-bundled-${VERSION}.jar
echo "now deploying Rule Engine App"
jobmanager.sh start
/app/wait-for-it.sh localhost:8081 -t 300 -- echo "flink jobmanager started. Now starting application."
/app/local-deploy.sh 
CONFIG=$(/app/local-deploy.sh 2>/dev/null)
echo "Configuration: $CONFIG"
flink run -c org.oisp.RuleEngineBuild target/rule-engine-bundled-0.1.jar --runner=FlinkRunner --streaming=true --JSONConfig="$CONFIG"
taskmanager.sh start-foreground
echo "started jobmanager and one taskmanager -- but should not reach this point"

