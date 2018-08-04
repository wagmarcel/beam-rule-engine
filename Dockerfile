# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#

#Build beam application and embedd in Spark container
FROM jamesdbloom/docker-java8-maven

RUN apt-get update -qq && apt-get install -y build-essential

ADD pom.xml /app/pom.xml
ADD src /app/src

WORKDIR /app

RUN mvn clean package -Pspark-runner  -DskipTests




FROM alpine:3.8
EXPOSE 6066 7077 8080



RUN mkdir /app
COPY --from=0 /app/target/rule-engine-bundled-0.1.jar /app
RUN mkdir -p /opt/spark
RUN wget https://archive.apache.org/dist/spark/spark-2.2.1/spark-2.2.1-bin-hadoop2.7.tgz -O /opt/spark/tmp.tgz
RUN cd /opt/spark; tar xvzf tmp.tgz --strip 1; rm tmp.tgz

RUN apk update
RUN apk add python py-pip wget bash openjdk8-jre libc6-compat
RUN pip install poster
RUN pip install requests
RUN pip install kafka-python
ADD deployer /app/deployer
ADD bootstrap.sh /app
ADD local-deploy.sh /app
ADD wait-for-it.sh /app

RUN chmod +x /app/bootstrap.sh

WORKDIR /app

CMD /bin/bash -c /app/bootstrap.sh


