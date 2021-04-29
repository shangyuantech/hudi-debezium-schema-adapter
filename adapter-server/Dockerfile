FROM openjdk:8-jre-alpine

RUN apk --no-cache add tzdata  && \
    ln -sf /usr/share/zoneinfo/Asia/Shanghai /etc/localtime && \
    echo "Asia/Shanghai" > /etc/timezone

RUN mkdir -p /opt/soft
ENV SERVICE_HOME=/opt/soft
WORKDIR ${SERVICE_HOME}

COPY deploy/entrypoint.sh ${SERVICE_HOME}/entrypoint.sh
RUN chmod +x ${SERVICE_HOME}/entrypoint.sh

ARG JAR_FILE=target/adapter-server-*.jar
COPY ${JAR_FILE} ${SERVICE_HOME}/adapter-server.jar

EXPOSE 8080

ENTRYPOINT ["/opt/soft/entrypoint.sh"]