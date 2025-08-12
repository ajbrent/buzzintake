FROM eclipse-temurin:21-jdk-alpine

WORKDIR /app

COPY pom.xml .
COPY dependency-reduced-pom.xml .

COPY src/ ./src/

RUN apk add --no-cache maven && \
    mvn clean package -DskipTests && \
    apk del maven && \
    rm -rf ~/.m2

RUN mkdir -p /app/target
# COPY target/buzzintake-*SNAPSHOT.jar /app/buzzintake.jar
RUN cp target/buzzintake-1.0-SNAPSHOT.jar /app/buzzintake.jar

RUN mkdir -p /app/resources
COPY src/main/resources/gdelt_og_schema.json /app/resources/

ENV JAVA_OPTS="-Xmx2g -Xms1g"
ENV ICEBERG_REST_URI="http://localhost:8181"
ENV ICEBERG_WAREHOUSE="/app/warehouse"
ENV SERVER_PORT="8080"

RUN mkdir -p /app/warehouse

EXPOSE 8080

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar /app/buzzintake.jar"]

CMD []
