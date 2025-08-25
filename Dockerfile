FROM eclipse-temurin:21-jdk-alpine

WORKDIR /app

COPY pom.xml .

COPY src/ ./src/

RUN apk add --no-cache maven && \
    mvn -q clean package -DskipTests && \
    apk del maven && \
    rm -rf ~/.m2

RUN mkdir -p /app/target
RUN cp target/buzzintake-1.0-SNAPSHOT.jar /app/buzzintake.jar

RUN mkdir -p /app/resources
COPY src/main/resources/gdelt_og_schema.json /app/resources/

# Accept build args and set as environment variables
ARG ICEBERG_REST_URI="http://localhost:8181"
ARG S3_BUCKET_NAME="default"
ARG ICEBERG_WAREHOUSE="s3://${S3_BUCKET_NAME}/warehouse"
ENV ICEBERG_REST_URI=${ICEBERG_REST_URI}
ENV ICEBERG_WAREHOUSE=${ICEBERG_WAREHOUSE}
ENV SERVER_PORT="8080"

EXPOSE 8080

ENTRYPOINT ["java","-jar","/app/buzzintake.jar"]

CMD []
