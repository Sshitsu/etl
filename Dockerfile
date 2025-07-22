FROM eclipse-temurin:17-jdk-alpine
LABEL authors="globa"

WORKDIR /app

COPY target/etl-app.jar ./etl-app.jar

COPY entrypoint.sh ./entrypoint.sh
RUN chmod +x ./entrypoint.sh

ENTRYPOINT ["./entrypoint.sh"]