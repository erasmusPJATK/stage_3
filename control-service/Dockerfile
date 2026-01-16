FROM maven:3.9.9-eclipse-temurin-17 AS build
WORKDIR /app
COPY pom.xml .
COPY src ./src
RUN mvn -q -DskipTests clean package

FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /app/target/*-all.jar /app/app.jar
EXPOSE 7000
ENTRYPOINT ["java","-jar","/app/app.jar"]
