# record_stream

* Clone the repository
* mvn clean install
* configure loader.path environment variable for the location of the JDBC JAR
* configure spring.profiles.active environment variable
* add required properties to the application-{profile}.yaml file
** spring.datasource.url is a JDBC URL (see https://docs.spring.io/spring-boot/docs/current/reference/html/boot-features-sql.html#boot-features-connect-to-production-database)
** jdbc.statement is the SQL statement to execute
** apache.camel.to is the destination endpoint to send the JSON encoded records