# Apache's Iceberg REST catalog fixture, plus the PostgreSQL JDBC driver.
# The fixture's default SQLite backend allows one writer at a time, so concurrent commits (Flink's bronze and
# silver Iceberg committers fire together on every checkpoint) failed with SQLITE_BUSY / SQLITE_BUSY_SNAPSHOT.
FROM apache/iceberg-rest-fixture:1.10.1
ADD --chown=iceberg:iceberg https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.12/postgresql-42.7.12.jar /usr/lib/iceberg-rest/postgresql.jar
CMD ["java", "-cp", "iceberg-rest-adapter.jar:postgresql.jar", "org.apache.iceberg.rest.RESTCatalogServer"]
