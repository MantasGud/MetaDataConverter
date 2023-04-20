# MetaDataConverter

MetadataConverter is a Java command-line tool that extracts metadata 
from DB2/AS400 databases (at the moment) 
and generates Oracle DDL and CSV files containing metadata and index information for the specified tables.

## Usage

```bash
java -jar MetaDataConverter.jar <url> <username> <password> <schema> <output_file> <table list file> <as400 file jar> 
```

## Arguments
- **url**: The JDBC URL to connect to the AS400 database, without the schema part.
- **username**: The username for connecting to the database.
- **password**: The password for connecting to the database.
- **schema**: The schema containing the tables to be processed.
- **output_file**: The base name for the output files (CSV and DDL files will be generated).
- **table list file**: The path to a text file containing the list of table names to process, one table name per row.
- **as400 file jar**: The path to the AS400 JDBC driver JAR file (e.g., as400-7.2.0.5.jar).

## Output Files
The tool generates two output files:

- **output_file.csv**: A CSV file containing the extracted metadata information for each table and its columns, including data types, lengths, scales, not-null constraints, and auto-increment attributes.
- **output_file_ddl.sql**: An SQL file containing the generated Oracle DDL statements for creating tables and indexes.

## Building
To build the project, use your favorite Java build tool, such as Maven or Gradle.

