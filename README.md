# MetaDataConverter

MetadataConverter is a Java command-line tool that extracts metadata 
from DB2/AS400 databases (at the moment) 
and generates Oracle DDL and CSV files containing metadata and index information for the specified tables.

### Full tool menu 
- Start process metadata to DDL/.csv file.
- Test connection to server.
- Insert created DDL file to database schema.(ORACLE)
- Compare schema(Oracle) to schema(Oracle) and show diffs in file.

## Usage

For linux :
```bash
java -jar MetaDataConverter.jar
```


## Output Files
The tool generates two output files:

- **output_file.csv**: A CSV file containing the extracted metadata information for each table and its columns, including data types, lengths, scales, not-null constraints, and auto-increment attributes.
- **output_file_ddl.sql**: An SQL file containing the generated Oracle DDL statements for creating tables and indexes.

## Parameters
Whole project connections can be configured when project is started. (Except part with "Compare schema(Oracle) to schema(Oracle) and show diffs in file." This one is in code right now.) 

## Building
To build the project, use your favorite Java build tool, such as Maven or Gradle.

# Future functionality
### Extract Metadata from:
- Oracle
- MySQL
- SQL
- DB2 **Done**
- AS400 **Done**
### Create Metadata DDL to:
- Oracle **Done**
- MySQL
- SQL
- DB2
- AS400
### Compare any type database schema to any type database schema.
### Select DDL file, extract tables, delete tables from schema and insert from DDL.
### Extracted table as an object.
