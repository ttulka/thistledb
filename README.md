# ThistleDB - Simple JSON Database

Simple JSON database based on the **file-access** and **server-client** approach with the *non-blocking* server and the *reactive* (asynchronous non-blocking) client.

## Prerequisites

- Java 8
- Maven 

## Get Started

First, clone or fork the repository:
```
$ git clone git@github.com:ttulka/thistledb.git
$ cd thistledb
```
Now build it and assemble:
```
$ mvn clean install -P localBuild
$ mvn package appassembler:assemble --file server-app/pom.xml
```
And finally run it: 
```
$ cd server-app/target/server/bin
$ server
```
Type your command/query ending with `;` or stop the server and exit the console by typing `quit`.

### Data Structures
ThistleDB is based on the file-access. All the data are persistent on a disk.

#### Collections
Collections are separated data spaces with schema-less structures.

A collection contains a set of documents.

Collection name can contain only literals, digits and underscore `_`. 

#### Documents
Documents are JSON objects in a collection.

##### Elements
Elements are paths in a document.

Example (`patient.name` is an element addressing a patient name in the document):
```json
{
    "patient" : {
        "id" : "123456789",
        "name" : "John Smith"
    }
}
```
Name of an element can contain literals, digits, underscore `_` and dash `-`. 

Example of valid element names:
```
name
123
patient-name_123
```
##### Values
Values could be strings, numbers, boolean literals and `null`.

### Commands and Queries
All the commands and queries are SQL-like.

#### Create a Collection
```
CREATE collection_name 
```
#### Drop a Collection
```
DROP collection_name 
```
#### Add an Element to Documents in a Collection
```
ALTER collection_name ADD element [WHERE element op value [{AND|OR} element op value [...]]]  
```
#### Remove an Element from Documents in a Collection
```
ALTER collection_name REMOVE element [WHERE element op value [{AND|OR} element op value [...]]]  
```
#### Insert a Document into a Collection
```
INSERT INTO collection_name VALUES json_document[,json_document[...]]
```
#### Select a Document from a Collection
```
SELECT {*|element[,element[...]]} FROM collection_name [WHERE element=value [{AND|OR} element=value [...]]]  
```
#### Delete a Document from a Collection
```
DELETE FROM collection_name [WHERE element op value [{AND|OR} element op value [...]]]  
```
#### Update a Document in a Collection
```
UPDATE collection_name SET element=value[,element=value [...]] [WHERE element op value [{AND|OR} element op value [...]]]  
```
Element is updated only when exists.

### Operators
| Operator   | Meaning                | Note                                                       | 
| ---------- | ---------------------- | ---------------------------------------------------------- |
| `=`        | Equal                  |                                                            |
| `!=`       | Not equal              |                                                            |
| `>`        | Greater                |                                                            |
| `>=`       | Greater or equal       |                                                            |
| `<`        | Less                   |                                                            |
| `<=`       | Less or equal          |                                                            |
| `LIKE`     | Equal by an expression | `*` any string, `_` one character, `?` any character (0,1) |

### Indexes 
For accelerating the speed of searching can be used indexes on a collection.

Only simple-value elements (numbers, strings, ...) can be indexed.
Indexes are applied only on conditions with the equals operator `=`.

As usual, indexes accelerate reading but degrading speed of data modifications - use them cleverly!  

#### Create an Index for a Collection
```
CREATE INDEX indexed_element ON collection_name  
```
#### Drop an Index for a Collection
```
DROP INDEX indexed_element ON collection_name  
```

### DUAL Collection
DUAL collection is an immutable system "echo" collection which returns what it gets.

| Query                    | Result                |
| ------------------------ | --------------------- |
| `SELECT 123 FROM dual`   | `{ "value" : 123 }`   |
| `SELECT 1.23 FROM dual`  | `{ "value" : 1.23 }`  |
| `SELECT true FROM dual`  | `{ "value" : true }`  |
| `SELECT "abc" FROM dual` | `{ "value" : "abc" }` |

There are special element to be returned.

| Query                   | Result                                    | Example                                  |
| ----------------------- | ----------------------------------------- | ---------------------------------------- |
| `SELECT * FROM dual`      | *empty*                                 | `{}`                                     |
| `SELECT name FROM dual`   | name of the collection                  | `{ "name" : "DUAL" }`                    |
| `SELECT random FROM dual` | a random integer                        | `{ "random" : -980456651 }`              |
| `SELECT date FROM dual`   | datetime in format `yyyy-mm-dd H:m:s.ms`| `{ "date" : "2017-02-19 14:25:02.122" }` |

## Client

ThistleDB provides a *Java driver* to build a client from a Java application.

Client implements [Reactive Streams, Version 1.0.0](http://www.reactive-streams.org).

Client is not thread-safe, to ensure concurrency must be run within a synchronized context.

Copy the Maven dependency into your project:
```xml
<dependency>
    <groupId>cz.net21.ttulka.thistledb</groupId>
    <artifactId>thistledb-client</artifactId>
    <version>1.0.0</version>
</dependency>
```
Open a client connection:
```
import cz.net21.ttulka.thistledb.client.Client;
// ...
Client client = new Client("localhost", 9658);
```
Create a collection:
```
client.executeCommand("CREATE test");
```
Put a document into the collection:
```
String json = "{\"patient\" : {\"id\" : \"123456789\", \"name\" : \"John Smith\"} }";
client.executeCommand("INSERT INTO test VALUES " + json);
```
Select the document from the collection (blocking):
```
String query = "SELECT * FROM test WHERE patient.id='123456789'";
List<String> result = client.executeQueryBlocking(query);

result.forEach(json -> System.out.println(json));
```
Select the document from the collection (non-blocking):
```
client.executeQuery(query).subscribe(System.out::println);
```
Close the client:
```
client.close();
```

## Server

Default server port is **9658**. 

Default data folder is `data`, the path relative to the executing directory.

Maximum client connections the server accepts is **20**.

Server can be started from the command-line or dynamically from a Java code.

### Starting from a Command-Line
After downloading binaries or compiling from the source code (see Get Started) run the server from the command-line:
```
$ cd server-app/target/server/bin
$ server
```

#### Changing the Default Port
```
-p, --port <port>
```
#### Changing the Default Data Folder
```
-d, --dataDir <path>
```
#### Maximum Client Connections
```
-m, --maxConnections <maximum>
```
#### Caching 
```
-c, --cacheExpirationTime <minutes>
```
Caching is active only together with indexes. Default value is 20 minutes, zero value means no caching.

### Starting from a Java Code
Copy the Maven dependency into your project:
```xml
<dependency>
    <groupId>cz.net21.ttulka.thistledb</groupId>
    <artifactId>thistledb-server</artifactId>
    <version>...</version>
</dependency>
```

#### Create a server instance
For creating a new server object use `ServerBuilder`:
```
Server.ServerBuilder builder = Server.builder();
Server server = builder.build();
```
Set the port:
```
int port = 1234;
Server server = Server.builder().port(port).build();
```
Set the data folder:
```
Path dataFolder = java.nio.file.Paths.get("/data");
Server server = Server.builder().dataFolder(dataFolder).build();
```
Set the cache expiration time (in minutes):
```
int cacheExpirationTime = 0;    // zero means cache is disabled
Server server = Server.builder().cacheExpirationTime(cacheExpirationTime).build();
```
Setters can be mixed as wanted:
```
Server server = Server.builder().port(1234).cacheExpirationTime(5).build();
```
Maximum client connections can be changed by a setter:
```
int maxClientConnections = 10;
server.setMaxClientConnections(maxClientConnections);
```
#### Start and stop the server
```
server.start();
server.stop();
```

## Console

Console provides a remote access to the server from a command-line.

First, build it and assemble:
```
$ mvn clean install -P localBuild
$ mvn package appassembler:assemble --file console-app/pom.xml
```
And run it: 
```
$ cd console-app/target/console/bin
$ console
```
Type your command/query ending with `;` or exit the console by typing `quit`.

## License

[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)
