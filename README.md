# ThistleDB - A Simple JSON Database

A simple JSON database based on the file-access with server-client approach. 

**!!! IN PROGRESS !!!**

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
Type your command/query or stop the server and exit the console by typing `quit`.

### Data Structures

ThistleDB is based on the file-access. All the data are persistent on a disk.

#### Collections

Collections are separated data spaces with schema-less structures.

A collection contains a set of documents.

#### Documents

Documents are JSON objects in a collection.

##### Elements

Elements are paths in a document.

Example: `patient.name` is an element addressing a patient name in the document:
```json
{
    "patient" : {
        "id" : "123456789",
        "name" : "John Smith"
    }
}
```

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
DELETE FROM collection_name [WHERE element=value [{AND|OR} element=value [...]]]  
```
#### Update a Document in a Collection
```
UPDATE collection_name SET element=value[,element=value [...]] [WHERE element=value [{AND|OR} element=value [...]]]  
```

## Client

ThistleDB provides a Java driver to build a client from a Java application.

Copy the Maven dependency into your project:
```xml
<dependency>
    <groupId>cz.net21.ttulka.thistledb</groupId>
    <artifactId>thistledb-client</artifactId>
    <version>1.0.0-SNAPSHOT</version>
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

## Server
Server could be started from the command line (see Get Started) or dynamically from a Java code.

Copy the Maven dependency into your project:
```xml
<dependency>
    <groupId>cz.net21.ttulka.thistledb</groupId>
    <artifactId>thistledb-server</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```
Create a server instance:
```
import cz.net21.ttulka.thistledb.server.Server;
// ...
Server server = new Server();
```
Start and stop the server:
```
server.start();
server.stop();
```

## Console

Console provides a remote access to the server from a command line.

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
Type your command/query or exit the console by typing `quit`.

## License

[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)
