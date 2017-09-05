# ThistleDB - Java Client 

ThistleDB provides a Java driver to build a client from a Java application.

Client implements [Reactive Streams, Version 1.0.0](http://www.reactive-streams.org).

## Create the client 

Copy the Maven dependency into your project:
```xml
<dependency>
    <groupId>cz.net21.ttulka.thistledb</groupId>
    <artifactId>thistledb-client</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```
There are different constructors to create a client:
```
import cz.net21.ttulka.thistledb.client.Client;
// ...
Client client = new Client();                   // default port on localhost 
Client client = new Client(9658);               // custom port on localhost
Client client = new Client("localhost");        // default port on a custom host
Client client = new Client("localhost", 9658);  // custom port and host
```

### Setting timeout for a response
Timeout for the client response must be greater than zero, zero means no timeout.
Timeout value is in milliseconds.
```
int timeout = 1000;
client.setTimeout(timeout);
```

## Close the client 
```
client.close();
```
Client implement `AutoCloseable` interface:
```
try (Client client = new Client()) {
    // work with the client
}
```

## Querying

There are two types of requests: queries and commands.

**Queries** ask the database for data response:
- SELECT

**Commands** ask for an executing and awaiting only one status response:
- CREATE
- DROP
- INSERT
- UPDATE
- DELETE
- CREATE INDEX
- DROP INDEX

### Types of response
Based on a request the response can be a result of query or status information.

| Type of response | Meaning               | Example                                                         |
| ---------------- | --------------------- | --------------------------------------------------------------- |
| DATA RESULT      | Next data result      | `{"value":1}`                                                   |
| OKAY             | Successfully executed | `{"status":"okay"}`                                             |
| INVALID          | Invalid request       | `{"status":"invalid", "message":"Description of the problem."}` |
| ERROR            | Error                 | `{"status":"error", "message":"Description of the error."}`     |

### Asynchronous methods

#### JSON Publisher

### Blocking methods

## License

[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)
