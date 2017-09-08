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

### Types of Response
Based on a request the response can be a result of query or status information.

| Type of response | Meaning               | Example                                                         |
| ---------------- | --------------------- | --------------------------------------------------------------- |
| DATA RESULT      | Next data result      | `{"value":1}`                                                   |
| OKAY             | Successfully executed | `{"status":"okay"}`                                             |
| INVALID          | Invalid request       | `{"status":"invalid", "message":"Description of the problem."}` |
| ERROR            | Error                 | `{"status":"error", "message":"Description of the error."}`     |

### Query Builder
Besides native String-based queries a intuitive Query-Builder could be used to prepare a query.

Example:
```
import cz.net21.ttulka.thistledb.client.Query;
// ...
Query insertQuery = Query.builder()
                        .insertInto("test")
                        .values("{\"v\":1}")
                        .values("{\"v\":2}")
                        .build();
                        
client.executeCommand(insertQuery, null);
```
The snippet above has the same meaning as following:
```
String insertQuery = "INSERT INTO test VALUES {\"v\":1},{\"v\":2}";
client.executeCommand(insertQuery, null);
```

### Asynchronous Methods
Asynchronous methods are based on Reactive Streams.

| Method           | Callback           | Meaning                           |
| ---------------- | ------------------ | --------------------------------- |
| `executeQuery`   | `JsonPublisher`    | Executes a query asynchronously   |
| `executeCommand` | `Consumer<String>` | Executes a command asynchronously |

There are always overloaded variants for native String-based and Query-Builder-based queries.

#### JSON Publisher
Class `JsonPublisher` implements `org.reactivestreams.Publisher`. 

Besides the method for subscribing an object of class `org.reactivestreams.Subscriber` offers a convenient method for subscribing an object of the standard Java class `Consumer`:
```
public JsonPublisher subscribe(Consumer<String> onNext)
```
This methods create a subscriber based on the gotten consumer.

##### Serial vs Parallel
As default runs the JSON Publisher in serial mode but it can run in parallel as well:
```
jsonPublisher.parallel();
```
Publisher could be set back to the serial mode:
```
jsonPublisher.serial();
```

If run in **serial** the order of elements always matchs the order the elements are consumed from a source.
If run in **parallel** the order of elements is unpredictable.

Examples:
```
// Consider a collection `test` containing values from 1 to 5
JsonPublisher publisher = client.executeQuery("SELECT value FROM test");

publisher.subscribe(System.out::println);
```
Prints always:
```
{"value":1}
{"value":2}
{"value":3}
{"value":4}
{"value":5}
```
When the same code is changed to run in parallel:
```
publisher.parallel().subscribe(System.out::println);
```
The result could look like this (or similar):
```
{"value":2}
{"value":3}
{"value":5}
{"value":1}
{"value":4}
```

##### Waiting for the Publisher to be finished
We can wait for the publisher to be finished in the blocking mode using the method `await()`:
```
// Create a publisher
JsonPublisher publisher = client.executeQuery("SELECT * FROM test");

// Subscribe a consumer to the publisher
publisher.subscribe(System.out::println);

// Do some work meanwhile...
doSomeStuff();

// All work is done, let's wait for the publisher
publisher.await();

// The end
System.exit(0);
```

### Blocking Methods
| Method                   | Return type    | Meaning                          |
| ------------------------ | -------------- | -------------------------------- |
| `executeQueryBlocking`   | `List<String>` | Executes a query synchronously   |
| `executeCommandBlocking` | `String`       | Executes a command synchronously |

There are always overloaded variants for native String-based and Query-Builder-based queries.

## License

[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)
