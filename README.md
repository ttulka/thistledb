# ThistleDB - A Simple JSON Database

A simple JSON database based on the files access. 

!!! IN PROGRESS !!!

## Get Started

### Data Structures

#### Collections

Collections are separated data spaces with schema-less structures.

#### Documents

##### Elements

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
INSERT INTO collection_name VALUES json_document 
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

## Server

## Client

## License

[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)
