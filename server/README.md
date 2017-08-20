# ThistleDB - Server

Server is non-blocking.

## Server Protocol

Server protocol is text-based.

### Executing a query
```
ACCEPTED
JSON result
[...]
FINISHED
```

### Executing a command
```
ACCEPTED
OKAY
FINISHED
```             

### Invalid input
```
ACCEPTED
INVALID feedback message
```

### Error
```
ACCEPTED
ERROR error message
```
