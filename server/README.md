# ThistleDB - Server

## Server Protocol

Server protocol is text-based.

### Executing a query
```
ACCEPTED
JSON result
[...]
FINISHED
new line
```

### Executing a command
```
ACCEPTED
OKAY
new line
```             

### Invalid input
```
ACCEPTED
INVALID feedback message
new line
```

### Error
```
ACCEPTED
ERROR error message
new line
```
