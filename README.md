# onms-sink-syslog

A simple Kafka Client to parse messages sent by OpenNMS Minions using the Sink API for Syslog

# Compile

```bash
CGO_ENABLED=1 go build -tags static_all,netgo,musl .
```

To explicitly compile for Linux:

```bash
CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -tags static_all,netgo,musl .
```

