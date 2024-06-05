# fluent-bit-clp

Repository contains CLP output plugins for fluent-bit that store records in CLP IR format.

### Linting

1. Install golangci-lint:

```shell
curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | \
  sh -s -- -b $(go env GOPATH)/bin v1.59.0
```

2. Run with `golangci-lint run`
