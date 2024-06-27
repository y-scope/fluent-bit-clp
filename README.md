# Fluent Bit output plugins for CLP

Repository contains Fluent Bit output plugins that store records in CLP's compressed IR 
(intermediate representation) format. More details on IR can be found in this [Uber Engineering Blog][1].

The general flow is as follows:

Raw Logs --> Fluent Bit --> Output Plugin --> IR --> Zstd --> Output


### Usage
Each plugin has its own README to help get started. Currently, we only have a 
[AWS S3 plugin](plugins/out_clp_s3/README.md), but please submit an issue if 
you need to send IR to another output. 

### Linting

1. Install golangci-lint:

```shell
curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | \
  sh -s -- -b $(go env GOPATH)/bin v1.59.0
```

2. Run with `golangci-lint run`

[1]: https://www.uber.com/en-US/blog/reducing-logging-cost-by-two-orders-of-magnitude-using-clp