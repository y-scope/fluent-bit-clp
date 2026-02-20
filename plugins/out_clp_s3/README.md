# Fluent Bit S3 output plugin for CLP

Fluent Bit output plugin that sends records in CLP's compressed IR format to AWS S3.

### Getting Started

First, confirm your AWS credentials are properly setup, see [AWS credentials](#AWS-credentials) for
information.

Next, change [fluent-bit.yaml](fluent-bit.yaml) to suit your needs. Note, if your logs are JSON, you should use the [Fluent Bit JSON parser][1] on your input.
See [Plugin configuration](#plugin-configuration) for description of output options.

See below for input and output examples:

```yaml
pipeline:
  inputs:
    - name: tail
      path: /var/log/app.json
      tag: app.json
      parser: json

  outputs:
    - name: out_clp_s3
      match: "*"
      s3_bucket: myBucket
```

Lastly start the plugin:

- [Using Docker](#using-docker)
- [Using local setup](#using-local-setup)

#### Using Docker

First build the image
  ```shell
  docker build ../../ -t fluent-bit-clp -f Dockerfile
  ```

Start a container
  ```shell
  docker run -it \
    -v ~/.aws/credentials:/root/.aws/credentials \
    -v ./fluent-bit.yaml:/fluent-bit/etc/fluent-bit.yaml \
    --rm fluent-bit-clp
  ```

Dummy logs will be written to your s3 bucket.

#### Using local setup

Install [go][2], [task][3], and [fluent-bit][4]

Download go dependencies
  ```shell
  go mod download
  ```

Run task to build a binary in the plugin directory
  ```shell
  task build
  ```

Run Fluent Bit
  ```shell
  fluent-bit -e ./out_clp_s3.so -c fluent-bit.yaml
  ```
### AWS Credentials

The plugin will look for credentials using the following hierarchy:
  1. Environment variables
  2. Shared configuration files
  3. If using ECS task definition or RunTask API, IAM role for tasks.
  4. If running on an Amazon EC2 instance, IAM role for Amazon EC2.

Moreover, the plugin can assume a role by adding optional `role_arn` to [fluent-bit.yaml](fluent-bit.yaml):
```yaml
role_arn: arn:aws:iam::000000000000:role/accessToMyBucket
```

More detailed information for specifying credentials from AWS can be found [here][5].

### Plugin configuration

| Key                 | Description                                                                                              | Default           |
|---------------------|----------------------------------------------------------------------------------------------------------|-------------------|
| `s3_region`         | The AWS region of your S3 bucket                                                                         | `us-east-1`       |
| `s3_bucket`         | S3 bucket name. Just the name, no aws prefix neccesary.                                                  | `None`            |
| `s3_bucket_prefix`  | Bucket prefix path                                                                                       | `logs/`           |
| `role_arn`          | ARN of an IAM role to assume                                                                             | `None`            |
| `id`                | Name of output plugin                                                                                    |  Random UUID      |
| `use_disk_buffer` | Buffer logs on disk. See [Disk Buffering](#disk-buffering) for more info. | `TRUE` |
| `disk_buffer_path` | Directory for disk buffer | `tmp/out_clp_s3/` |
| `upload_size_mb` | Set upload size in MB. Size refers to the compressed size. | `16` |

#### Disk Buffering

The output plugin recieves raw logs from Fluent Bit in small chunks and accumulates them in a compressed
buffer until the upload size is reached before sending to S3.

With `use_disk_buffer` set, logs are stored on disk as IR and Zstd compressed IR. On a graceful shutdown
or abrupt crash, stored logs will be sent to S3 when Fluent Bit restarts. For an abrupt crash, there is
a very small chance of data corruption if the plugin crashed mid write. The upload index restarts on
recovery.

With `use_disk_buffer` off, logs are stored in memory as Zstd compressed IR. On a graceful shutdown, the
plugin will attempt to upload any buffered data to S3 before Fluent Bit terminates it. On an abrupt
crash, in-memory data is lost.

### S3 Objects

Each upload will have a unique key in the following format:
```
<FLUENT_BIT_TAG>_<INDEX>_<UPLOAD_TIME_RFC3339>_<ID>.zst
```
The index starts at 0 is incremented after each upload. The Fluent Bit tag is also attached to the
object using the tag key `fluentBitTag`.

[1]: https://docs.fluentbit.io/manual/data-pipeline/parsers/json
[2]: https://go.dev/doc/install
[3]: https://taskfile.dev/installation
[4]: https://docs.fluentbit.io/manual/installation/getting-started-with-fluent-bit
[5]: https://docs.aws.amazon.com/sdk-for-go/v2/developer-guide/configure-gosdk.html#specifying-credentials
