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

Install [go][2], [fluent-bit][3], and [task][4]

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

Moreover, the plugin can assume a role by adding optional `role_arn` to your output configuration:
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
| `use_disk_buffer`   | Buffer logs on disk prior to sending to S3. See [Disk Buffering](#disk-buffering) for more info.         | `TRUE`            |
| `disk_buffer_path`  | Directory for disk buffer                                                                                | `tmp/out_clp_s3/` |
| `upload_size_mb`    | Set upload size in MB when disk store is enabled. Size refers to the compressed size.                    | `16`              |

#### Disk Buffering

The output plugin recieves raw logs from Fluent Bit in small chunks. With `use_disk_buffer` set, the
output plugin will accumulate logs on disk until the upload size is reached. Buffering logs will
reduce the amount of S3 API requests and improve the compression ratio. However, the plugin will use
disk space and have higher memory requirements. The amount of system resources will be proportional
to the amount of Fluent Bit tags. With `use_disk_buffer` off, the plugin will immediately process
each chunk and send it to S3.

Logs are stored on the disk as IR and Zstd compressed IR. If the plugin were to crash, stored logs
will be sent to S3 when Fluent Bit restarts. The upload index restarts on recovery.

### S3 Objects

Each upload will have a unique key in the following format:
```
<FLUENT_BIT_TAG>_<INDEX>_<UPLOAD_TIME_RFC3339>_<ID>.zst
```
The index starts at 0 is incremented after each upload. The Fluent Bit tag is also attached to the
object using the tag key `fluentBitTag`.

[1]: https://docs.fluentbit.io/manual/data-pipeline/parsers/json
[2]: https://go.dev/doc/install
[3]: https://docs.fluentbit.io/manual/installation/getting-started-with-fluent-bit
[4]: https://taskfile.dev/installation
[5]: https://docs.aws.amazon.com/sdk-for-go/v2/developer-guide/configure-gosdk.html#specifying-credentials
