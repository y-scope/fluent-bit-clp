# Fluent Bit S3 output plugin for CLP

Fluent Bit output plugin that sends records in CLP's compressed IR format to AWS S3.

### Getting Started

First, confirm your AWS credentials are properly setup, see [AWS credentials](#AWS-credentials) for
information.

Next, change the output section [fluent-bit.conf](fluent-bit.conf) to suit your needs.
See [Plugin configuration](#plugin-configuration) for description of fields.

See below for an example:

 ```
[OUTPUT]
    name  out_clp_s3
    s3_bucket myBucket
    match *
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
  docker run -it -v ~/.aws/credentials:/root/.aws/credentials --rm fluent-bit-clp
  ```

Dummy logs will be written to your s3 bucket.

#### Using local setup

Install [go][1] and [fluent-bit][2]

Download go dependencies
  ```shell
  go mod download
  ```

Run task to build a binary in the plugin directory
  ```shell
  task build
  ```
Change [plugin-config.conf](plugin-config.conf) to reference the plugin binary
  ```shell
  [PLUGINS]
      Path /<LOCAL_PATH>/out_clp_s3.so
  ```
Note changing this path may break docker setup. To preserve docker setup, copy
[plugin-config.conf](plugin-config.conf) and change `plugins_file` in
[fluent-bit.conf](fluent-bit.conf) to new file name.

Run Fluent Bit
  ```shell
  fluent-bit -c fluent-bit.conf
  ```
### AWS Credentials

The plugin will look for credentials using the following hierarchy:
  1. Environment variables
  2. Shared configuration files
  3. If using ECS task definition or RunTask API, IAM role for tasks.
  4. If running on an Amazon EC2 instance, IAM role for Amazon EC2.

Moreover, the plugin can assume a role by adding optional `role_arn` to
[plugin-config.conf](plugin-config.conf). Example shown below:
```
role_arn arn:aws:iam::000000000000:role/accessToMyBucket
```

More detailed information for specifying credentials from AWS can be found [here][3].

### Plugin configuration

| Key                 | Description                                                                                              | Default           |
|---------------------|----------------------------------------------------------------------------------------------------------|-------------------|
| `s3_region`         | The AWS region of your S3 bucket                                                                         | `us-east-1`       |
| `s3_bucket`         | S3 bucket name. Just the name, no aws prefix neccesary.                                                  | `None`            |
| `s3_bucket_prefix`  | Bucket prefix path                                                                                       | `logs/`           |
| `role_arn`          | ARN of an IAM role to assume                                                                             | `None`            |
| `id`                | Name of output plugin                                                                                    |  Random UUID      |
| `use_single_key`    | Output single key from Fluent Bit record. See [Use Single Key](#use-single-key) for more info.           | `TRUE`            |
| `allow_missing_key` | Fallback to whole record if key is missing from log. If set to false, an error will be recorded instead. | `TRUE`            |
| `single_key`        | Value for single key                                                                                     | `log`             |
| `use_disk_buffer`   | Buffer logs on disk prior to sending to S3. See [Disk Buffering](#disk-buffering) for more info.         | `TRUE`            |
| `store_dir`         | Directory for disk store                                                                                 | `tmp/out_clp_s3/` |
| `upload_size_mb`    | Set upload size in MB when disk store is enabled. Size refers to the compressed size.                    | `16`              |
| `time_zone`         | Time zone of the log source, so that local times (non-unix timestamps) are handled correctly.            | `America/Toronto` |

#### Use Single Key

Output the value corresponding to this key, instead of the whole Fluent Bit record. It is
recommended to set this to true. A Fluent Bit record is a JSON-like object, and while CLP
can parse JSON into IR it is not recommended. Key is set with `single_key` and will typically be set
to `log`, the default Fluent Bit key for unparsed logs. If this is set to false, plugin will parse
the record as JSON.

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

[1]: https://go.dev/doc/install
[2]: https://docs.fluentbit.io/manual/installation/getting-started-with-fluent-bit
[3]: https://aws.github.io/aws-sdk-go-v2/docs/configuring-sdk/#specifying-credentials
