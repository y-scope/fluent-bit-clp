# S3 CLP output plugin

Output plugin for Fluent Bit that sends records in CLP IR format to AWS S3.

### Getting Started

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

Run task to build a binary in the plugin directory
  ```shell
  task build
  ```
Change [plugin-config.conf](plugin-config.conf) to reference the plugin binary
  ```shell
  [PLUGINS]
      Path /<LOCAL_PATH>/out_clp_s3.so
  ```

Change the output section [fluent-bit.conf](fluent-bit.conf) to suit your needs. 
See [Plugin configuration](#plugin-configuration) for description of fields.
Note changing configuration files may break docker setup, so best to copy them first.

See below for an example:

 ```
[OUTPUT]
    s3_bucket myBucket
    role_arn arn:aws:iam::000000000000:role/accessToMyBucket
    match *
  ```

Run Fluent Bit
  ```shell
  fluent-bit -c fluent-bit-custom.conf
  ```

### Plugin configuration

| Key               | Description                                                                                              | Default         |
| ----------------- | -------------------------------------------------------------------------------------------------------- | --------------- |
| s3_region         | The AWS region of your S3 bucket                                                                         | us-east-1       |
| s3_bucket         | S3 bucket name. Just the name, no aws prefix neccesary.                                                  | None            |
| s3_bucket_prefix  | Bucket prefix path                                                                                       | logs/           |
| role_arn          | ARN of an IAM role to assume                                                                             | None            |
| id                | Name of output plugin                                                                                    | Random UUID     |
| use_single_key    | Output single key from Fluent Bit record. See [use single key](#use-single-key) for more info.           | TRUE            |
| allow_missing_key | Fallback to whole record if key is missing from log. If set to false, an error will be recorded instead. | TRUE            |
| single_key        | Value for single key                                                                                     | log             |
| time_zone         | Time zone of the log source, so that local times (non-unix timestamps) are handled correctly.            | America/Toronto |

#### Use Single Key

Output the value corresponding to this key, instead of the whole Fluent Bit record. It is 
recommended to set this to true. A Fluent Bit record is a JSON-like object, and while CLP 
can parse JSON into IR it is not recommended. Key is set with single_key and will typically
 be set to "log", the default Fluent Bit key for unparsed logs. If this is set to false, plugin
will parse the record as JSON.

[1]: https://go.dev/doc/install
[2]: https://docs.fluentbit.io/manual/installation/getting-started-with-fluent-bit
