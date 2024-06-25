# S3 CLP output plugin

Output plugin for Fluent Bit that sends records in CLP IR format to AWS S3.

### Getting Started

You can start the plugin
- [Using Docker](#using-docker)
- [Using local setup](#using-local-setup)

#### Using Docker

First build the image
  ```shell
  docker build ../../ -t fluent-bit-clp -f Dockerfile
  ```

Start a container
  ```shell
  docker run -it -v $(pwd):/root/fluent-bit/logs:rw --rm fluent-bit-clp
  ```

 Dummy logs will be written to your current working directory.

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

Change [fluent-bit.conf](fluent-bit.conf) to suit your needs. 
See [Plugin configuration](#plugin-configuration) for description of fields.
Note changing configuration files may break docker setup, so best to copy them first.

Run Fluent Bit
  ```shell
  fluent-bit -c fluent-bit-custom.conf
  ```

### Plugin configuration

The following options must be configured in [fluent-bit.conf](fluent-bit.conf)
- `id`: Name of output
- `path`: Directory for output
- `file`: File name prefix. Plugin will generate many files and append a timestamp.
- `use_single_key`: Output the value corresponding to this key, instead of the whole Fluent Bit 
record. It is recommended to set this to true. A Fluent Bit record is a JSON-like object, and while 
CLP can parse JSON into IR it is not recommended. Key is set with `single_key` and
will typically be set to "log", the default Fluent Bit key for unparsed logs. If this is set to false, 
plugin will parse the record as JSON.
- `allow_missing_key`: Fallback to whole record if key is missing from log. If set to false, an error will
be recorded instead.
- `single_key`: Value for single key
- `time_zone`: Time zone of the source producing the log events, so that local times (any time
that is not a unix timestamp) are handled correctly.

See below for an example:

 ```
[OUTPUT]
    name out_clp_s3
    id dummy_metrics
    path ./
    file dummy
    use_single_key true
    allow_missing_key true
    single_key log
    time_zone America/Toronto
    match *
  ```

[1]: https://go.dev/doc/install
[2]: https://docs.fluentbit.io/manual/installation/getting-started-with-fluent-bit