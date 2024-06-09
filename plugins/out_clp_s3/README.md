# S3 CLP output plugin

Output plugin for fluent-bit that sends records in CLP IR format to AWS S3.

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

Change [fluent-bit.conf](fluent-bit.conf) to specify
- Id (output name),
- Path (output directory)
- File (output file name)

  ```shell
  [OUTPUT]
      name  out_clp_s3
      Id   <OUTPUT_NAME>
      Path <OUTPUT_DIRECTORY>
      File <OUTPUT_FILE_NAME>
      match *
  ```

  [1]: https://go.dev/doc/install
  [2]: https://docs.fluentbit.io/manual/installation/getting-started-with-fluent-bit






