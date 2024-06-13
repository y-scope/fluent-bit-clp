// Package contains constants.
package constant

const S3PluginName = "out_clp_s3"
const RawBytesFlushThreshold = 4*1024*1024
// IR buffer using [bytes.Buffer.Write] so it will dynamically adjust if undersizes
const IRBufferSize = RawBytesFlushThreshold
const DefaultEncoding = FourByteEncoding
const DefaultTimeZone = "America/Toronto"

