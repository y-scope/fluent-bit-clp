// Package implements msgpack decoder. [output] already has a msgpack decoder; however, it will
// decode strings as []int8. This has two undesirable consequences.
//
//  1. Printing values with %v may output non-human readable arrays.
//
//  2. Strings in []int8 format marshalled to JSON will output non-human readable base64 encoded
//     strings.
//
// To solve these issues, all other plugins such as the [aws firehose plugin], have recursive
// functions which comb through decoded msgpack structures and convert bytes to strings (effectively
// another decoder). Modifying the decoder to output strings instead of bytes is cleaner, removes
// complex recursive functions, and likely more performant. [NewStringDecoder] interfaces with
// [output.GetRecord]; however, a type conversion is neccesary.
//
// [aws firehose plugin]: https://github.com/aws/amazon-kinesis-firehose-for-fluent-bit/blob/dcbe1a0191abd6242182af55547ccf99ee650ce9/plugins/plugins.go#L153
package decoder

import (
	"C"
	"reflect"
	"unsafe"

	"github.com/fluent/fluent-bit-go/output"
	"github.com/ugorji/go/codec"
)

// Redefined struct from fluent-bit-go. Unfortunately, unable to import directly from
// [output.FLBDecoder] since fields are defined privately.
type FLBDecoder struct {
	handle *codec.MsgpackHandle
	mpdec  *codec.Decoder
}

// Initializes a msgpack decoder which automatically converts bytes to strings.
//
// Parameters:
//   - data: msgpack data
//   - length: Byte length
//
// Returns:
//   - FLBDecoder: msgpack decoder
func NewStringDecoder(data unsafe.Pointer, length int) *output.FLBDecoder {
	var b []byte
	dec := new(FLBDecoder)
	dec.handle = new(codec.MsgpackHandle)
	dec.handle.RawToString = true
	dec.handle.WriteExt = true
	dec.handle.SetBytesExt(reflect.TypeOf(output.FLBTime{}), 0, &output.FLBTime{})

	b = C.GoBytes(data, C.int(length))
	dec.mpdec = codec.NewDecoderBytes(b, dec.handle)

	// For decoder to interace with [output.GetRecord], it must be type converted.
	// See [FLBDecoder] for reason why not using fluent-bit-go type.
	decFluentTyped := (*output.FLBDecoder)(unsafe.Pointer(dec))
	return decFluentTyped
}
