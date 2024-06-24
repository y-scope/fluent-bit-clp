// Package implements Msgpack decoder. Fluent Bit Go already has a Msgpack decoder; however, it
// will decode strings as []int8. This has two undesirable consequences.
//
//  1. Printing values with %v may output non-human readable arrays.
//
//  2. Strings in []int8 format marshalled to JSON will output non-human readable base64 encoded
//     strings.
//
// To solve these issues, all other plugins such as the [aws firehose plugin], have recursive
// functions which comb through decoded Msgpack structures and convert bytes to strings (effectively
// another decoder). Creating a new decoder to output strings instead of bytes is cleaner,
// removes complex recursive functions, and likely more performant.
//
// [aws firehose plugin]: https://github.com/aws/amazon-kinesis-firehose-for-fluent-bit/blob/dcbe1a0191abd6242182af55547ccf99ee650ce9/plugins/plugins.go#L153
package decoder

import (
	"C"
	"encoding/binary"
	"fmt"
	jsoniter "github.com/json-iterator/go"
	"reflect"
	"time"
	"unsafe"

	"github.com/ugorji/go/codec"
)

// Initializes a Msgpack decoder which automatically converts bytes to strings. Decoder has an
// extension setup for a custom Fluent Bit [timestamp format]. During [timestamp encoding],
// Fluent Bit will set the [Msgpack extension type] to "0". This decoder can recognize the
// extension type, and will then decode the custom Fluent Bit timestamp using a specific function
// [ReadExt].
//
// Parameters:
//   - data: Msgpack data
//   - length: Byte length
//
// Returns:
//   - decoder: Msgpack decoder
//
// [timestamp format]: https://github.com/fluent/fluent-bit-docs/blob/master/development/msgpack-format.md#fluent-bit-usage
// [timestamp encoding]: https://github.com/fluent/fluent-bit/blob/2138cee8f4878733956d42d82f6dcf95f0aa9339/src/flb_time.c#L237
// [Msgpack extension type]: https://github.com/msgpack/msgpack/blob/master/spec.md#extension-types
func New(data unsafe.Pointer, length int) *codec.Decoder {
	var b []byte
	var mh codec.MsgpackHandle

	// Decoder settings for string conversion and error handling.
	mh.RawToString = true
	mh.WriteExt = true
	mh.ErrorIfNoArrayExpand = true

	// Set up custom extension for Fluent Bit timestamp format.
	mh.SetBytesExt(reflect.TypeOf(FlbTime{}), 0, &FlbTime{})

	b = C.GoBytes(data, C.int(length))
	decoder := codec.NewDecoderBytes(b, &mh)
	return decoder
}

// Fluent-bit can encode timestamps in Msgpack [fixext 8] format. Format stores an integer and a
// byte array whose length is 8 bytes. The integer is the type, and the 4 MSBs are the seconds
// (big-endian uint32) and 4 LSBs are nanoseconds.
// [fixext 8]: https://github.com/msgpack/msgpack/blob/master/spec.md#ext-format-family
type FlbTime struct {
	time.Time
}

// Updates a value from a []byte.
//
// Parameters:
//   - i: Pointer to the registered extension type
//   - b: msgback data in fixext 8 format
func (f FlbTime) ReadExt(i interface{}, b []byte) {
	// Note that ts refers to the same object since i is a pointer.
	ts := i.(*FlbTime)
	sec := binary.BigEndian.Uint32(b)
	nsec := binary.BigEndian.Uint32(b[4:])
	ts.Time = time.Unix(int64(sec), int64(nsec))
}

// Function required by codec but not being used by decoder.
func (f FlbTime) WriteExt(interface{}) []byte {
	panic("unsupported")
}

// Function required by codec but not being used by decoder.
func (f FlbTime) ConvertExt(v interface{}) interface{} {
	return nil
}

// Function required by codec but not being used by decoder.
func (f FlbTime) UpdateExt(dest interface{}, v interface{}) {
	panic("unsupported")
}

// Retrieves data and timestamp from Msgpack object.
//
// Parameters:
//   - decoder: Msgpack decoder
//
// Returns:
//   - timestamp: Timestamp retrieved from Fluent Bit
//   - record: Structured record from Fluent Bit with variable amount of keys
//   - endOfStream: true if chunk finished
//   - err: error retrieving timestamp, error retrieving record, error marshalling record
func GetRecord(decoder *codec.Decoder) (interface{}, string, bool, error) {
	// Expect array of length 2 for timestamp and data.
	var m [2]interface{}
	err := decoder.Decode(&m)

	if err != nil {
		// If there is an error, it most likely means the chunk has no more data. Logic does not
		// catch other decoding errors.
		return nil, "", true, nil
	}

	// Timestamp is located in first index.
	t := m[0]
	var timestamp interface{}

	// Fluent Bit can provide timestamp in multiple formats, so we use type switch to process
	// correctly.
	switch v := t.(type) {
	// For earlier format [TIMESTAMP, MESSAGE].
	case FlbTime:
		timestamp = v
	case uint64:
		timestamp = v
	// For fluent-bit V2 metadata type of format [[TIMESTAMP, METADATA], MESSAGE].
	case []interface{}:
		if len(v) < 2 {
			err = fmt.Errorf("error decoding timestamp %v from stream", v)
			return nil, "",false, err
		}
		timestamp = v[0]
	default:
		err = fmt.Errorf("error decoding timestamp %v from stream", v)
		return nil,"",false, err
	}

	// Record is located in second index.
	record, ok := m[1].(map[interface{}]interface{})
	if !ok {
		err = fmt.Errorf("error decoding record %v from stream", record)
		return nil, "", false, err
	}

	// Marshall record to json string.
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	jsonRecord, err := json.MarshalToString(record)
	if err != nil {
		err = fmt.Errorf("failed to marshal record %v: %w", record, err)
		return nil, "", false, err
	}

	return timestamp, jsonRecord ,false, nil
}
