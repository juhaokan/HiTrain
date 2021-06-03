package redis

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"io/ioutil"

	"github.com/golang/snappy"
)

func gzipEncode(in []byte, encodeType, compressLevel int) (interface{}, error) {
	var (
		buffer bytes.Buffer
		out    []byte
		err    error
	)
	if encodeType == Snappy || encodeType == SnappyBase64 {
		data := snappy.Encode(nil, in)
		if encodeType == Snappy {
			return data, nil
		}
		encodeString := base64.StdEncoding.EncodeToString(data)
		return encodeString, nil
	}
	writer, _ := gzip.NewWriterLevel(&buffer, compressLevel)
	_, err = writer.Write(in)
	if err != nil {
		writer.Close()
		return out, err
	}
	err = writer.Close()
	if err != nil {
		return out, err
	}
	if encodeType == GzipBase64 {
		encodeString := base64.StdEncoding.EncodeToString(buffer.Bytes())
		return encodeString, nil
	}

	return buffer.Bytes(), nil
}

func gzipDecode(in interface{}, encodeType int) ([]byte, error) {
	var decodeBytes []byte
	var out []byte
	if encodeType == GzipBase64 {
		decodeBytes, _ = base64.StdEncoding.DecodeString(in.(string))
	} else {
		decodeBytes = in.([]byte)
	}
	reader, err := gzip.NewReader(bytes.NewReader(decodeBytes))
	if err != nil {
		return out, err
	}
	defer reader.Close()

	return ioutil.ReadAll(reader)
}
