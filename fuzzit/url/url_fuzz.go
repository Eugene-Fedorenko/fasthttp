// +build gofuzz

package fuzzit

import (
	"bytes"

	"github.com/eugene-fedorenko/fasthttp"
)

func Fuzz(data []byte) int {
	u := fasthttp.AcquireURI()
	defer fasthttp.ReleaseURI(u)

	u.UpdateBytes(data)

	w := bytes.Buffer{}
	if _, err := u.WriteTo(&w); err != nil {
		return 0
	}

	return 1
}
