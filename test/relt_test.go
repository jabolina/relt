package test

import (
	"relt/pkg/relt"
	"testing"
)

func TestRelt_StartAndStop(t *testing.T) {
	conf := relt.DefaultReltConfiguration()
	relt, err := relt.NewRelt(*conf)
	if err != nil {
		t.Fatalf("failed creating relt. %v", err)
	}
	relt.Close()
}
