package test

import (
	"relt/pkg/relt"
	"testing"
)

func TestRelt_StartAndStop(t *testing.T) {
	conf := relt.DefaultReltConfiguration()
	relt := relt.NewRelt(*conf)
	relt.Close()
}
