package kv

import (
	"testing"

	kvpb "github.com/atharv-pdarshi/kvraft/proto/kvpb"
)

func put(k, v string) *kvpb.Command {
	return &kvpb.Command{Op: kvpb.Command_PUT, Key: k, Value: v}
}

func TestStorePutGet(t *testing.T) {
	s := NewStore()
	s.Apply(put("foo", "bar"))

	res := s.Apply(&kvpb.Command{Op: kvpb.Command_GET, Key: "foo"})
	if !res.Found || res.Value != "bar" {
		t.Fatalf("got (%q,%v), want (bar,true)", res.Value, res.Found)
	}
}

func TestStoreOverwrite(t *testing.T) {
	s := NewStore()
	s.Apply(put("k", "1"))
	s.Apply(put("k", "2"))
	if v, _ := s.Get("k"); v != "2" {
		t.Fatalf("got %q, want 2", v)
	}
}

func TestStoreDelete(t *testing.T) {
	s := NewStore()
	s.Apply(put("k", "v"))
	s.Apply(&kvpb.Command{Op: kvpb.Command_DELETE, Key: "k"})
	if _, ok := s.Get("k"); ok {
		t.Fatalf("key should be gone after delete")
	}
}

func TestStoreGetMissing(t *testing.T) {
	s := NewStore()
	res := s.Apply(&kvpb.Command{Op: kvpb.Command_GET, Key: "nope"})
	if res.Found {
		t.Fatalf("missing key reported as found")
	}
}

func TestEncodeRoundTrip(t *testing.T) {
	cmd := &kvpb.Command{Op: kvpb.Command_PUT, Key: "a", Value: "b", ClientId: "c1", Seq: 7}
	b, err := EncodeCommand(cmd)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeCommand(b)
	if err != nil {
		t.Fatal(err)
	}
	if got.Key != "a" || got.Value != "b" || got.ClientId != "c1" || got.Seq != 7 {
		t.Fatalf("round trip mismatch: %+v", got)
	}
}
