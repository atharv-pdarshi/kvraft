package tests

import kvpb "github.com/atharv-pdarshi/kvraft/proto/kvpb"

// makePut builds a PutRequest with explicit client id and seq, so a test can
// replay the exact same logical write to exercise server-side dedup.
func makePut(key, value, clientID string, seq uint64) *kvpb.PutRequest {
	return &kvpb.PutRequest{Key: key, Value: value, ClientId: clientID, Seq: seq}
}
