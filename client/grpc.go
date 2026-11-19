package client

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	kvpb "github.com/atharv-pdarshi/kvraft/proto/kvpb"
)

// grpcEndpoint adapts a kvpb.KVClient to the Endpoint interface (dropping the
// variadic call options so it matches *kv.Server's signatures).
type grpcEndpoint struct {
	conn   *grpc.ClientConn
	client kvpb.KVClient
}

func (e *grpcEndpoint) Put(ctx context.Context, req *kvpb.PutRequest) (*kvpb.PutResponse, error) {
	return e.client.Put(ctx, req)
}
func (e *grpcEndpoint) Get(ctx context.Context, req *kvpb.GetRequest) (*kvpb.GetResponse, error) {
	return e.client.Get(ctx, req)
}
func (e *grpcEndpoint) Delete(ctx context.Context, req *kvpb.DeleteRequest) (*kvpb.DeleteResponse, error) {
	return e.client.Delete(ctx, req)
}
func (e *grpcEndpoint) Status(ctx context.Context) (*kvpb.StatusResponse, error) {
	return e.client.GetStatus(ctx, &kvpb.StatusRequest{})
}

// Cluster holds the dialed endpoints of a real gRPC cluster.
type Cluster struct {
	endpoints map[string]*grpcEndpoint
	ids       []string
}

// Dial parses an "id=addr,id=addr,..." spec and dials every node.
func Dial(spec string) (*Cluster, error) {
	peers, err := ParsePeers(spec)
	if err != nil {
		return nil, err
	}
	c := &Cluster{endpoints: make(map[string]*grpcEndpoint)}
	for id, addr := range peers {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, fmt.Errorf("dial %s (%s): %w", id, addr, err)
		}
		c.endpoints[id] = &grpcEndpoint{conn: conn, client: kvpb.NewKVClient(conn)}
		c.ids = append(c.ids, id)
	}
	sort.Strings(c.ids)
	return c, nil
}

// Client builds a retrying Client over the dialed cluster.
func (c *Cluster) Client(clientID string) *Client {
	eps := make(map[string]Endpoint, len(c.endpoints))
	for id, e := range c.endpoints {
		eps[id] = e
	}
	return New(clientID, eps)
}

// IDs returns the sorted node ids.
func (c *Cluster) IDs() []string { return c.ids }

// Status fetches one node's status.
func (c *Cluster) Status(ctx context.Context, id string) (*kvpb.StatusResponse, error) {
	e, ok := c.endpoints[id]
	if !ok {
		return nil, fmt.Errorf("unknown node %q", id)
	}
	return e.Status(ctx)
}

// Close shuts all connections.
func (c *Cluster) Close() {
	for _, e := range c.endpoints {
		e.conn.Close()
	}
}

// ParsePeers parses "id=host:port,id=host:port" into a map.
func ParsePeers(spec string) (map[string]string, error) {
	out := map[string]string{}
	for _, part := range strings.Split(spec, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("bad peer spec %q (want id=host:port)", part)
		}
		out[strings.TrimSpace(kv[0])] = strings.TrimSpace(kv[1])
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no peers in %q", spec)
	}
	return out, nil
}
