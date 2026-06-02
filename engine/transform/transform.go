package transform

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Srajan-Sanjay-Saxena/cdc-axon/engine/engine_source"
	"github.com/Srajan-Sanjay-Saxena/cdc-axon/engine/event"
)

func FilterByEventType(types ...string) engine_source.Transform {
	allowed := make(map[string]struct{}, len(types))
	for _, t := range types {
		allowed[t] = struct{}{}
	}
	return func(ctx context.Context, e event.Event) (event.Event, bool, error) {
		_, ok := allowed[e.EventType]
		return e, ok, nil
	}
}

func FilterByOperation(ops ...event.OperationType) engine_source.Transform {
	allowed := make(map[event.OperationType]struct{}, len(ops))
	for _, op := range ops {
		allowed[op] = struct{}{}
	}
	return func(ctx context.Context, e event.Event) (event.Event, bool, error) {
		_, ok := allowed[e.Operation]
		return e, ok, nil
	}
}

func AddHeader(key string, value []byte) engine_source.Transform {
	return func(ctx context.Context, e event.Event) (event.Event, bool, error) {
		if e.Headers == nil {
			e.Headers = make(map[string][]byte)
		}
		e.Headers[key] = value
		return e, true, nil
	}
}

func Deduplicate(store engine_source.PersistenceStore, ttl time.Duration) engine_source.Transform {
	if store == nil {
		panic("cdc-axon: Deduplicate requires a non-nil PersistenceStore")
	}
	return func(ctx context.Context, e event.Event) (event.Event, bool, error) {
		key := "dedup:" + e.ID
		data, err := store.Load(ctx, key)
		if err != nil {
			return e, false, err
		}
		if data != nil {
			ts := int64(binary.BigEndian.Uint64(data))
			if time.Since(time.Unix(0, ts)) < ttl {
				return e, false, nil
			}
		}
		now := make([]byte, 8)
		binary.BigEndian.PutUint64(now, uint64(time.Now().UnixNano()))
		if err := store.Save(ctx, key, now); err != nil {
			return e, false, err
		}
		return e, true, nil
	}
}

func RateLimit(eventsPerSecond int) engine_source.Transform {
	var mu sync.Mutex
	tokens := float64(eventsPerSecond)
	last := time.Now()
	max := float64(eventsPerSecond)

	return func(ctx context.Context, e event.Event) (event.Event, bool, error) {
		mu.Lock()
		now := time.Now()
		tokens += now.Sub(last).Seconds() * max
		if tokens > max {
			tokens = max
		}
		last = now
		if tokens < 1 {
			wait := time.Duration((1 - tokens) / max * float64(time.Second))
			mu.Unlock()
			select {
			case <-time.After(wait):
			case <-ctx.Done():
				return e, false, ctx.Err()
			}
			mu.Lock()
			tokens = 0
		} else {
			tokens--
		}
		mu.Unlock()
		return e, true, nil
	}
}

func RouteByEventType(routes map[string]string) engine_source.Transform {
	return func(ctx context.Context, e event.Event) (event.Event, bool, error) {
		if e.Headers == nil {
			e.Headers = make(map[string][]byte)
		}
		if dest, ok := routes[e.EventType]; ok {
			e.Headers["routing_key"] = []byte(dest)
		} else if def, ok := routes["default"]; ok {
			e.Headers["routing_key"] = []byte(def)
		}
		return e, true, nil
	}
}

func MaskField(fields ...string) engine_source.Transform {
	mask := make(map[string]struct{}, len(fields))
	for _, f := range fields {
		mask[f] = struct{}{}
	}
	return func(ctx context.Context, e event.Event) (event.Event, bool, error) {
		if e.Payload == nil {
			return e, true, nil
		}
		var m map[string]interface{}
		if err := json.Unmarshal(e.Payload, &m); err != nil {
			return e, true, nil
		}
		for field := range mask {
			if _, exists := m[field]; exists {
				m[field] = "***"
			}
		}
		masked, err := json.Marshal(m)
		if err != nil {
			return e, false, err
		}
		e.Payload = masked
		return e, true, nil
	}
}

func AddTimestamp() engine_source.Transform {
	return func(ctx context.Context, e event.Event) (event.Event, bool, error) {
		if e.Headers == nil {
			e.Headers = make(map[string][]byte)
		}
		now := make([]byte, 8)
		binary.BigEndian.PutUint64(now, uint64(time.Now().UnixNano()))
		e.Headers["captured_at"] = now
		return e, true, nil
	}
}

func EnrichFromStore(store engine_source.PersistenceStore, keyPattern string, headerKey string) engine_source.Transform {
	if store == nil {
		panic("cdc-axon: EnrichFromStore requires a non-nil PersistenceStore")
	}
	return func(ctx context.Context, e event.Event) (event.Event, bool, error) {
		key := fmt.Sprintf(keyPattern, e.ID)
		data, err := store.Load(ctx, key)
		if err != nil {
			return e, true, nil
		}
		if data != nil {
			if e.Headers == nil {
				e.Headers = make(map[string][]byte)
			}
			e.Headers[headerKey] = data
		}
		return e, true, nil
	}
}

func SampleRate(n int) engine_source.Transform {
	var counter atomic.Int64
	return func(ctx context.Context, e event.Event) (event.Event, bool, error) {
		c := counter.Add(1)
		return e, c%int64(n) == 0, nil
	}
}
