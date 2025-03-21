package internal

import (
	"cmp"
	"context"
	"log/slog"
	"slices"
	"sync"

	"github.com/linkedin/diderot/ads"
	"github.com/linkedin/diderot/internal/utils"
	serverstats "github.com/linkedin/diderot/stats/server"
	"golang.org/x/time/rate"
	"google.golang.org/protobuf/proto"
)

func NewDeltaHandler(
	ctx context.Context,
	granularLimiter *rate.Limiter,
	globalLimiter *rate.Limiter,
	statsHandler serverstats.Handler,
	maxChunkSize int,
	typeUrl string,
	send func(res *ads.DeltaDiscoveryResponse) error,
) BatchSubscriptionHandler {
	return newDeltaHandler(
		ctx,
		(*rateLimiterWrapper)(granularLimiter),
		(*rateLimiterWrapper)(globalLimiter),
		statsHandler,
		maxChunkSize,
		typeUrl,
		send,
	)
}

func newDeltaHandler(
	ctx context.Context,
	granularLimiter handlerLimiter,
	globalLimiter handlerLimiter,
	statsHandler serverstats.Handler,
	maxChunkSize int,
	typeURL string,
	send func(res *ads.DeltaDiscoveryResponse) error,
) *handler {
	ds := &deltaSender{
		ctx:          ctx,
		typeURL:      typeURL,
		maxChunkSize: maxChunkSize,
		statsHandler: statsHandler,
		minChunkSize: initialChunkSize(typeURL),
	}

	return newHandler(
		ctx,
		granularLimiter,
		globalLimiter,
		statsHandler,
		false,
		func(entries map[string]*ads.RawResource) error {
			for i, chunk := range ds.chunk(entries) {
				if i > 0 {
					// Respect the global limiter in between chunks
					err := waitForGlobalLimiter(ctx, globalLimiter, statsHandler)
					if err != nil {
						return err
					}
				}
				err := send(chunk)
				if err != nil {
					return err
				}
			}
			return nil
		},
	)
}

type queuedResourceUpdate struct {
	Name string
	Size int
}

type deltaSender struct {
	ctx          context.Context
	typeURL      string
	statsHandler serverstats.Handler
	// The maximum size (in bytes) that a chunk can be. This is determined by the client as anything
	// larger than this size will cause the message to be dropped.
	maxChunkSize int

	// The minimum size an encoded chunk will serialize to, in bytes. Used to check whether a given
	// update can _ever_ be sent, and as the initial size of a chunk. Note that this value only depends
	// on utils.NonceLength and the length of typeURL.
	minChunkSize int
}

var queuedUpdatesPool = sync.Pool{}

// newQueue creates a new []queuedResourceUpdate with at least enough capacity to hold the required
// size. Note that this returns a pointer to a slice instead of a slice to avoid heap allocations.
// This is the recommended way to use a [sync.Pool] with slices.
func newQueue(size int) *[]queuedResourceUpdate {
	// Attempt to get a queue from the pool. If it returns nil, ok will be false meaning the pool was
	// empty.
	queue, ok := queuedUpdatesPool.Get().(*[]queuedResourceUpdate)
	if ok && cap(*queue) >= size {
		*queue = (*queue)[:0]
	} else {
		if ok {
			// Return the queue that was too short to the pool
			queuedUpdatesPool.Put(queue)
		}
		queue = new([]queuedResourceUpdate)
		*queue = make([]queuedResourceUpdate, 0, size)
	}
	return queue
}

func (ds *deltaSender) chunk(resourceUpdates map[string]*ads.RawResource) (chunks []*ads.DeltaDiscoveryResponse) {
	queuePtr := newQueue(len(resourceUpdates))
	defer queuedUpdatesPool.Put(queuePtr)

	queue := *queuePtr
	for name, e := range resourceUpdates {
		queue = append(queue, queuedResourceUpdate{
			Name: name,
			Size: encodedUpdateSize(name, e),
		})
	}
	// Sort the updates in descending order
	slices.SortFunc(queue, func(a, b queuedResourceUpdate) int {
		return -cmp.Compare(a.Size, b.Size)
	})

	// This nested loop builds the fewest possible chunks it can from the given resourceUpdates map. It
	// implements an approximation of the bin-packing algorithm called next-fit-decreasing bin-packing
	// https://en.wikipedia.org/wiki/Next-fit-decreasing_bin_packing
	idx := 0
	for idx < len(queue) {
		// This chunk will hold all the updates for this loop iteration
		chunk := ds.newChunk()
		chunkSize := proto.Size(chunk)

		for ; idx < len(queue); idx++ {
			update := queue[idx]
			r := resourceUpdates[update.Name]

			if ds.maxChunkSize > 0 {
				if ds.minChunkSize+update.Size > ds.maxChunkSize {
					// This condition only occurs if the update can never be sent, i.e. it is too large and will
					// always be dropped by the client. It should therefore be skipped altogether, but flagged
					// accordingly.
					if ds.statsHandler != nil {
						ds.statsHandler.HandleServerEvent(ds.ctx, &serverstats.ResourceOverMaxSize{
							Resource:        r,
							ResourceSize:    update.Size,
							MaxResourceSize: ds.maxChunkSize,
						})
					}
					slog.ErrorContext(
						ds.ctx,
						"Cannot send resource update because it is larger than configured max delta response size",
						"maxDeltaResponseSize", ds.maxChunkSize,
						"name", update.Name,
						"updateSize", update.Size,
						"resource", r,
					)
					continue
				}
				if chunkSize+update.Size > ds.maxChunkSize {
					// This update it too large to be sent along with the current chunk, skip it for now and
					// attempt it in the next chunk.
					break
				}
			}

			if r != nil {
				chunk.Resources = append(chunk.Resources, r)
			} else {
				chunk.RemovedResources = append(chunk.RemovedResources, update.Name)
			}
			// Add the resource since it is small enough to be added to the chunk
			chunkSize += update.Size
		}

		chunks = append(chunks, chunk)
	}

	if len(chunks) > 1 {
		slog.WarnContext(
			ds.ctx,
			"Response exceeded max response size, sent in chunks",
			"chunks", len(chunks),
			"typeURL", ds.typeURL,
			"updates", len(queue),
		)
		for i, c := range chunks {
			c.Nonce = utils.NewNonce(len(chunks) - i - 1)
		}
	} else {
		chunks[0].Nonce = utils.NewNonce(0)
	}

	return chunks
}

func (ds *deltaSender) newChunk() *ads.DeltaDiscoveryResponse {
	return &ads.DeltaDiscoveryResponse{
		TypeUrl: ds.typeURL,
	}
}

const protobufSliceOverhead = 2

func initialChunkSize(typeUrl string) int {
	return protobufSliceOverhead + len(typeUrl) + protobufSliceOverhead + utils.NonceLength
}

// encodedUpdateSize returns the amount of bytes it takes to encode the given update in an *ads.DeltaDiscoveryResponse.
func encodedUpdateSize(name string, r *ads.RawResource) int {
	resourceSize := protobufSliceOverhead
	if r != nil {
		resourceSize += proto.Size(r)
	} else {
		resourceSize += len(name)
	}
	return resourceSize
}
