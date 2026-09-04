package stream

import (
	"context"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

/*
	The bson:"..." tags tell the Mongo driver "when decoding BSON, map this field name to this Go field". So:

	bson:"operationType" → picks up "insert" or "update" from the change event

	bson:"fullDocument" → the entire nested document (your actual outbox row)

	bson:"_id" → the _id field of your outbox document → becomes ID

	bson:"event_type" → your custom field → becomes EventType

	bson:"payload" → your JSON payload → becomes bson.M

	bson.M is just map[string]interface{} — a generic BSON map. It holds whatever is in your payload field without you needing to define a strict struct for it. Then in eventsManager.go you do json.Marshal(raw.FullDocument.Payload) to convert it to json.RawMessage for the unified Event.
*/
type ChangeEvent struct {
	OperationType string `bson:"operationType"`
	FullDocument  struct {
		ID        string `bson:"_id"`
		EventType string `bson:"event_type"`
		Payload   bson.M `bson:"payload"`
	} `bson:"fullDocument"`
}

func Open(ctx context.Context, collection *mongo.Collection, resumeToken bson.Raw) (*mongo.ChangeStream, error) {
	/*
		opts := options.ChangeStream().SetFullDocument(options.UpdateLookup)

		This is critical. By default, Mongo change streams for update events only send you the diff — what changed, not the full document. SetFullDocument(options.UpdateLookup) tells Mongo: "go fetch the full document after the update and include it in fullDocument". Without this, FullDocument would be empty on updates and you'd have nothing to publish.

		For insert events, Mongo always includes the full document by default — because the entire document was just created, there's no "diff" concept. So SetFullDocument(options.UpdateLookup) is a no-op for inserts, fullDocument is always populated.

		For update events, without UpdateLookup Mongo only sends you something like:

		{
			"operationType": "update",
			"updateDescription": {
				"updatedFields": { "status": "shipped" },
				"removedFields": []
			},
			"fullDocument": null  ← empty, useless
		}

		With UpdateLookup Mongo does an extra read — goes back to the collection, fetches the current state of the document, and stuffs it into fullDocument:

		{
			"operationType": "update",
			"fullDocument": {
				"_id": "evt-123",
				"event_type": "ORDER_UPDATED",
				"payload": { "orderId": 99, "status": "shipped" }
			}
		}

		So SetFullDocument(options.UpdateLookup) only actually does anything for updates. For inserts it's irrelevant. That's why the name is UpdateLookup — it's literally "go look up the full document after an update".


	*/
	opts := options.ChangeStream().SetFullDocument(options.UpdateLookup)
	if resumeToken != nil {
		opts.SetResumeAfter(resumeToken)
	}

	pipeline := mongo.Pipeline{
		bson.D{{Key: "$match", Value: bson.D{{Key: "operationType", Value: bson.D{{Key: "$in", Value: bson.A{"insert", "update"}}}}}}},
	}
	/*
		collection.Watch → opens a long-lived connection to Mongo
                 → Mongo keeps it open
                 → pushes change events as they happen
                 → cs.Next() unblocks each time one arrives
	*/

	return collection.Watch(ctx, pipeline, opts)
}
