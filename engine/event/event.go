package event

import "encoding/json"

//Id : Unique identifier for the event, can be a UUID or a combination of source and timestamp
//Source : Table name or collection name from which the event originated
//Operation : Type of database operation (insert, update, delete)
//EventType : A string to categorize the event, e.g., "outbox.insert"
//Payload : The actual data of the event, stored as JSON for flexibility

type Event struct {
	ID        string
	Source    string
	Operation OperationType
	EventType string
	Payload   json.RawMessage
}

type OperationType string

const (
	INSERT OperationType = "insert"
	UPDATE OperationType = "update"
	DELETE OperationType = "delete"
)