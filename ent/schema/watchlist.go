package schema

import (
	"time"

	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema/edge"
	"entgo.io/ent/schema/field"
)

// WatchList holds the schema definition for the WatchList entity.
type WatchList struct {
	ent.Schema
}

// Fields of the WatchList.
func (WatchList) Fields() []ent.Field {
	return []ent.Field{
		field.Int("id").Unique().Immutable(),
		field.Int64("user_id").Unique(),
		field.Int64("title_id").Unique(), // TODO: add title_id foreign key after adding titles table
		field.Time("added_at").Default(time.Now),
	}
}

// Edges of the WatchList.
func (WatchList) Edges() []ent.Edge {
	return []ent.Edge{
		edge.To("users", User.Type).
			Annotations(entsql.OnDelete(entsql.Cascade)),
	}
}
