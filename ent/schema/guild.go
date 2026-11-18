package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/schema/field"
)

// Guild holds the schema definition for the Guild entity.
type Guild struct {
	ent.Schema
}

// Fields of the Guild.
func (Guild) Fields() []ent.Field {
	return []ent.Field{
		field.Int64("id").Unique(),
		field.Text("name"),
		field.Time("created_at"),
		field.Time("left_at").Nillable(),
		field.Time("updated_at"),
	}
}

// Edges of the Guild.
func (Guild) Edges() []ent.Edge {
	return nil
}
