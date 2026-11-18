package schema

import (
	"time"

	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema/edge"
	"entgo.io/ent/schema/field"
)

// Queue holds the schema definition for the Queue entity.
type Queue struct {
	ent.Schema
}

// Fields of the Queue.
func (Queue) Fields() []ent.Field {
	return []ent.Field{
		field.Int("id"),
		field.Int64("guild_id"),
		field.Int64("title_id"), // TODO: add foreign key after creating titles table
		field.Int64("added_by"),
		field.Int("position"),
		field.Time("added_at").Default(time.Now),
	}
}

// Edges of the Queue.
func (Queue) Edges() []ent.Edge {
	return []ent.Edge{
		edge.To("guilds", Guild.Type).
			Annotations(entsql.OnDelete(entsql.Cascade)),
		edge.To("users", User.Type).
			Annotations(entsql.OnDelete(entsql.Cascade)),
	}
}
