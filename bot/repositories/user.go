package repositories

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"arikawa-discord/ent"
	"arikawa-discord/ent/user"
)

type UserRepo struct {
	db *ent.Client
}

func CreateUserRepo(db *ent.Client) *UserRepo {
	return &UserRepo{db: db}
}

// parseSnowflake converts a Discord id string to its int64 value.
func parseSnowflake(raw string) (int64, error) {
	id, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || id < 0 {
		return 0, fmt.Errorf("discord id %q is not a snowflake: %w", raw, err)
	}
	return id, nil
}

// Ensure creates the user on first interaction and refreshes the username cache.
func (r *UserRepo) Ensure(ctx context.Context, discordUserID, username string) (*ent.User, error) {
	id, err := parseSnowflake(discordUserID)
	if err != nil {
		return nil, err
	}

	existing, err := r.db.User.Query().Where(user.ID(id)).Only(ctx)
	if ent.IsNotFound(err) {
		now := time.Now()
		return r.db.User.Create().
			SetID(id).
			SetUserID(discordUserID).
			SetUsername(username).
			SetCreatedAt(now).
			SetUpdatedAt(now).
			Save(ctx)
	}
	if err != nil {
		return nil, err
	}

	if username != "" && existing.Username != username {
		return existing.Update().
			SetUsername(username).
			SetUpdatedAt(time.Now()).
			Save(ctx)
	}

	return existing, nil
}
