package repositories

import (
	"arikawa-discord/ent"
	"context"
)

type WatchListRepo struct {
	db *ent.Client
}

func CreateWatchListRepo(db *ent.Client) *WatchListRepo {
	return &WatchListRepo{db: db}
}

func (w *WatchListRepo) List(ctx context.Context) ([]*ent.WatchList, error) {
	return w.db.WatchList.Query().All(ctx)
}
