package repositories

import (
	"arikawa-discord/ent"
	"arikawa-discord/ent/watchlist"
	"context"
)

type WatchListRepo struct {
	db *ent.Client
}

func CreateWatchListRepo(db *ent.Client) *WatchListRepo {
	return &WatchListRepo{db: db}
}

func (w *WatchListRepo) List(ctx context.Context, userId string) ([]*ent.WatchList, error) {
	return w.db.WatchList.Query().Where(watchlist.UserID(userId)).All(ctx)
}

// func (w *WatchListRepo) Add(ctx context.Context) error {

// }
