package repositories

type Repositories struct {
	Watchlist *WatchListRepo
}

func MergeDatabaseServices(w *WatchListRepo) *Repositories {
	return &Repositories{
		Watchlist: w,
	}
}
