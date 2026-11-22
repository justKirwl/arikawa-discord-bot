package repositories

type Repositories struct {
	Users     *UserRepo
	Watchlist *WatchListRepo
}

func MergeDatabaseServices(u *UserRepo, w *WatchListRepo) *Repositories {
	return &Repositories{
		Users:     u,
		Watchlist: w,
	}
}
