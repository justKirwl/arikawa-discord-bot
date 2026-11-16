package handlers

import "github.com/bwmarrin/discordgo"

type CommandHandler func(s *discordgo.Session, i *discordgo.InteractionCreate)

var CommandHandlers = map[string]CommandHandler{
	"ping":      HandlePing,
	"watchlist": HandleWatchList,
	"queue":     HandleQueue,
}
