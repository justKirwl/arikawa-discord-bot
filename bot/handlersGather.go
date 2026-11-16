package main

import "github.com/bwmarrin/discordgo"

type CommandHandler func(s *discordgo.Session, i *discordgo.InteractionCreate)

var commandHandlers = map[string]CommandHandler{
	"ping":      handlePing,
	"watchlist": handleWatchList,
	"queue":     handleQueue,
}
