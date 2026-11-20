package handlers

import (
	"arikawa-discord/bot/repositories"
	"log"

	"github.com/bwmarrin/discordgo"
)

type BotHandler struct {
	db       *repositories.Repositories
	Commands map[string]func(s *discordgo.Session, i *discordgo.InteractionCreate)
}

func NewBotHandler(db *repositories.Repositories) *BotHandler {
	h := &BotHandler{
		db: db,
	}

	h.Commands = map[string]func(s *discordgo.Session, i *discordgo.InteractionCreate){
		"ping":      h.HandlePing,
		"watchlist": h.HandleWatchList,
		"queue":     h.HandleQueue,
	}

	return h
}

func (h *BotHandler) InteractionCreate(s *discordgo.Session, i *discordgo.InteractionCreate) {
	if i.Type != discordgo.InteractionApplicationCommand {
		return
	}

	cmd := i.ApplicationCommandData().Name

	if handler, ok := h.Commands[cmd]; ok {
		handler(s, i)
	} else {
		log.Println("Unknown slash command:", cmd)
	}
}
