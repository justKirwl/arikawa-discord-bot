package handlers

import (
	"context"
	"log"
	"time"

	"arikawa-discord/bot/repositories"

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

	// In a guild discordgo fills Member instead of User; in a DM the reverse.
	u := i.User
	if u == nil && i.Member != nil {
		u = i.Member.User
	}
	if u == nil {
		log.Println("interaction without user")
		return
	}

	// Register the interacting user before any command touches the database.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := h.db.Users.Ensure(ctx, u.ID, u.Username); err != nil {
		log.Println("Error registering user:", err)
	}

	cmd := i.ApplicationCommandData().Name

	if handler, ok := h.Commands[cmd]; ok {
		handler(s, i)
	} else {
		log.Println("Unknown slash command:", cmd)
	}
}
