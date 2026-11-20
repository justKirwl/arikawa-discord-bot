package handlers

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/bwmarrin/discordgo"
)

func (h *BotHandler) HandlePing(s *discordgo.Session, i *discordgo.InteractionCreate) {
	err := s.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
		Type: discordgo.InteractionResponseChannelMessageWithSource,
		Data: &discordgo.InteractionResponseData{
			Content: "Pong!",
		},
	})
	if err != nil {
		fmt.Println("error responding to interaction:", err)
	}
}

func (h *BotHandler) HandleWatchList(s *discordgo.Session, i *discordgo.InteractionCreate) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var sb strings.Builder

	watchlist, fetchErr := h.db.Watchlist.List(ctx)
	if fetchErr != nil {
		log.Println("Error in fetching watch list occured:", fetchErr)
	}
	if len(watchlist) == 0 {
		sb.WriteString("**You don't have watchlist for now**")
	} else {
		sb.WriteString("**Your watchlist:\n\n**")

		// TODO: add fetching for title when title table is gonna exist
		fmt.Fprintf(&sb, "**1.** Fight Club (ID: `550`) • Added <t:%d:R>\n", time.Now().Add(-72*time.Hour).Unix())
		fmt.Fprintf(&sb, "**2.** Interstellar (ID: `157336`) • Added <t:%d:R>\n", time.Now().Add(-5*time.Hour).Unix())
		fmt.Fprintf(&sb, "**3.** Inception (ID: `27205`) • Added <t:%d:R>\n", time.Now().Add(-15*time.Minute).Unix())
		fmt.Fprintf(&sb, "**4.** Breaking Bad (ID: `1396`) • Added <t:%d:R>\n", time.Now().Add(-2*time.Minute).Unix())
	}

	embed := &discordgo.MessageEmbed{
		Title:       "Watchlist",
		Description: sb.String(),
		Color:       0x6aa84f,
	}

	err := s.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
		Type: discordgo.InteractionResponseChannelMessageWithSource,
		Data: &discordgo.InteractionResponseData{
			Embeds: []*discordgo.MessageEmbed{embed},
		},
	})
	if err != nil {
		fmt.Println("error responding to interaction:", err)
	}
}

func (h *BotHandler) HandleQueue(s *discordgo.Session, i *discordgo.InteractionCreate) {
	err := s.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
		Type: discordgo.InteractionResponseChannelMessageWithSource,
		Data: &discordgo.InteractionResponseData{
			Content: "Queue of downloading here.",
		},
	})
	if err != nil {
		fmt.Println("error responding to interaction:", err)
	}
}
