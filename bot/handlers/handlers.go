package handlers

import (
	"fmt"

	"github.com/bwmarrin/discordgo"
)

func HandlePing(s *discordgo.Session, i *discordgo.InteractionCreate) {
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

func HandleWatchList(s *discordgo.Session, i *discordgo.InteractionCreate) {
	err := s.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
		Type: discordgo.InteractionResponseChannelMessageWithSource,
		Data: &discordgo.InteractionResponseData{
			Content: "Watch list from database here.",
		},
	})
	if err != nil {
		fmt.Println("error responding to interaction:", err)
	}
}

func HandleQueue(s *discordgo.Session, i *discordgo.InteractionCreate) {
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
