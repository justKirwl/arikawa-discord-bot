package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/bwmarrin/discordgo"
)

func main() {
	token := os.Getenv("DISCORD_KEY")
	if token == "" {
		fmt.Println("no given DISCORD_KEY")
		return
	}

	dg, err := discordgo.New("Bot" + token)
	if err != nil {
		fmt.Println("error while creating a session:", err)
		return
	}

	dg.AddHandler(messageCreate)
	dg.Identify.Intents = discordgo.IntentsGuildMessages | discordgo.IntentMessageContent
	err = dg.Open()
	if err != nil {
		fmt.Println("error while opening discord websocket:", err)
		return
	}
	defer dg.Close()

	fmt.Println("Bot is running, press CTRL+C to exit")

	sc := make(chan os.Signal, 1)
	signal.Notify(sc, syscall.SIGTERM, os.Interrupt)
	<-sc
}

func messageCreate(s *discordgo.Session, m *discordgo.MessageCreate) {
	if m.Content == "!ping" {
		_, _ = s.ChannelMessageSend(m.ChannelID, "Pong!")
	}
}
