package main

import (
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/bwmarrin/discordgo"
)

func main() {
	token := normalizeToken(os.Getenv("DISCORD_KEY"))
	if token == "" {
		fmt.Println("no given DISCORD_KEY")
		return
	}

	dg, err := discordgo.New("Bot " + token)
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

// normalizeToken tolerates the usual .env mistakes: surrounding whitespace,
// CRLF, quotes and a duplicated "Bot " prefix.
func normalizeToken(raw string) string {
	token := strings.TrimSpace(raw)
	token = strings.Trim(token, `"'`)
	token = strings.TrimSpace(token)
	if token == "" || token == "Bot" {
		return ""
	}
	token = strings.TrimPrefix(token, "Bot ")
	token = strings.TrimSpace(token)
	token = strings.Trim(token, `"'`)
	return strings.TrimSpace(token)
}
