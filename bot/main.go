package main

import (
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/bwmarrin/discordgo"
)

func main() {
	token := normalizeToken(os.Getenv("DISCORD_KEY"))
	if token == "" {
		fmt.Println("no given DISCORD_KEY")
		return
	}

	guildID := strings.TrimSpace(os.Getenv("GUILD_ID"))

	dg, err := discordgo.New("Bot " + token)
	if err != nil {
		fmt.Println("error while creating a session:", err)
		return
	}

	dg.AddHandler(interactionCreate)
	dg.AddHandler(textInteractionCreate)

	dg.Identify.Intents = discordgo.IntentsGuilds | discordgo.IntentsGuildMessages | discordgo.IntentMessageContent

	err = dg.Open()
	if err != nil {
		fmt.Println("error while opening discord websocket:", err)
		return
	}
	defer dg.Close()

	deadline := time.Now().Add(30 * time.Second)
	for dg.State.User == nil {
		if time.Now().After(deadline) {
			fmt.Println("error: Discord session is not ready")
			return
		}
		time.Sleep(100 * time.Millisecond)
	}

	cmd := &discordgo.ApplicationCommand{
		Name:        "ping",
		Description: "Ping bot.",
	}

	createdCmd, err := dg.ApplicationCommandCreate(dg.State.User.ID, guildID, cmd)
	if err != nil {
		fmt.Println("error while creating slash command:", err)
		return
	}

	fmt.Println("Bot is running, press CTRL+C to exit")

	sc := make(chan os.Signal, 1)
	signal.Notify(sc, syscall.SIGTERM, os.Interrupt)
	<-sc

	_ = dg.ApplicationCommandDelete(dg.State.User.ID, guildID, createdCmd.ID)
}

func interactionCreate(s *discordgo.Session, i *discordgo.InteractionCreate) {
	if i.Type != discordgo.InteractionApplicationCommand {
		return
	}

	if i.ApplicationCommandData().Name == "ping" {
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
}

func textInteractionCreate(s *discordgo.Session, m *discordgo.MessageCreate) {
	if m.Type != discordgo.MessageTypeDefault ||
		m.Content == "" || !strings.HasPrefix(m.Content, "!") {
		return
	}

	if m.Content == "!ping" {
		s.ChannelMessageSendReply(m.GuildID, "Pong!", m.Reference())
	}
}

func normalizeToken(raw string) string {
	token := strings.Trim(raw, `"'`)
	token = strings.TrimSpace(token)
	if token == "" || token == "Bot" {
		return ""
	}
	token = strings.TrimPrefix(token, "Bot ")
	return token
}
