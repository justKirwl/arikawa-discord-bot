package main

import (
	"arikawa-discord/bot/handlers"
	"arikawa-discord/bot/repositories"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/joho/godotenv"
)

func main() {
	_ = godotenv.Load()
	token := normalizeToken(os.Getenv("DISCORD_KEY"))
	if token == "" {
		log.Fatal("no given DISCORD_KEY")
	}

	guildID := strings.TrimSpace(os.Getenv("GUILD_ID"))

	dg, err := discordgo.New("Bot " + token)
	if err != nil {
		log.Fatal("error while creating a session:", err)
	}

	client := ConnectToDatabase()

	watchlistRepo := repositories.CreateWatchListRepo(client)

	dbServices := repositories.MergeDatabaseServices(watchlistRepo)
	botHandler := handlers.NewBotHandler(dbServices)

	dg.AddHandler(botHandler.InteractionCreate)
	dg.AddHandler(textInteractionCreate)

	dg.Identify.Intents = discordgo.IntentsGuilds | discordgo.IntentsGuildMessages | discordgo.IntentMessageContent

	err = dg.Open()
	if err != nil {
		log.Fatal("error while opening discord websocket:", err)
	}
	defer dg.Close()

	deadline := time.Now().Add(30 * time.Second)
	for dg.State.User == nil {
		if time.Now().After(deadline) {
			log.Fatal("error: Discord session is not ready")
		}
		time.Sleep(100 * time.Millisecond)
	}

	cmds := []*discordgo.ApplicationCommand{{
		Name:        "ping",
		Description: "Ping bot.",
	},
		{
			Name:        "watchlist",
			Description: "Watchlist here.",
			Options: []*discordgo.ApplicationCommandOption{
				{
					Name:        "list",
					Description: "List whole wished watchlist.",
					Type:        discordgo.ApplicationCommandOptionSubCommand,
				},
			},
		},
		{
			Name:        "queue",
			Description: "Queue of downloading here.",
		}}

	_, err = dg.ApplicationCommandBulkOverwrite(dg.State.User.ID, guildID, cmds)
	if err != nil {
		log.Fatal("error while creating slash command:", err)
	}

	log.Println("Bot is running, press CTRL+C to exit")

	sc := make(chan os.Signal, 1)
	signal.Notify(sc, syscall.SIGTERM, os.Interrupt)
	<-sc
}

func textInteractionCreate(s *discordgo.Session, m *discordgo.MessageCreate) {
	if m.Type != discordgo.MessageTypeDefault ||
		m.Content == "" || !strings.HasPrefix(m.Content, "!") {
		return
	}

	if m.Content == "!ping" {
		if _, err := s.ChannelMessageSendReply(m.ChannelID, "Pong!", m.Reference()); err != nil {
			log.Println("error replying to message:", err)
		}
	}
}

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
