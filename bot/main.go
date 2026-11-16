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

	// Укажите ID сервера в .env для мгновенного появления команды.
	// Если оставить пустым, команда станет глобальной (появится в течение часа).
	guildID := strings.TrimSpace(os.Getenv("GUILD_ID"))

	dg, err := discordgo.New("Bot " + token)
	if err != nil {
		fmt.Println("error while creating a session:", err)
		return
	}

	// Регистрируем обработчик slash-команд
	dg.AddHandler(interactionCreate)

	// Для slash-команд привилегированный IntentMessageContent больше не нужен
	dg.Identify.Intents = discordgo.IntentsGuilds

	err = dg.Open()
	if err != nil {
		fmt.Println("error while opening discord websocket:", err)
		return
	}
	defer dg.Close()

	// Open returns before READY is processed, so State.User can still be nil.
	deadline := time.Now().Add(30 * time.Second)
	for dg.State.User == nil {
		if time.Now().After(deadline) {
			fmt.Println("error: Discord session is not ready")
			return
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Регистрируем команду /ping в Discord API
	cmd := &discordgo.ApplicationCommand{
		Name:        "ping",
		Description: "Проверяет отклик бота",
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

	// Удаляем команду при выключении, чтобы не оставлять дубликаты при тестах
	_ = dg.ApplicationCommandDelete(dg.State.User.ID, guildID, createdCmd.ID)
}

func interactionCreate(s *discordgo.Session, i *discordgo.InteractionCreate) {
	// Игнорируем всё, кроме вызова slash-команд
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
