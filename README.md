# Cinema bot — services

Монорепо проєкту «Кінотеатр» (див. PLAN.md): Go-бот керує, Rust обробляє
real-time і медіа-пайплайн.

| Тека | Сервіс | Стан |
| --- | --- | --- |
| [`roomd/`](roomd/README.md) | Rust: стан кімнат, синхронізація відтворення, WebSocket fan-out | S3.3 готово, далі S3.4 |
| [`bot/`](bot/main.go) | Go: Discord-бот (discordgo) | смоук-бот підключений до Gateway |
| `mediad/` | Rust: завантаження, HLS-транскодування, підписані URL | заблоковано рішенням D1 |

Контракти (`roomd/proto/`, `roomd/docs/protocol.md`) спільні з Go-ботом і
веб-клієнтом: зміни до них — окремим комітом `contract-change` після review.

## Запуск усього стека

Створи `.env` з токеном Discord:

```sh
cp .env.example .env
# DISCORD_KEY=<raw bot token, без префікса "Bot ">
docker compose up -d --build
```

| Сервіс | Порти | Перевірка |
| --- | --- | --- |
| `roomd` | `8080` (HTTP + `/ws`), `50051` (gRPC) | `curl http://localhost:8080/healthz` |
| `bot` | — | `docker compose logs -f bot` → `Bot is running` |

## Локальна розробка `roomd`

```sh
cd roomd
cp .env.example .env
cargo run
```

Тести: `cargo test`, лінтер: `cargo clippy --all-targets -- -D warnings`.
