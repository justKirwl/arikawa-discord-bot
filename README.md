# Cinema bot — Rust services

Монорепо Rust-частини проєкту «Кінотеатр» (див. PLAN.md): real-time і медіа-пайплайн.

| Тека | Сервіс | Стан |
| --- | --- | --- |
| [`roomd/`](roomd/README.md) | стан кімнат, синхронізація відтворення, WebSocket fan-out | S3.1 готово, далі S3.2 |
| `mediad/` | завантаження, HLS-транскодування, підписані URL | заблоковано рішенням D1 |

Контракти (`roomd/proto/`, `roomd/docs/protocol.md`) спільні з Go-ботом і
веб-клієнтом: зміни до них — окремим комітом `contract-change` після review.

Швидкий старт:

```sh
cd roomd
cp .env.example .env
cargo run
```
