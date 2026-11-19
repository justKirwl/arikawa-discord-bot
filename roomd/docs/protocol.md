# roomd WebSocket protocol

**Версія:** 0.1 (2026-11-16)
**Статус:** заморожено на M0. Зміни — лише за погодженням Go + Rust + web.
**Реалізація:** `roomd/` (S3.3–S3.10). Клієнт — плеєр у `web/`.

Протокол описує обмін між браузером і `roomd`. Go-бот у ньому не бере участі:
він лише створює кімнату через gRPC і видає токен.

---

## 1. Транспорт

| Властивість | Значення |
| --- | --- |
| Endpoint | `GET /ws` (той самий HTTP-порт, що й `/healthz`) |
| Продакшен | `wss://` за reverse proxy; TLS термінується проксі |
| Формат | текстові фрейми, один JSON-об'єкт на фрейм, UTF-8 |
| Бінарні фрейми | закрити з кодом `1003` |
| Максимальний розмір повідомлення | 32 KiB, більше — `error{code:"message_too_large"}` і close `1009` |
| Одна кімната на з'єднання | так; для іншої кімнати відкривається нове з'єднання |

WebSocket-level ping не використовується для синхронізації годинників —
тільки прикладний `ping`/`pong` (розділ 6).

---

## 2. Конверт повідомлення

Кожне повідомлення — JSON-об'єкт:

```json
{ "type": "play", "request_id": 42 }
```

| Поле | Напрям | Обов'язкове | Опис |
| --- | --- | --- | --- |
| `type` | обидва | так | тип повідомлення |
| `request_id` | клієнт → сервер | ні | ціле число; сервер повторює його в `error`, щоб клієнт знайшов винну дію |
| `seq` | сервер → клієнт | для `participants`, `chat`, `reaction` | монотонний лічильник подій кімнати (не стану) |
| `server_ts` | сервер → клієнт | так, крім `error` | Unix epoch ms за годинником `roomd` |

Правила:

- поля в `snake_case`;
- клієнт і сервер ігнорують невідомі поля (additive evolution);
- невідомий `type` → `error{code:"unknown_type"}`, з'єднання не закривається;
- битий JSON або відсутній `type` → `error{code:"bad_message"}`, з'єднання лишається.

---

## 3. Клієнт → сервер

| `type` | Поля | Дозволено | Опис |
| --- | --- | --- | --- |
| `join` | `token` | один раз | авторизація та вхід у кімнату |
| `play` | — | host | почати/продовжити відтворення |
| `pause` | — | host | пауза |
| `seek` | `position_ms` | host | перемотка, `0 ≤ position_ms ≤ duration_ms` |
| `ping` | `client_ts` | усі | оцінка RTT і зсуву годинника |
| `chat_send` | `text`, `client_msg_id?` | усі | повідомлення в чат кімнати |
| `reaction` | `emoji` | усі | реакція |

### 3.1 `join`

```json
{ "type": "join", "token": "<jwt>" }
```

- Токен перевіряється за розділом 13 до будь-якої іншої дії.
- Успіх: сервер надсилає `state`, потім `participants`, потім розсилає іншим
  учасникам `participants` з оновленим списком.
- Повторний `join` на тому ж з'єднанні → `error{code:"already_joined"}`.
- Невдала авторизація → `error{code:"invalid_token" | "token_expired"}` і close `1008`.

### 3.2 `seek`

```json
{ "type": "seek", "position_ms": 120000, "request_id": 7 }
```

`position_ms` поза межами `[0, duration_ms]` затискається до межі (не є помилкою).
Якщо медіа не встановлене → `error{code:"no_media"}`.

### 3.3 `ping`

```json
{ "type": "ping", "client_ts": 1763300000123 }
```

`client_ts` — локальний час клієнта в ms. Сервер повертає його без змін у `pong`.

### 3.4 `chat_send`

```json
{ "type": "chat_send", "text": "починаємо!", "client_msg_id": "c-17" }
```

- `text` після `trim()`: 1–500 символів, інакше `error{code:"bad_message"}`.
- `client_msg_id` — до 64 символів, непрозорий для сервера, повертається в `chat`.

### 3.5 `reaction`

```json
{ "type": "reaction", "emoji": "🍿" }
```

`emoji` — один емодзі, до 32 байт UTF-8. Реакції не зберігаються.

---

## 4. Сервер → клієнт

| `type` | Поля | Коли надсилається |
| --- | --- | --- |
| `state` | `version`, `media_ref`, `position_ms`, `playing`, `server_ts` | після `join`, після кожної прийнятої зміни стану |
| `participants` | `participants[]` | після входу/виходу учасника |
| `pong` | `client_ts`, `server_ts` | у відповідь на `ping` |
| `chat` | `user_id`, `text`, `client_msg_id?`, `server_ts`, `seq` | після прийнятого `chat_send` |
| `reaction` | `user_id`, `emoji`, `server_ts`, `seq` | після прийнятої реакції |
| `error` | `code`, `message?`, `request_id?` | відмова або помилка |

### 4.1 `state`

```json
{
  "type": "state",
  "version": 8,
  "media_ref": {
    "media_id": "01J9Z6K3",
    "kind": "hls",
    "url": "https://media.example/u/01J9Z6K3/master.m3u8?sig=...",
    "duration_ms": 7200000,
    "audio_tracks": [{ "id": "a1", "language": "uk", "label": "Українська", "default": true }],
    "subtitle_tracks": [{ "id": "s1", "language": "uk", "label": "Українські", "url": "https://...", "default": true }],
    "poster_url": "https://..."
  },
  "position_ms": 60000,
  "playing": true,
  "server_ts": 1763300000500
}
```

- `media_ref: null` — медіа ще не встановлене; плеєр показує заглушку.
- `position_ms` — позиція **на момент** `server_ts` (див. розділ 5).
- `version` строго зростає в межах кімнати.

### 4.2 `participants`

```json
{
  "type": "participants",
  "participants": [
    { "user_id": "123", "role": "host", "joined_at_server_ms": 1763300000000 },
    { "user_id": "456", "role": "viewer", "joined_at_server_ms": 1763300001000 }
  ],
  "seq": 12,
  "server_ts": 1763300001000
}
```

### 4.3 `pong`

```json
{ "type": "pong", "client_ts": 1763300000123, "server_ts": 1763300000140 }
```

### 4.4 `chat` і `reaction`

```json
{ "type": "chat", "user_id": "456", "text": "починаємо!", "client_msg_id": "c-17", "server_ts": 1763300002000, "seq": 13 }
{ "type": "reaction", "user_id": "456", "emoji": "🍿", "server_ts": 1763300003000, "seq": 14 }
```

Історії чату немає: новий учасник бачить лише повідомлення після `join`.

### 4.5 `error`

```json
{ "type": "error", "code": "not_host", "message": "only the host can control playback", "request_id": 7 }
```

`message` — для логів і діагностики, не для показу користувачу (UI мапиться за `code`).

---

## 5. Модель стану та синхронізація

`roomd` зберігає для кімнати:

```
media_ref, position_ms, playing, updated_at_server_ms, version
```

Правило: `position_ms` завжди означає позицію на момент `updated_at_server_ms`.
У повідомленні `state` це поле передається як `server_ts`, тому формула з
PLAN.md §2.3 застосовується без додаткових перетворень.

Очікувана позиція на клієнті в момент `server_now`:

```
paused:    position_ms
playing:   position_ms + (server_now - server_ts)
```

`server_now` клієнт оцінює через `ping`/`pong` (розділ 6).

Політика корекції в плеєрі:

| Відхилення `|expected - current|` | Дія |
| --- | --- |
| ≤ 150 ms | нічого |
| 150–500 ms | плавна зміна `playbackRate` у межах `[0.95, 1.05]` |
| > 500 ms | `seek(expected)` |
| > 500 ms після 5 s корекції `playbackRate` | `seek(expected)` |

`version`:

- початкове значення `1`;
- інкремент на кожну прийняту мутацію (`play`, `pause`, `seek`, `SetMedia`);
- клієнт **ігнорує** `state` з `version <= last_seen_version` (захист від переупорядкування);
- `MediaEnded` не змінює `version`, поки бот не викличе `SetMedia` (S6.3).

---

## 6. Оцінка зсуву годинника

Клієнт періодично (кожні 10 s, і одразу після `join`) надсилає `ping`:

```
t0 = local_now()
-> ping{client_ts: t0}
<- pong{client_ts: t0, server_ts: t1}
t2 = local_now()

rtt    = t2 - t0
offset = t1 - (t0 + t2) / 2
server_now_est = local_now() + offset
```

- клієнт бере 5 вимірювань і використовує те, у якого найменший `rtt`;
- `offset` перераховується кожні 30 s;
- якщо `rtt > 2000 ms`, вимірювання відкидається;
- після реконнекту оцінка скидається і знімається заново.

---

## 7. Вхід, реконнект, пізнє приєднання

- **Пізнє приєднання:** `join` → `state` містить актуальну позицію; плеєр робить
  `seek` перед стартом, якщо `|expected - 0| > 500 ms`.
- **Реконнект:** нове WebSocket-з'єднання, той самий токен (поки не минув `exp`),
  `join` → `state` + `participants`. Черга й історія чату не відновлюються.
- **Хост зник:** кімната лишається живою; роль визначається claim-ом токена, а не
  присутністю. Хост може перепідключитися.
- **Порожня кімната:** якщо всі вийшли, `roomd` тримає кімнату `room_idle_timeout`
  (типово 300 s, S3.7), далі закриває з причиною `idle_timeout` і надсилає боту
  `RoomClosed`.
- **Закриття ботом:** `CloseRoom` → усім клієнтам close `1000`, підписникам
  `StreamRoomEvents` — `RoomClosed{reason:"closed_by_host"}`.

---

## 8. Ролі та права

| Дія | host | viewer |
| --- | --- | --- |
| `play`, `pause`, `seek` | так | `error{code:"not_host"}` |
| `chat_send`, `reaction` | так | так |
| `ping` | так | так |
| `SetMedia` | лише через Go (gRPC) | — |

`role` береться з токена. Кількість хостів не обмежена (типово один).

---

## 9. Чат, реакції, антифлуд, backpressure

Ліміти на одне з'єднання (token bucket):

| Категорія | Sustained | Burst |
| --- | --- | --- |
| `chat_send` | 3 за 5 s | 3 |
| `reaction` | 5 за 5 s | 5 |
| `play`/`pause`/`seek` | 5 за 1 s | 10 |
| `ping` | 1 за 1 s | 3 |

- Перевищення → `error{code:"rate_limited", retry_after_ms}`;
- більше 20 порушень за хвилину → close `1008`;
- `join` дозволений один раз на з'єднання.

Backpressure на вихідну чергу з'єднання (256 повідомлень):

1. при переповненні першими відкидаються `chat` і `reaction`;
2. `state`, `participants`, `pong`, `error` не відкидаються ніколи;
3. якщо черга перевищила 1024 повідомлення — `error{code:"slow_consumer"}` і
   close `1013`.

---

## 10. Коди закриття WebSocket

| Код | Причина |
| --- | --- |
| `1000` | нормальне закриття (бот закрив кімнату, клієнт вийшов) |
| `1003` | бінарний фрейм |
| `1008` | невалідний/прострочений токен, повторні порушення лімітів |
| `1009` | повідомлення завелике |
| `1011` | внутрішня помилка |
| `1013` | повільний споживач |

---

## 11. Коди помилок (`error.code`)

| Код | Значення |
| --- | --- |
| `bad_message` | невалідний JSON, відсутні/некоректні поля |
| `unknown_type` | невідомий `type` |
| `invalid_token` | підпис/claims не пройшли перевірку |
| `token_expired` | `exp` у минулому |
| `already_joined` | повторний `join` |
| `room_not_found` | кімнати не існує |
| `room_closed` | кімнату закрито |
| `room_full` | кімната заповнена (максимум учасників, §14) |
| `not_host` | дія доступна лише хосту |
| `no_media` | медіа не встановлене |
| `rate_limited` | перевищено ліміт частоти |
| `message_too_large` | повідомлення > 32 KiB |
| `slow_consumer` | клієнт не встигає читати |
| `internal` | внутрішня помилка `roomd` |

---

## 12. Авторизація

Токен — JWT, підпис **Ed25519** (`alg: "EdDSA"`), видається Go-ботом
(PLAN.md §2.4). `roomd` перевіряє підпис публічним ключем і не звертається до Go.

Header:

```json
{ "alg": "EdDSA", "typ": "JWT", "kid": "k-2026-11" }
```

Claims:

```json
{
  "iss": "cinema-bot",
  "sub": "123456789012345678",
  "guild_id": "987654321098765432",
  "room_id": "019a4f2c-...",
  "user_id": "123456789012345678",
  "role": "host",
  "iat": 1763300000,
  "exp": 1763300300
}
```

Правила перевірки:

- `alg` рівно `EdDSA`, `kid` відомий сервісу;
- `exp > now`, допустимий скос годинника — 30 s;
- `room_id` у токені збігається з кімнатою з'єднання;
- `role` ∈ {`host`, `viewer`}, `user_id` непорожній;
- TTL токена — до 5 хвилин; токен можна перевикористати для реконнекту до `exp`.

Доставка публічних ключів (файл/PEM, зміна `kid` без рестарту) фіксується в
ADR-002. Для розробки `roomd` використовує тестовий ключ і dev-утиліту видачі
токенів (S0, див. PLAN.md).

---

## 13. `media_ref` у JSON

| Поле | Тип | Опис |
| --- | --- | --- |
| `media_id` | string | id від `mediad` |
| `kind` | `"file"` \| `"hls"` | тип джерела |
| `url` | string | HTTPS-URL маніфесту або файлу (може бути підписаний, з TTL) |
| `duration_ms` | int64 | тривалість, `0` якщо невідома |
| `audio_tracks[]` | array | `id`, `language`, `label`, `default` |
| `subtitle_tracks[]` | array | `id`, `language`, `label`, `url`, `default` |
| `poster_url` | string \| null | постер для UI |

Відповідність proto: `media.v1.MediaKind` → `"file"`, `"hls"`;
`MEDIA_KIND_UNSPECIFIED` у JSON не зустрічається.

---

## 14. Ліміти

| Параметр | Значення |
| --- | --- |
| Максимум учасників у кімнаті | 50 (S3.7; конфігуровано) |
| Максимум повідомлення | 32 KiB |
| `room_idle_timeout` | 300 s |
| TTL токена кімнати | ≤ 300 s |
| Період `ping` | 10 s |

---

## 15. Сумісність

- Версія 0.1 заморожена на віхах M0.
- Додавання нових `type`, полів і кодів помилок — зворотно сумісне, версія не змінюється.
- Видалення/перейменування полів, зміна семантики — лише нова версія протоколу
  (`/v2/ws`) після review обома розробниками.
- Кожна зміна контракту — окремий коміт із префіксом `contract-change`.
