# Arena of PESTS — multiplayer combat server

This turns the combat screen from "trust the browser" into a real
client/server game:

- **Transport:** raw `ws` WebSockets. No socket.io, no HTTP polling fallback,
  permessage-deflate disabled — messages are tiny JSON objects and a
  turn-based card game doesn't need anything heavier.
- **Truth:** `game-engine.js` is a DOM-free port of the exact combat rules
  that used to run in `index.html` (cards, elements, status effects, weapon/
  defense durability, gacha odds). It only runs on the server now. The client
  sends *intents* (deploy this card / attack with this slot / end turn), the
  server validates and resolves them, and pushes the result back.
- **Money:** gold, gems, wins/losses, the card collection, and the saved
  deck all live in Postgres via Supabase and are only ever written by
  `server.js` using the service-role key. The client never has a code path
  that can change its own balance — it can only ask the server to open a
  pack or play a match, and the server decides what that's worth.

## Pointing the client at this server

`index.html` now has a full multiplayer mode wired in (a "⚔ Multiplayer Duel"
button next to practice mode). It connects to whatever WebSocket URL it
finds, in this order:

1. `window.ARENA_WS_URL` — set this in a `<script>` tag before the game's
   script runs if you're embedding/deploying it, e.g.
   `<script>window.ARENA_WS_URL='wss://arena.yourdomain.com';</script>`
2. A `?ws=` query param, e.g. `index.html?ws=ws://localhost:8787` — handy for
   local testing.
3. Falls back to `ws://localhost:8787`.

Practice-vs-AI mode never touches the network and keeps working with zero
config, online or offline. The moment the player reaches the main menu the
client tries to connect and authenticate as a guest (or via a logged-in
Supabase session if you wire that in) so gold/gems/collection/deck are
already synced before they ever queue for a match.

## Setup

```bash
npm install
cp .env.example .env   # fill in SUPABASE_URL + SUPABASE_SERVICE_ROLE_KEY
npm start               # listens on :8787 (PORT env var to change)
```

Run `supabase-schema.sql` once in the Supabase SQL editor to create
`profiles`, `player_cards`, `player_decks`, and `match_history`.

If you leave the Supabase env vars blank, the server still runs — matches
work over WebSockets — but wallets are in-memory only and reset on restart.
That's useful for local dev, but don't ship it that way.

`GET /health` returns `{ ok, matches, queue }` for a load balancer / uptime
check.

## Wire protocol

Everything is JSON over one WebSocket connection. Client → server messages:

| type | payload | when |
|---|---|---|
| `auth` | `{ token }` (Supabase JWT) or `{ guest: true, name }` | first message, before anything else |
| `queue_join` | — | enter matchmaking |
| `queue_leave` | — | leave matchmaking |
| `deploy` | `{ instanceId }` | play a card from hand (weapon/defense/active), during SETUP or on your MAIN turn |
| `ready_battle` | — | done deploying, ready to start (SETUP phase) |
| `attack` | `{ slot: 'slot1'|'slot2', target: 'slot1'|'slot2'|null, atkIndex: 0|1 }` | your MAIN turn |
| `end_turn` | — | your MAIN turn |
| `forfeit` | — | concede the current match |
| `get_profile` | — | refresh wallet/collection snapshot |
| `save_deck` | `{ cardIds: [...] }` | persist a deck (server drops any id you don't own, caps at 10) |
| `buy_pack` | `{ packId }` | open a pack; server debits gold/gems and rolls cards |

Server → client messages:

| type | payload |
|---|---|
| `auth_ok` | `{ userId, profile }` |
| `queue_status` | `{ inQueue }` |
| `match_found` | `{ matchId, youAre: 0|1, opponentName }` |
| `state` | `{ matchId, phase, turn, you, state, events }` — full snapshot from your perspective (your hand's contents, opponent's hand *count* only) plus an `events` array to replay animations (`hit`, `dot`, `status`, `death`, `weapon_use`, `defense_use`, `curse_recoil`, `rocks`, `coinflip`, `ability`, `miss`, `excess`, `turn_skip`) |
| `opponent_ready` / `opponent_disconnected` / `opponent_reconnected` | setup/connection status |
| `match_over` | `{ result: 'win'|'loss', reward: {gold, gems}, profile }` |
| `profile` | `{ profile }` |
| `deck_saved` | `{ cardIds }` |
| `pack_result` | `{ packId, cards, currency, newBalance }` |
| `error` | `{ reason }` |

## Anti-cheat notes for the client rewrite

- Never trust local HP/gold/gem/collection state for anything the game
  scores you on. Treat every `state`/`profile` message as the source of
  truth and re-render from it.
- The client should only reference cards it received in its own `hand`
  array — it can't invent an `instanceId` because the server checks every
  `deploy`/`attack` against its own match state.
- Reconnect: keep sending the same `guestId` (or the same logged-in user)
  and the server will slot you back into your live match within a 20s grace
  window instead of forfeiting you immediately.

## Known simplifications (call these out if picking this up later)

- Matchmaking is FIFO, no skill rating.
- Turn timer auto-ends a stalled turn after 45s; setup phase auto-readies
  after 60s.
- Reconnect uses a stable client-supplied `guestId` for guests; logged-in
  users reconnect via their Supabase user id automatically.