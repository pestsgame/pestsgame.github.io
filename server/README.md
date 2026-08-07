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

`index.html` connects to whatever WebSocket URL it finds, in this order:

1. `window.ARENA_WS_URL` — set this in a `<script>` tag before the game's
   script runs if you're embedding/deploying it, e.g.
   `<script>window.ARENA_WS_URL='wss://arena.yourdomain.com';</script>`
2. A `?ws=` query param, e.g. `index.html?ws=ws://localhost:8787` — handy for
   local testing.
3. Falls back to `ws://localhost:8787`.

The client is multiplayer-only: the moment the player reaches the main menu
and hits "Find Match," it connects and authenticates as a guest (or via a
logged-in Supabase session if you wire that in) so gold/gems/collection/deck
are already synced before the match starts. There's no offline/practice mode
and no client-side combat resolution — `index.html` only renders whatever
the server sends it.

## Setup

```bash
npm install
cp .env.example .env   # fill in SUPABASE_URL + SUPABASE_SERVICE_ROLE_KEY
npm start               # listens on :8787 (PORT env var to change)
```

Run `supabase-schema.sql` once in the Supabase SQL editor to create
`profiles`, `player_cards`, `player_decks`, `match_history`, the global
chat table, the `friendships`/`presence` tables the Social tab uses, the
`guilds`/`guild_members`/guild chat tables, the
`marketplace_listings`/`marketplace_bids`/`direct_messages` tables the
Bazaar tab uses, and the `tournament_events`/`tournament_registrations`/
`tournament_brackets` tables the Tourney tab uses.

If you leave the Supabase env vars blank, the server still runs — matches
work over WebSockets — but wallets, friends, and presence are in-memory
only and reset on restart. That's useful for local dev, but don't ship it
that way.

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
| `heartbeat` | — | tells the server "I'm online right now." Only sent once the client has actually finished auth and entered the app — never before, and never automatically just from having a socket open. Repeats every ~2 min. |
| `friends_list` | — | fetch your friends + incoming/outgoing requests |
| `friend_request` | `{ username }` or `{ userId }` | send a friend request |
| `friend_respond` | `{ userId, accept }` | accept or decline an incoming request from `userId` |
| `friend_remove` | `{ userId }` | unfriend, or cancel your own outgoing request, or decline an incoming one |
| `duel_request` | `{ userId }` | challenge an online friend to a 1v1 |
| `duel_respond` | `{ userId, accept }` | accept/decline an incoming duel invite from `userId` — accepting starts a match immediately, same as a normal matched game |
| `market_browse` | `{ listingType?, currency? }` | fetch active Bazaar listings (optionally filtered) |
| `market_my_listings` | — | fetch your own listings, any status |
| `market_list_card` | `{ cardId, listingType:'price'\|'auction', currency:'gold'\|'gems', durationDays (1-14), price? (price listings), startingBid? / buyoutPrice? (auctions) }` | list an owned card — the card is escrowed off your collection immediately |
| `market_cancel_listing` | `{ listingId }` | cancel your own active listing (refunds any auction bidder), returns the card |
| `market_buy_listing` | `{ listingId }` | buy a 'price' listing, or instant-buyout an auction that has a `buyoutPrice` |
| `market_place_bid` | `{ listingId, amount }` | bid on an auction (escrows bid+tax immediately; refunds whoever it outbids) |
| `dm_conversations` | — | list your DM conversations with last message + unread count |
| `dm_history` | `{ userId }` | full message history with one other player (marks their messages read) |
| `dm_send` | `{ toId, text, listingId?, offerAmount?, offerCurrency? }` | send a DM, optionally carrying a price offer tied to a 'price' listing — negotiation only, auctions use bids instead. Messages sent with a `listingId` are auto-deleted ~1hr after that listing settles (see `cleanupExpiredListingDMs`), so a thread doesn't accumulate old negotiation clutter every time you buy from the same seller again |
| `dm_accept_offer` | `{ messageId }` | accept a pending offer sent *to you*, executing the sale immediately at that price |
| `tournament_list` | — | fetch the current official Daily/Weekly slots, open unofficial lobbies, and your own registrations/brackets |
| `tournament_create` | `{ name, maxPlayers, prizePoolPercent, startAt (ISO), entryCurrency:'gold'\|'gems', entryAmount }` | host an unofficial tournament — registration itself is capped at `maxPlayers`, so unlike official events this is always exactly one bracket |
| `tournament_join` | `{ eventId }` | register + pay the entry fee for an official or unofficial event, any time before it locks |
| `tournament_leave` | `{ eventId }` | unregister before lock time — full refund. Can't leave once it's locked/running |
| `tournament_cancel` | `{ eventId }` | host-only, unofficial only, before lock — refunds every registrant |
| `tournament_bracket` | `{ bracketId }` | fetch one specific bracket's full live tree, for the bracket view |

Server → client messages:

| type | payload |
|---|---|
| `auth_ok` | `{ userId, profile }` |
| `queue_status` | `{ inQueue }` |
| `match_found` | `{ matchId, youAre: 0|1, opponentName, opponentIcon, tournament? }` — `tournament` is present when this match is a bracket match: `{ bracketId, eventName, roundIndex, totalRounds, roundLabel }` |
| `state` | `{ matchId, phase, turn, you, state, events }` — full snapshot from your perspective (your hand's contents, opponent's hand *count* only) plus an `events` array to replay animations (`hit`, `dot`, `status`, `death`, `weapon_use`, `defense_use`, `curse_recoil`, `rocks`, `coinflip`, `ability`, `miss`, `excess`, `turn_skip`) |
| `opponent_ready` / `opponent_disconnected` / `opponent_reconnected` | setup/connection status |
| `match_over` | `{ result: 'win'|'loss', reward: {gold, gems}, profile, tournament? }` — `tournament` (bracket matches only) is `{ bracketId, eventName, tournamentComplete, championId, nextRoundLabel }` |
| `profile` | `{ profile }` |
| `player_profile` | `{ profile, friendship }` — response to `view_profile`; `friendship` is `'none'|'outgoing'|'incoming'|'friends'|null` (null when viewing yourself or a bot) |
| `deck_saved` | `{ cardIds }` |
| `pack_result` | `{ packId, cards, currency, newBalance }` |
| `friends_list` | `{ friends: [{userId,username,icon,online}], incoming: [...], outgoing: [...] }` |
| `friend_request_received` | `{ userId, username, icon }` — pushed to the target of a new request, if they're connected |
| `presence_update` | `{ userId, online }` — pushed to every online friend whenever someone's presence flips |
| `duel_request_received` | `{ userId, username, icon }` |
| `duel_request_sent` | `{ userId }` |
| `duel_declined` | `{ userId }` |
| `error` | `{ reason }` |
| `market_listings` / `market_my_listings` | `{ listings: [...] }` — browse results / your own listings, each with `sellerName`, `currentBidderName`, `expiresAt`, etc. |
| `market_listing_created` / `market_listing_cancelled` | `{ listing, profile }` — confirms your own action, with a fresh wallet/collection snapshot |
| `market_purchase_complete` | `{ listingId, cardId, profile }` — pushed to the buyer (direct buy, auction buyout, or an accepted offer) |
| `market_bid_placed` | `{ listing }` — confirms your own bid |
| `market_new_bid` | `{ listing }` — pushed to the seller whenever someone bids on their auction |
| `market_outbid` | `{ listingId, reason:'outbid'\|'bought_out'\|'cancelled' }` — pushed to whoever just got refunded |
| `market_auction_won` / `market_item_sold` | pushed to the winning bidder / the seller once an auction settles |
| `market_listing_expired` | pushed to the seller when an unsold listing expires and the card is returned |
| `dm_conversations` | `{ conversations: [{userId,username,icon,lastMessage,unread}] }` |
| `dm_history` | `{ userId, username, icon, messages }` |
| `dm_message` | `{ message }` — pushed to both sides of a new DM (with `fromUsername`/`fromIcon` for the recipient) |
| `tournament_list` | `{ officialDaily, officialWeekly, unofficial, mine }` — each an array of event summaries (`registeredCount`, `youRegistered`, `youCheckedIn`, `myBracketId` once running) |
| `tournament_created` / `tournament_joined` / `tournament_left` / `tournament_cancelled` | `{ event, profile? }` — confirms your own action |
| `tournament_refunded` | `{ eventId, reason:'cancelled'\|'not_enough_players', profile }` — pushed when an event you paid into gets cancelled out from under you (never sent for a no-show — that forfeits) |
| `tournament_disqualified` | `{ eventId, reason:'no_show' }` — best-effort; only reaches you if you happen to reconnect right as it's sent |
| `tournament_bracket_assigned` | `{ bracket }` — pushed to every checked-in participant the instant their shard's bracket is formed |
| `tournament_bracket_update` | `{ bracket }` — pushed to every participant of a bracket whenever its state changes (a round advances, it completes) |
| `tournament_bracket` | `{ bracket }` — response to a `tournament_bracket` request |
| `tournament_won` | `{ bracket, prize:{currency,amount}, profile }` — pushed specifically to the champion |
| `tournament_rematch` | `{ bracketId }` — pushed to both sides when their match drew and an immediate rematch is starting |

A `bracket` payload is `{ id, eventId, eventName, eventKind, prizeCurrency, prizePool, winnerPayout, status, winnerId, participants: [{userId,username,icon,seed}], rounds: [[{a,b,winnerId,loserId,isBye,matchId}, ...], ...] }` — everything the client needs to draw the full tree, one round per array entry.

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
- **Card stats live in exactly one place: `cards.json`.** The server loads
  it, hashes it (SHA-256 of the raw file), and serves it back verbatim at
  `GET /cards.json`. The client fetches that file at boot instead of
  hardcoding its own copy, and sends the hash it computed back on `auth`.
  If it doesn't match `CARD_LIBRARY_HASH` exactly, auth is refused before a
  `userId` is even assigned — so a modified/forked client can't buff a
  card's stats and matchmake with it. To change a card's numbers, edit
  `cards.json` and redeploy both sides; there's nothing to keep in sync by
  hand anymore.


## Known simplifications (call these out if picking this up later)

- **Bazaar tax rate locks in at bid/purchase time**, not at settlement. If a
  buyer and seller share a guild when a bid/offer/purchase happens, that 5%
  rate is what actually gets charged even if one of them leaves the guild
  before an auction ends — recomputing it later would mean charging a bidder
  more than the total they already had escrowed.
- **Auction bids escrow the full bid + tax immediately** and refund whoever
  they outbid; there's no "authorize now, charge later" step, so a bidder's
  balance always reflects money that's actually spoken for.
- The bot AI (`Match.botTurn` et al.) only ever picks `atkIndex` 0 or 1 (which
  works fine even for `shareAttack` cards, since sharing is baked into the top
  slot itself at deck-build time — no special index needed). It just doesn't
  specifically play around revive, multi-attack, or heal passives; it plays
  cards and attacks as before.
- `index.html` has no client-side combat engine at all — it's a pure renderer
  of server state, so all the new mechanics above (synergy, revive,
  multi-attack, heal) work in it automatically with zero client changes.
- Matchmaking is FIFO, no skill rating.
- Turn timer auto-ends a stalled turn after 45s; setup phase auto-readies
  after 60s.
- Reconnect uses a stable client-supplied `guestId` for guests; logged-in
  users reconnect via their Supabase user id automatically.
- **Tournaments assume a single server process.** `tournamentSweep()` guards
  against overlapping runs with an in-process flag, not a DB-level lock —
  fine for one instance, not safe to run as-is behind multiple server
  processes sharing one Supabase project (two processes could both lock the
  same event). "Showing up" for check-in/walkover purposes means "has an
  open WebSocket to *this* process right now", so it has the same
  single-process assumption as `matches`/`activeMatchByUser` already do
  elsewhere in this file.
- A drawn tournament match triggers an immediate rematch (new `Match`,
  same bracket slot) rather than any tie-break rule — draws are already
  rare in normal play (see `finishDraw`'s doc comment), so this keeps the
  bracket simple instead of adding a sudden-death mode nobody asked for.
