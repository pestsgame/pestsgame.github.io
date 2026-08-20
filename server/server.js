'use strict';
/**
 * Arena of PESTS, realtime combat server.
 *
 * - Transport: raw `ws` WebSockets (no socket.io tax, no polling fallback).
 *   Messages are small JSON objects; permessage-deflate is disabled because
 *   these payloads are tiny and compression negotiation overhead/latency
 *   isn't worth it for a turn-based game.
 * - Truth: the engine in game-engine.js runs ONLY here. The client sends
 *   *intents* ("I want to attack with slot1 into slot2 using my bottom
 *   attack"), the server validates and resolves them, and broadcasts the
 *   resulting state + animation events back to both players. A client can
 *   send garbage and the worst it can do is get an {type:'error'} back.
 * - Money: gold, gems, wins/losses, the card collection, and the saved deck
 *   all live in Postgres via Supabase and are only ever mutated here, with
 *   the service-role key. The client only ever *reads* a profile snapshot.
 */

require('dotenv').config();
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');
const { createClient } = require('@supabase/supabase-js');
const Engine = require('./game-engine');

/* ── CHAT IMAGE CATALOG ───────────────────────────────────────────────
 * The server's own copy of the same image.json the client's chat-image
 * picker reads from (docs/chat-images/image.json), kept as a separate
 * file here on purpose, so this validation never depends on trusting
 * whatever a connecting client claims its catalog looks like. The actual
 * image files are never touched or served by this server (they're static
 * assets shipped with the client); only the id list is needed, to decide
 * whether a `{image:<id>}` token a client tries to send is one of the
 * official images or something a modified client made up. Any chat text
 * that matches the token shape but isn't a real catalog id is rejected
 * outright rather than relayed or persisted. */
const CHAT_IMAGE_CATALOG_PATH = path.join(__dirname, 'image.json');
const CHAT_IMAGE_IDS = new Set(Object.keys(JSON.parse(fs.readFileSync(CHAT_IMAGE_CATALOG_PATH, 'utf8'))));
const CHAT_IMAGE_TOKEN_RE = /^\{image:([a-zA-Z0-9_-]{1,64})\}$/;
/** True for ordinary text (nothing shaped like an image token) or a token
 * naming a real catalog id; false only for a token naming an id that isn't
 * in the official catalog, the one case every chat send path must block. */
function isAllowedChatText(text) {
  const m = String(text || '').trim().match(CHAT_IMAGE_TOKEN_RE);
  return !m || CHAT_IMAGE_IDS.has(m[1]);
}

/* ── CONFIG ───────────────────────────────────────────────────────── */
const PORT = process.env.PORT || 8787;
const SUPABASE_URL = process.env.SUPABASE_URL || '';
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || '';
const HAS_SUPABASE = !!(SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY);
const WIN_GOLD_REWARD = 50;
const TURN_TIME_MS = 45_000;      // auto end-turn after this long
const SETUP_TIME_MS = 60_000;     // auto-ready after this long in setup
const RECONNECT_GRACE_MS = 20_000;

/* Real-opponent search window: if nobody else is in queue by the time this
 * (randomized) window elapses, the player is quietly handed off to a
 * server-controlled opponent instead of being left waiting. */
const BOT_FALLBACK_MIN_MS = 13_000;
const BOT_FALLBACK_MAX_MS = 17_000;

/* ── BOT NAMES ────────────────────────────────────────────────────── */
/** Pool of human-sounding usernames used for the fallback opponent, so it
 * reads like any other player rather than an obvious "Bot #3". */
let BOT_NAMES = ['Guest417', 'Player882', 'Newcomer19'];
try {
  const raw = fs.readFileSync(path.join(__dirname, 'names.json'), 'utf8');
  const parsed = JSON.parse(raw);
  if (Array.isArray(parsed) && parsed.length) BOT_NAMES = parsed;
} catch (e) {
  console.warn('[arena] names.json missing/invalid, falling back to a tiny built-in name list.');
}
const recentBotNames = []; // small rolling window to avoid back-to-back repeats
function pickBotName() {
  let name;
  let attempts = 0;
  do {
    name = BOT_NAMES[Math.floor(Math.random() * BOT_NAMES.length)];
    attempts++;
  } while (recentBotNames.includes(name) && attempts < 8 && BOT_NAMES.length > recentBotNames.length);
  recentBotNames.push(name);
  if (recentBotNames.length > Math.min(6, Math.max(1, BOT_NAMES.length - 1))) recentBotNames.shift();
  return name;
}

const supabase = HAS_SUPABASE
  ? createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, { auth: { persistSession: false } })
  : null;

if (!HAS_SUPABASE) {
  console.warn('[arena] SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY not set, running in GUEST-ONLY mode.');
  console.warn('[arena] Wins/losses/gold/gems/collection will NOT persist. See .env.example.');
}

/** Keeps the `chat_image_catalog` table in Postgres in sync with this
 * server's own image.json. That table exists solely so the global-chat
 * insert policy (global chat is written directly by the client to
 * Supabase and never touches this server) can reject a `{image:<id>}`
 * token naming anything outside the official catalog, battle/guild/DM
 * chat get the same check in-process via isAllowedChatText, but global
 * chat has no server in the loop to ask, so the check has to live in the
 * database itself. image.json here stays the single source of truth;
 * this just mirrors it. */
async function syncChatImageCatalog() {
  if (!HAS_SUPABASE) return;
  try {
    const ids = [...CHAT_IMAGE_IDS];
    if (ids.length) {
      const { error: upsertErr } = await supabase.from('chat_image_catalog').upsert(ids.map(id => ({ id, updated_at: new Date().toISOString() })));
      if (upsertErr) throw upsertErr;
    }
    const { data: existing, error: selectErr } = await supabase.from('chat_image_catalog').select('id');
    if (selectErr) throw selectErr;
    const stale = (existing || []).map(r => r.id).filter(id => !CHAT_IMAGE_IDS.has(id));
    if (stale.length) {
      const { error: deleteErr } = await supabase.from('chat_image_catalog').delete().in('id', stale);
      if (deleteErr) throw deleteErr;
    }
    console.log(`[arena] chat image catalog synced (${ids.length} official image${ids.length === 1 ? '' : 's'})`);
  } catch (e) {
    console.error('[arena] chat image catalog sync failed, global chat image posts may be rejected until this is fixed', e);
  }
}

/* ── IN-MEMORY REGISTRIES ────────────────────────────────────────── */
/** userId -> Connection */
const connections = new Map();
/** userId -> Match */
const activeMatchByUser = new Map();
/** matchId -> Match */
const matches = new Map();
/** FIFO queues of userIds waiting for an opponent, kept separate so a
 * ranked player is never paired against someone who queued casual, and
 * vice versa. */
const rankedQueue = [];
const casualQueue = [];
/** userId -> 'ranked' | 'casual', tracking which queue a waiting user is in
 * so queue_leave/disconnect/duel-accept can pull them out of the right one
 * without having to search both every time. */
const queueMode = new Map();
function queueForMode(mode) { return mode === 'casual' ? casualQueue : rankedQueue; }
/** Removes a user from whichever queue (if any) they're currently sitting
 * in. Safe to call unconditionally, used from every place a user needs to
 * be pulled out of matchmaking (leave, disconnect, duel accept, etc). */
function removeFromQueues(userId) {
  const mode = queueMode.get(userId);
  const q = queueForMode(mode);
  const i = q.indexOf(userId);
  if (i !== -1) q.splice(i, 1);
  queueMode.delete(userId);
}
/** userId -> pending bot-fallback Timeout, armed while that user sits in a queue */
const queueTimers = new Map();
/** targetUserId -> requesterUserId, at most one live incoming duel invite
 * tracked per target; a newer invite simply replaces an older unanswered one. */
const pendingDuels = new Map();
/** targetUserId -> requesterUserId, same shape as pendingDuels, but for
 * trade invites. A player can have at most one pending trade AND one
 * pending duel at a time, tracked independently. */
const pendingTrades = new Map();
/** tradeId -> TradeSession, live trade negotiations. */
const tradeSessions = new Map();
/** userId -> TradeSession, at most one active trade per user, mirroring
 * activeMatchByUser so "already trading"/"already in a match" checks read
 * the same way everywhere. */
const activeTradeByUser = new Map();
/** userId -> matchId, at most one live spectate session per viewer;
 * starting a new one silently replaces whatever they were watching before. */
const spectatingUserMatch = new Map();

/** Sends to literally every connected client, used only for the
 * lightweight "this player is now in/out of a match" presence blip that
 * powers the purple spectate-eye indicator client-side. Small enough scale
 * here that a full broadcast is simpler and cheaper than targeted fan-out. */
function broadcastAll(payload) {
  for (const c of connections.values()) c.send(payload);
}

/* Presence: a user only counts as online when BOTH a live WS connection
 * exists on this process AND its last heartbeat is recent. The Supabase
 * `presence` table (guest-mode fallback: guestPresence) is the source of
 * truth for "recent" so this also works across a restart / multiple
 * server instances, per the design in supabase-schema.sql. */
const PRESENCE_HEARTBEAT_TIMEOUT_MS = 5 * 60 * 1000; // no heartbeat in this long => offline
const PRESENCE_SWEEP_MS = 60 * 1000;
const guestPresence = new Map(); // userId -> { lastHeartbeat, online }, only used when Supabase isn't configured
const guestFriendships = new Map(); // pairKey -> { status:'pending'|'accepted', requestedBy, createdAt }, guest-mode fallback

/** Guild registries, only used when Supabase isn't configured. Mirrors the
 * shape of the real tables closely enough that the data-layer functions
 * below can branch on HAS_SUPABASE the same way every other feature does. */
const guestGuilds = new Map();              // guildId -> { id, name, leaderId, icon, frame, visibility, joinFeeEnabled, joinFeeCurrency, joinFeeAmount, createdAt }
const guestGuildMembers = new Map();         // guildId -> Map(userId -> { role, joinedAt })
const guestUserGuild = new Map();            // userId -> guildId (a player is in at most one guild)
const guestGuildApplications = new Map();    // guildId -> Map(userId -> { createdAt })
const guestUserApplication = new Map();      // userId -> guildId (at most one pending application at a time)
const guestGuildInvites = new Map();         // guildId -> Map(userId -> { invitedBy, createdAt })
const guestUserInvite = new Map();           // userId -> guildId (at most one pending invite at a time)
const guestGuildChatMessages = new Map();    // guildId -> Array<{ id, userId, message, createdAt }>, oldest first
const guildChatLastSentAt = new Map();       // userId -> ms timestamp of their last chat message (simple per-user rate limit)

/** Marketplace registries, only used when Supabase isn't configured.
 * Mirrors marketplace_listings/marketplace_bids/direct_messages closely
 * enough that the data-layer functions below branch on HAS_SUPABASE the
 * same way every other feature does. */
const guestListings = new Map();             // listingId -> listing object (camelCase, see rowToListing shape)
const guestBids = new Map();                 // listingId -> Array<{id,bidderId,amount,createdAt}>, oldest first
const guestDMs = new Map();                  // pairKey(a,b) -> Array<message>, oldest first
const guestDMArchived = new Map();           // `${ownerId}|${otherId}` -> archivedAt ISO string

/** Tournament registries, only used when Supabase isn't configured.
 * Mirrors tournament_events/tournament_registrations/tournament_brackets
 * closely enough that the data-layer functions below branch on
 * HAS_SUPABASE the same way every other feature does. */
const guestTournamentEvents = new Map();         // eventId -> event object (camelCase, see rowToTournamentEvent shape)
const guestTournamentRegistrations = new Map();  // eventId -> Map(userId -> registration object)
const guestTournamentBrackets = new Map();       // bracketId -> bracket object (camelCase, see rowToBracket shape)

/** Quest registries, only used when Supabase isn't configured. Mirrors
 * player_quest_progress/player_quest_bars, keyed the same way the real
 * tables are (owner_id+quest_id+period_key / owner_id+scope+period_key)
 * so the data-layer functions below branch on HAS_SUPABASE like everything
 * else. See "QUEST SYSTEM" further down for the actual logic. */
const guestQuestProgress = new Map(); // `${userId}|${questId}|${periodKey}` -> { progress, completed, rewarded }
const guestQuestBars = new Map();     // `${userId}|${scope}|${periodKey}` -> { points, claimedMilestones }

let nextGuestId = 1;

/* ── PROFILE CUSTOMIZATION (validated allow-lists) ────────────────── */
// The server is the only thing that ever writes these fields, and it only
// ever accepts values from these lists, an emoji/theme the client didn't
// offer never reaches Postgres, no matter what a modified client sends.
// Icon values are ids (not emoji), the client maps each id to a custom SVG
// glyph it draws itself. Keep this list in sync with ICON_SVGS in docs/index.html.
const PROFILE_ICONS_FREE = ['star','crown','skull','flame','blade','shield','moon','ward','thorn','storm','spider','scorpion','beetle','serpent','laurel'];
const PROFILE_BANNERS_FREE = ['violet','crimson','emerald','gold','azure','obsidian','rose','storm'];
// Quest-locked cosmetics, only reachable by completing the matching
// permanent quest (see game-engine.js's QUEST_DEFS reward.icon/reward.banner
// fields, and grantQuestCosmetics, which is what actually unlocks these).
// Valid to EQUIP only once actually unlocked (see updateProfile), being in
// this list just means the id exists/is drawable, same as the free set.
const PROFILE_ICONS_LOCKED = ['first_card','collector','overlord_master','merchant','flipper','nemesis','pack_fiend','lucky_bastard'];
const PROFILE_BANNERS_LOCKED = ['first_pack','archivist','first_sale','market_crash','hello_world','guildgoer','fabled_pull','absolute'];
const PROFILE_ICONS = [...PROFILE_ICONS_FREE, ...PROFILE_ICONS_LOCKED];
const PROFILE_BANNERS = [...PROFILE_BANNERS_FREE, ...PROFILE_BANNERS_LOCKED];
const BIO_MAX = 140;
const USERNAME_MAX = 24;
const FAVORITES_MAX = 3;

/* ── COSMETIC UNLOCKS (quest-granted icons/banners) ───────────────────
 * Small bit of bookkeeping, same posture as the other quest support
 * tables: server-only source of truth for which quest-locked icons/banners
 * a player has actually earned (see grantQuestCosmetics, called from
 * recordQuestEvent/recordQuestThreshold the moment a permanent quest with
 * a reward.icon/reward.banner completes, NOT gated behind claim_quest,
 * same as that quest's currency: "permanent quests give immediate
 * currency... and profile banners/icons" per spec). */
const guestCosmeticUnlocks = new Map(); // userId -> { icons:Set, banners:Set }
async function grantCosmeticUnlock(userId, kind, itemId) {
  if (!itemId) return;
  if (!HAS_SUPABASE) {
    let u = guestCosmeticUnlocks.get(userId);
    if (!u) { u = { icons: new Set(), banners: new Set() }; guestCosmeticUnlocks.set(userId, u); }
    (kind === 'icon' ? u.icons : u.banners).add(itemId);
    return;
  }
  await supabase.from('player_cosmetic_unlocks').upsert({ owner_id: userId, kind, item_id: itemId }, { onConflict: 'owner_id,kind,item_id' });
}
async function getCosmeticUnlocks(userId) {
  if (!HAS_SUPABASE) {
    const u = guestCosmeticUnlocks.get(userId);
    return { icons: u ? [...u.icons] : [], banners: u ? [...u.banners] : [] };
  }
  const { data } = await supabase.from('player_cosmetic_unlocks').select('kind,item_id').eq('owner_id', userId);
  const icons = [], banners = [];
  for (const row of (data || [])) (row.kind === 'icon' ? icons : banners).push(row.item_id);
  return { icons, banners };
}
/** Called wherever a permanent quest's completion is processed, grants
 * any icon/banner named in its reward. A no-op for quests whose reward is
 * currency-only (most of them). */
async function grantQuestCosmetics(userId, reward) {
  if (isBotId(userId) || !reward) return;
  if (reward.icon) await grantCosmeticUnlock(userId, 'icon', reward.icon);
  if (reward.banner) await grantCosmeticUnlock(userId, 'banner', reward.banner);
}

/* ── GUILDS (validated allow-lists, same posture as profile icons/banners) ─
 * `icon` is the emblem drawn in the middle (reuses the same hand-drawn SVG
 * glyph set as player profiles, never emoji). `frame` is a separate
 * decorative border drawn around it; the client maps each id to its own
 * SVG ring/border shape. Keep both lists in sync with GUILD_ICON_SVGS /
 * GUILD_FRAME_SVGS in docs/index.html. */
const GUILD_ICONS = PROFILE_ICONS;
const GUILD_FRAMES = ['ring','hex','shield','crest','laurel','spiked','ironclad','gilded'];
const GUILD_NAME_MIN = 3;
const GUILD_NAME_MAX = 24;
const GUILD_MAX_MEMBERS = 30;
const GUILD_CREATE_COST_GEMS = Number(process.env.GUILD_CREATE_COST_GEMS) || 200;
const GUILD_JOIN_FEE_MAX_GOLD = 100000;
const GUILD_JOIN_FEE_MAX_GEMS = 10000;
const GUILD_CHAT_MESSAGE_MAX = 300;
const GUILD_CHAT_HISTORY_LIMIT = 100;
const GUILD_CHAT_RETENTION_MS = Number(process.env.GUILD_CHAT_RETENTION_MS) || 7 * 24 * 60 * 60 * 1000; // messages auto-delete after 7 days
const GUILD_CHAT_RATE_LIMIT_MS = 800; // per-user minimum gap between messages

/* ── MARKETPLACE + DIRECT MESSAGES ────────────────────────────────── */
const MARKET_TAX_NORMAL = 0.10;       // paid by buyer on top, taken from seller's earnings
const MARKET_TAX_SAME_GUILD = 0.05;   // reduced rate when buyer + seller share a guild
const MARKET_MIN_DURATION_DAYS = 1;
const MARKET_MAX_DURATION_DAYS = 14;
const MARKET_MAX_AMOUNT = 10_000_000; // sanity ceiling on price/bid fields
const MARKET_SWEEP_MS = 30_000;       // how often expired listings/auctions get settled
const MARKET_BROWSE_LIMIT = 200;
const MARKET_MY_LISTINGS_LIMIT = 100;
const DM_MESSAGE_MAX = 300;
const DM_HISTORY_LIMIT = 200;
const DM_CONVERSATIONS_LIMIT = 50;
const DM_LISTING_MESSAGE_RETENTION_MS = 60 * 60 * 1000; // messages tied to a listing are purged 1hr after it's settled
const DM_CLEANUP_SWEEP_MS = 10 * 60 * 1000;              // how often that purge runs

/* ── TOURNAMENTS ──────────────────────────────────────────────────────
 * Two flavors of "event" (the thing you register into ahead of time):
 *   - official_daily / official_weekly: the server keeps exactly one
 *     upcoming slot of each open for registration at all times (see
 *     ensureUpcomingOfficialEvents). Cheap, fixed entry fee, always caps
 *     each actual bracket at TOURNAMENT_BRACKET_SIZE players, an event
 *     that draws more entrants than that just produces more brackets, all
 *     running in parallel, each with its own independent prize pool.
 *   - unofficial: a player sets everything (name/cap/cut/time/fee) and
 *     registration itself is capped at their chosen player count, so an
 *     unofficial event is always exactly one bracket.
 * Both kinds share the exact same lock/shard/bracket/payout machinery,
 * see lockAndShardEvent(), they only differ in where their settings came
 * from (fixed constants vs. a validated player submission). */
const TOURNAMENT_BRACKET_SIZE = 16;                 // official bracket cap; also the max an unofficial host can choose
const TOURNAMENT_OFFICIAL_PRIZE_PERCENT = 80;        // official: winner takes 80% of the pool, 20% is a currency sink (same idea as bazaar tax)
const TOURNAMENT_DAILY_ENTRY_GOLD = Number(process.env.TOURNAMENT_DAILY_ENTRY_GOLD) || 15;   // within the "10-25 coins" range, cheap enough to be a daily habit
const TOURNAMENT_WEEKLY_ENTRY_GEMS = Number(process.env.TOURNAMENT_WEEKLY_ENTRY_GEMS) || 5;
const TOURNAMENT_DAILY_HOUR_UTC = Number(process.env.TOURNAMENT_DAILY_HOUR_UTC ?? 20);       // 20:00 UTC daily
const TOURNAMENT_WEEKLY_DAY_UTC = Number(process.env.TOURNAMENT_WEEKLY_DAY_UTC ?? 0);        // 0 = Sunday
const TOURNAMENT_WEEKLY_HOUR_UTC = Number(process.env.TOURNAMENT_WEEKLY_HOUR_UTC ?? 20);
const TOURNAMENT_MIN_PLAYERS_TO_RUN = 2;             // fewer checked-in than this and the whole event just refunds + cancels, there's no bracket to run
const TOURNAMENT_UNOFFICIAL_MIN_PLAYERS = 2;
const TOURNAMENT_UNOFFICIAL_MAX_PLAYERS = TOURNAMENT_BRACKET_SIZE;
const TOURNAMENT_UNOFFICIAL_PRIZE_PERCENT_MIN = 50;  // floor exists so a host can't pocket almost the whole pool and leave players playing for scraps, the rest above the winner's cut goes to the host, not burned
const TOURNAMENT_UNOFFICIAL_PRIZE_PERCENT_MAX = 100;
const TOURNAMENT_UNOFFICIAL_ENTRY_MAX_GOLD = 5000;
const TOURNAMENT_UNOFFICIAL_ENTRY_MAX_GEMS = 500;
const TOURNAMENT_UNOFFICIAL_MIN_LEAD_MS = 5 * 60 * 1000;         // must be scheduled at least 5 minutes out
const TOURNAMENT_UNOFFICIAL_MAX_LEAD_MS = 14 * 24 * 60 * 60 * 1000; // and no more than 2 weeks out
const TOURNAMENT_NAME_MIN = 3;
const TOURNAMENT_NAME_MAX = 40;
const TOURNAMENT_MAX_HOSTED_ACTIVE = 3;              // per-user cap on not-yet-started unofficial tournaments they're hosting, spam guard
const TOURNAMENT_SWEEP_MS = 15_000;                  // how often we check for events whose start_at has arrived
const TOURNAMENT_LIST_LIMIT = 40;
const TOURNAMENT_MINE_LIMIT = 20;

/** Picks out only the whitelisted, well-formed fields from a client's
 * `update_profile` message. Anything absent or invalid is simply omitted
 * rather than erroring, so a client can update just one field at a time. */
function sanitizeProfileFields(msg) {
  const out = {};
  if (typeof msg.username === 'string') {
    const name = msg.username.trim().slice(0, USERNAME_MAX);
    if (name.length) out.username = name;
  }
  if (typeof msg.icon === 'string' && PROFILE_ICONS.includes(msg.icon)) out.icon = msg.icon;
  if (typeof msg.banner === 'string' && PROFILE_BANNERS.includes(msg.banner)) out.banner = msg.banner;
  if (typeof msg.bio === 'string') out.bio = msg.bio.trim().slice(0, BIO_MAX);
  return out;
}

/** Favorite cards must actually be owned, checked against the caller's own
 * collection, never trusted from the client. */
function sanitizeFavorites(favoriteCards, ownedSet) {
  if (!Array.isArray(favoriteCards)) return undefined;
  return [...new Set(favoriteCards)].filter(id => ownedSet.has(id)).slice(0, FAVORITES_MAX);
}

/* ── PROFILE LAYER (Supabase-backed, guest fallback) ─────────────── */
const guestProfiles = new Map(); // only used when Supabase isn't configured

async function fetchProfile(userId, fallbackName) {
  if (!HAS_SUPABASE) {
    if (!guestProfiles.has(userId)) {
      guestProfiles.set(userId, { id: userId, username: fallbackName || `Guest${nextGuestId++}`,
        gold: 0, gems: 0, wins: 0, losses: 0, rankPoints: 0, icon: 'star', banner: 'violet', bio: '', favoriteCards: [],
        collection: seedStarterIds(), deck: [], tutorialBonusClaimed: false });
    }
    const p = guestProfiles.get(userId);
    const unlocks = await getCosmeticUnlocks(userId);
    return { ...p, rank: Engine.getRank(p.rankPoints || 0),
      unlockedIcons: [...PROFILE_ICONS_FREE, ...unlocks.icons], unlockedBanners: [...PROFILE_BANNERS_FREE, ...unlocks.banners] };
  }
  let { data: profile, error } = await supabase.from('profiles').select('*').eq('id', userId).maybeSingle();
  if (error) throw error;
  if (!profile) {
    const insert = { id: userId, username: fallbackName || `Pestmaster${nextGuestId++}`, gold: 0, gems: 0, wins: 0, losses: 0, rank_points: 0 };
    const { data: created, error: insErr } = await supabase.from('profiles').insert(insert).select('*').single();
    if (insErr) throw insErr;
    profile = created;
    // seed starter collection for brand-new accounts, one jsonb row, not
    // one row per starter card
    const starter = seedStarterIds();
    await supabase.from('player_cards').upsert(
      { owner_id: userId, cards: collectionCounts(starter) },
      { onConflict: 'owner_id' }
    );
  }
  const { data: cardsRow } = await supabase.from('player_cards').select('cards').eq('owner_id', userId).maybeSingle();
  const { data: deckRow } = await supabase.from('player_decks').select('card_ids').eq('owner_id', userId).maybeSingle();
  const rankPoints = profile.rank_points || 0;
  const unlocks = await getCosmeticUnlocks(userId);
  return {
    id: profile.id, username: profile.username, gold: profile.gold, gems: profile.gems,
    wins: profile.wins, losses: profile.losses, rankPoints, rank: Engine.getRank(rankPoints),
    icon: profile.icon || 'star', banner: profile.banner || 'violet', bio: profile.bio || '',
    favoriteCards: profile.favorite_cards || [],
    collection: Object.entries(cardsRow?.cards || {}).flatMap(([id, qty]) => Array(qty).fill(id)),
    deck: (deckRow && deckRow.card_ids) || [],
    unlockedIcons: [...PROFILE_ICONS_FREE, ...unlocks.icons], unlockedBanners: [...PROFILE_BANNERS_FREE, ...unlocks.banners],
  };
}

/** Validates and persists a profile customization update (name/icon/banner/
 * bio/favorite cards). Silently drops anything that fails validation rather
 * than erroring the whole request, then returns a fresh full profile
 * snapshot so the client can re-render from one source of truth. */
async function updateProfile(userId, msg, ownedSet) {
  const fields = sanitizeProfileFields(msg);
  const favoriteCards = sanitizeFavorites(msg.favoriteCards, ownedSet);

  // Icon/banner must actually be UNLOCKED, not just a recognized id,
  // sanitizeProfileFields already guarantees it's a real id from
  // PROFILE_ICONS/PROFILE_BANNERS, but a quest-locked one additionally
  // requires having actually completed its quest. A modified client
  // trying to equip one it hasn't earned just has the field dropped here,
  // same "silently ignored" posture as any other invalid field.
  if (fields.icon && PROFILE_ICONS_LOCKED.includes(fields.icon)) {
    const unlocks = await getCosmeticUnlocks(userId);
    if (!unlocks.icons.includes(fields.icon)) delete fields.icon;
  }
  if (fields.banner && PROFILE_BANNERS_LOCKED.includes(fields.banner)) {
    const unlocks = await getCosmeticUnlocks(userId);
    if (!unlocks.banners.includes(fields.banner)) delete fields.banner;
  }

  if (!HAS_SUPABASE) {
    const p = guestProfiles.get(userId);
    if (p) {
      Object.assign(p, fields);
      if (favoriteCards !== undefined) p.favoriteCards = favoriteCards;
    }
    if (Object.keys(fields).length || favoriteCards !== undefined) {
      recordQuestEvent(userId, Engine.QUEST_TRACK.PROFILE_CUSTOMIZE, 1).catch(e => console.error('[arena] profile_customize quest event failed', e));
    }
    return fetchProfile(userId);
  }

  const dbFields = { ...fields };
  if (favoriteCards !== undefined) dbFields.favorite_cards = favoriteCards;
  if (Object.keys(dbFields).length) {
    const { error } = await supabase.from('profiles').update(dbFields).eq('id', userId);
    if (error) throw error;
    recordQuestEvent(userId, Engine.QUEST_TRACK.PROFILE_CUSTOMIZE, 1).catch(e => console.error('[arena] profile_customize quest event failed', e));
  }
  return fetchProfile(userId);
}
/* ── FRIENDS + PRESENCE LAYER (Supabase-backed, guest fallback) ───────
 * Every mutation still flows through a WebSocket message like everything
 * else in this file, this just decides where the resulting row lives.
 * Presence and the friendships themselves persist in Supabase (see
 * supabase-schema.sql); friend requests, accept/decline, unfriend, and
 * duels are ordinary WS request/response, same as deploy/attack/end_turn. */

const pairKey = (a, b) => (a < b ? `${a}|${b}` : `${b}|${a}`);

/** Case-insensitive exact-username lookup, used for "add friend by name." */
async function findUserIdByUsername(username) {
  const name = String(username || '').trim();
  if (!name) return null;
  if (!HAS_SUPABASE) {
    for (const [id, p] of guestProfiles) {
      if (p.username.toLowerCase() === name.toLowerCase()) return id;
    }
    return null;
  }
  const { data, error } = await supabase.from('profiles').select('id').ilike('username', name).limit(1).maybeSingle();
  if (error) throw error;
  return data ? data.id : null;
}

/** Lightweight {username,icon} lookup for many ids at once, a friends
 * list has no business pulling everyone's full collection/deck the way
 * fetchProfile does. */
async function fetchProfileSummaries(userIds) {
  const ids = [...new Set(userIds)];
  const out = new Map();
  if (!ids.length) return out;
  if (!HAS_SUPABASE) {
    for (const id of ids) {
      const p = guestProfiles.get(id);
      out.set(id, { username: p ? p.username : 'Unknown', icon: (p && p.icon) || 'star', rank: Engine.getRank(p ? (p.rankPoints || 0) : 0) });
    }
    return out;
  }
  const { data, error } = await supabase.from('profiles').select('id,username,icon,rank_points').in('id', ids);
  if (error) throw error;
  for (const row of data || []) out.set(row.id, { username: row.username, icon: row.icon || 'star', rank: Engine.getRank(row.rank_points || 0) });
  for (const id of ids) if (!out.has(id)) out.set(id, { username: 'Unknown', icon: 'star', rank: Engine.getRank(0) });
  return out;
}

/** Returns { status, requestedBy } for the relationship between two users, or null. */
async function getFriendship(userId, otherId) {
  if (!HAS_SUPABASE) return guestFriendships.get(pairKey(userId, otherId)) || null;
  const [a, b] = userId < otherId ? [userId, otherId] : [otherId, userId];
  const { data, error } = await supabase.from('friendships').select('status,requested_by').eq('user_a', a).eq('user_b', b).maybeSingle();
  if (error) throw error;
  return data ? { status: data.status, requestedBy: data.requested_by } : null;
}

async function createFriendRequest(fromId, toId) {
  if (!HAS_SUPABASE) {
    guestFriendships.set(pairKey(fromId, toId), { status: 'pending', requestedBy: fromId, createdAt: Date.now() });
    return;
  }
  const [a, b] = fromId < toId ? [fromId, toId] : [toId, fromId];
  const { error } = await supabase.from('friendships').insert({ user_a: a, user_b: b, status: 'pending', requested_by: fromId });
  if (error) throw error;
}

async function acceptFriendRequest(userId, otherId) {
  if (!HAS_SUPABASE) {
    const row = guestFriendships.get(pairKey(userId, otherId));
    if (row) { row.status = 'accepted'; row.respondedAt = Date.now(); }
    return;
  }
  const [a, b] = userId < otherId ? [userId, otherId] : [otherId, userId];
  const { error } = await supabase.from('friendships').update({ status: 'accepted', responded_at: new Date().toISOString() }).eq('user_a', a).eq('user_b', b);
  if (error) throw error;
}

/** Deletes the relationship regardless of status, covers unfriending an
 * accepted friend, cancelling your own outgoing request, and declining an
 * incoming one, since none of those need a lingering row. */
async function deleteFriendship(userId, otherId) {
  if (!HAS_SUPABASE) { guestFriendships.delete(pairKey(userId, otherId)); return; }
  const [a, b] = userId < otherId ? [userId, otherId] : [otherId, userId];
  const { error } = await supabase.from('friendships').delete().eq('user_a', a).eq('user_b', b);
  if (error) throw error;
}

/** Every relationship (any status) involving userId, from that user's point of view. */
async function listFriendshipRows(userId) {
  if (!HAS_SUPABASE) {
    const rows = [];
    for (const [key, row] of guestFriendships) {
      const [a, b] = key.split('|');
      if (a === userId || b === userId) rows.push({ otherId: a === userId ? b : a, status: row.status, requestedBy: row.requestedBy });
    }
    return rows;
  }
  const { data, error } = await supabase.from('friendships').select('*').or(`user_a.eq.${userId},user_b.eq.${userId}`);
  if (error) throw error;
  return (data || []).map(r => ({ otherId: r.user_a === userId ? r.user_b : r.user_a, status: r.status, requestedBy: r.requested_by }));
}

async function markPresenceOnline(userId) {
  if (!HAS_SUPABASE) { guestPresence.set(userId, { lastHeartbeat: Date.now(), online: true }); return; }
  const { error } = await supabase.from('presence').upsert(
    { user_id: userId, last_heartbeat: new Date().toISOString(), online: true }, { onConflict: 'user_id' }
  );
  if (error) throw error;
}

async function markPresenceOffline(userId) {
  if (!HAS_SUPABASE) { const p = guestPresence.get(userId); if (p) p.online = false; return; }
  const { error } = await supabase.from('presence').upsert(
    { user_id: userId, last_heartbeat: new Date().toISOString(), online: false }, { onConflict: 'user_id' }
  );
  if (error) throw error;
}

/** Batched online check, a user counts as online only if their presence
 * row says so AND its heartbeat hasn't gone stale. */
async function onlineStatusBatch(userIds) {
  const ids = [...new Set(userIds)];
  const out = new Map();
  if (!ids.length) return out;
  const now = Date.now();
  if (!HAS_SUPABASE) {
    for (const id of ids) {
      const p = guestPresence.get(id);
      out.set(id, !!(p && p.online && (now - p.lastHeartbeat) < PRESENCE_HEARTBEAT_TIMEOUT_MS));
    }
    return out;
  }
  const { data, error } = await supabase.from('presence').select('user_id,online,last_heartbeat').in('user_id', ids);
  if (error) throw error;
  for (const row of data || []) {
    out.set(row.user_id, !!(row.online && (now - new Date(row.last_heartbeat).getTime()) < PRESENCE_HEARTBEAT_TIMEOUT_MS));
  }
  for (const id of ids) if (!out.has(id)) out.set(id, false);
  return out;
}

/** Pushes a presence flip to every online friend's live connection,
 * there's no need to persist this event, only the resulting row. */
async function broadcastPresence(userId, online) {
  try {
    const rows = await listFriendshipRows(userId);
    for (const r of rows) {
      if (r.status !== 'accepted') continue;
      const c = connections.get(r.otherId);
      if (c) c.send({ type: 'presence_update', userId, online });
    }
  } catch (e) { console.error('[arena] broadcastPresence failed', e); }
}

/** Full friends_list payload: accepted friends (with live online status),
 * plus incoming/outgoing pending requests. */
async function buildFriendsList(userId) {
  const rows = await listFriendshipRows(userId);
  const friends = rows.filter(r => r.status === 'accepted');
  const incoming = rows.filter(r => r.status === 'pending' && r.requestedBy !== userId);
  const outgoing = rows.filter(r => r.status === 'pending' && r.requestedBy === userId);
  const [summaries, online] = await Promise.all([
    fetchProfileSummaries(rows.map(r => r.otherId)),
    onlineStatusBatch(friends.map(r => r.otherId)),
  ]);
  const toEntry = withOnline => r => ({
    userId: r.otherId,
    username: summaries.get(r.otherId)?.username || 'Unknown',
    icon: summaries.get(r.otherId)?.icon || 'star',
    rank: summaries.get(r.otherId)?.rank || Engine.getRank(0),
    ...(withOnline ? { online: !!online.get(r.otherId), inMatch: activeMatchByUser.has(r.otherId) } : {}),
  });
  return {
    friends: friends.map(toEntry(true)),
    incoming: incoming.map(toEntry(false)),
    outgoing: outgoing.map(toEntry(false)),
  };
}

/* ── GUILDS LAYER (Supabase-backed, guest fallback) ───────────────────
 * A player is in at most one guild at a time (enforced by the unique
 * `user_id` constraint on guild_members, and by the guest-mode maps
 * mirroring it). Everything here is an ordinary WS request/response, same
 * as friends, the client never talks to these tables directly. */

/** Case-insensitive exact-name lookup, used to reject duplicate guild names
 * before ever attempting an insert. */
async function findGuildByName(name) {
  if (!HAS_SUPABASE) {
    for (const g of guestGuilds.values()) if (g.name.toLowerCase() === name.toLowerCase()) return g;
    return null;
  }
  const { data, error } = await supabase.from('guilds').select('*').ilike('name', name).maybeSingle();
  if (error) throw error;
  return data ? rowToGuild(data) : null;
}

function rowToGuild(row) {
  return {
    id: row.id, name: row.name, leaderId: row.leader_id, icon: row.icon, frame: row.frame,
    visibility: row.visibility, joinFeeEnabled: row.join_fee_enabled,
    joinFeeCurrency: row.join_fee_currency, joinFeeAmount: row.join_fee_amount, createdAt: row.created_at,
  };
}

async function getGuildById(guildId) {
  if (!HAS_SUPABASE) return guestGuilds.get(guildId) || null;
  const { data, error } = await supabase.from('guilds').select('*').eq('id', guildId).maybeSingle();
  if (error) throw error;
  return data ? rowToGuild(data) : null;
}

/** {guildId, role} for whatever guild userId currently belongs to, or null. */
async function getGuildMembership(userId) {
  if (!HAS_SUPABASE) {
    const guildId = guestUserGuild.get(userId);
    if (!guildId) return null;
    const m = guestGuildMembers.get(guildId)?.get(userId);
    return m ? { guildId, role: m.role } : null;
  }
  const { data, error } = await supabase.from('guild_members').select('guild_id,role').eq('user_id', userId).maybeSingle();
  if (error) throw error;
  return data ? { guildId: data.guild_id, role: data.role } : null;
}

async function countGuildMembers(guildId) {
  if (!HAS_SUPABASE) return guestGuildMembers.get(guildId)?.size || 0;
  const { count, error } = await supabase.from('guild_members').select('user_id', { count: 'exact', head: true }).eq('guild_id', guildId);
  if (error) throw error;
  return count || 0;
}

/** Full enriched roster: userId/username/icon/online/role/joinedAt, sorted
 * leader-first then alphabetically, same "who's actually here" shape the
 * friends list already gives the client. */
async function listGuildMembers(guildId) {
  let rows;
  if (!HAS_SUPABASE) {
    rows = [...(guestGuildMembers.get(guildId) || new Map())].map(([userId, m]) => ({ userId, role: m.role, joinedAt: m.joinedAt }));
  } else {
    const { data, error } = await supabase.from('guild_members').select('user_id,role,joined_at').eq('guild_id', guildId);
    if (error) throw error;
    rows = (data || []).map(r => ({ userId: r.user_id, role: r.role, joinedAt: r.joined_at }));
  }
  const [summaries, online] = await Promise.all([
    fetchProfileSummaries(rows.map(r => r.userId)),
    onlineStatusBatch(rows.map(r => r.userId)),
  ]);
  return rows
    .map(r => ({
      userId: r.userId, role: r.role, joinedAt: r.joinedAt,
      username: summaries.get(r.userId)?.username || 'Unknown',
      icon: summaries.get(r.userId)?.icon || 'star',
      online: !!online.get(r.userId),
      inMatch: activeMatchByUser.has(r.userId),
    }))
    .sort((a, b) => (a.role === b.role ? a.username.localeCompare(b.username) : (a.role === 'leader' ? -1 : 1)));
}

async function addGuildMember(guildId, userId, role) {
  if (!HAS_SUPABASE) {
    if (!guestGuildMembers.has(guildId)) guestGuildMembers.set(guildId, new Map());
    guestGuildMembers.get(guildId).set(userId, { role, joinedAt: new Date().toISOString() });
    guestUserGuild.set(userId, guildId);
    return;
  }
  const { error } = await supabase.from('guild_members').insert({ guild_id: guildId, user_id: userId, role });
  if (error) throw error;
}

async function removeGuildMember(guildId, userId) {
  if (!HAS_SUPABASE) {
    guestGuildMembers.get(guildId)?.delete(userId);
    guestUserGuild.delete(userId);
    return;
  }
  const { error } = await supabase.from('guild_members').delete().eq('guild_id', guildId).eq('user_id', userId);
  if (error) throw error;
}

async function setGuildLeader(guildId, newLeaderId) {
  if (!HAS_SUPABASE) {
    const g = guestGuilds.get(guildId); if (g) g.leaderId = newLeaderId;
    const members = guestGuildMembers.get(guildId);
    if (members) for (const [uid, m] of members) m.role = uid === newLeaderId ? 'leader' : 'member';
    return;
  }
  const { error: e1 } = await supabase.from('guilds').update({ leader_id: newLeaderId }).eq('id', guildId);
  if (e1) throw e1;
  const { error: e2 } = await supabase.from('guild_members').update({ role: 'member' }).eq('guild_id', guildId);
  if (e2) throw e2;
  const { error: e3 } = await supabase.from('guild_members').update({ role: 'leader' }).eq('guild_id', guildId).eq('user_id', newLeaderId);
  if (e3) throw e3;
}

async function deleteGuild(guildId) {
  if (!HAS_SUPABASE) {
    guestGuilds.delete(guildId);
    guestGuildMembers.delete(guildId);
    guestGuildApplications.delete(guildId);
    guestGuildInvites.delete(guildId);
    for (const [uid, gid] of guestUserGuild) if (gid === guildId) guestUserGuild.delete(uid);
    for (const [uid, gid] of guestUserApplication) if (gid === guildId) guestUserApplication.delete(uid);
    for (const [uid, gid] of guestUserInvite) if (gid === guildId) guestUserInvite.delete(uid);
    return;
  }
  const { error } = await supabase.from('guilds').delete().eq('id', guildId); // cascades members/applications/invites
  if (error) throw error;
}

/** Validated field extraction shared by guild_create, throws with a `.code`
 * the client can key off of, same convention as saveDeck/grantPack. */
function sanitizeGuildCreateFields(msg) {
  const name = String(msg.name || '').trim();
  if (name.length < GUILD_NAME_MIN || name.length > GUILD_NAME_MAX) {
    const e = new Error('bad_guild_name'); e.code = 'guild_name_invalid'; throw e;
  }
  const icon = GUILD_ICONS.includes(msg.icon) ? msg.icon : GUILD_ICONS[0];
  const frame = GUILD_FRAMES.includes(msg.frame) ? msg.frame : GUILD_FRAMES[0];
  const visibility = msg.visibility === 'private' ? 'private' : 'public';
  let joinFeeEnabled = !!msg.joinFeeEnabled;
  let joinFeeCurrency = null, joinFeeAmount = 0;
  if (joinFeeEnabled) {
    joinFeeCurrency = msg.joinFeeCurrency === 'gems' ? 'gems' : 'gold';
    const max = joinFeeCurrency === 'gems' ? GUILD_JOIN_FEE_MAX_GEMS : GUILD_JOIN_FEE_MAX_GOLD;
    joinFeeAmount = Math.max(0, Math.min(max, Math.floor(Number(msg.joinFeeAmount) || 0)));
    if (joinFeeAmount <= 0) joinFeeEnabled = false; // "enabled" with a 0 amount is just "no fee"
  }
  return { name, icon, frame, visibility, joinFeeEnabled, joinFeeCurrency, joinFeeAmount };
}

/** Creates a new guild, deducting the flat gem cost from the founder first.
 * Founder becomes leader and member #1. Throws with `.code` on any failure
 * (insufficient funds, duplicate name, already in a guild, bad fields),
 * nothing is created or charged unless every check passes. */
async function createGuild(userId, msg) {
  const existing = await getGuildMembership(userId);
  if (existing) { const e = new Error('already_in_guild'); e.code = 'already_in_guild'; throw e; }
  const fields = sanitizeGuildCreateFields(msg);
  if (await findGuildByName(fields.name)) { const e = new Error('guild_name_taken'); e.code = 'guild_name_taken'; throw e; }

  const profile = await fetchProfile(userId);
  if (profile.gems < GUILD_CREATE_COST_GEMS) { const e = new Error('insufficient_funds'); e.code = 'insufficient_funds'; throw e; }
  const newGems = profile.gems - GUILD_CREATE_COST_GEMS;
  if (!HAS_SUPABASE) {
    guestProfiles.get(userId).gems = newGems;
  } else {
    const { error } = await supabase.from('profiles').update({ gems: newGems }).eq('id', userId);
    if (error) throw error;
  }

  let guildId;
  if (!HAS_SUPABASE) {
    guildId = crypto.randomUUID();
    guestGuilds.set(guildId, { id: guildId, leaderId: userId, createdAt: new Date().toISOString(), ...fields });
  } else {
    const { data, error } = await supabase.from('guilds').insert({
      name: fields.name, leader_id: userId, icon: fields.icon, frame: fields.frame, visibility: fields.visibility,
      join_fee_enabled: fields.joinFeeEnabled, join_fee_currency: fields.joinFeeCurrency, join_fee_amount: fields.joinFeeAmount,
    }).select('*').single();
    if (error) throw error;
    guildId = data.id;
  }
  await addGuildMember(guildId, userId, 'leader');
  return guildId;
}

/** Charges a guild's join fee (if any) to userId. Throws `insufficient_funds`
 * without mutating anything if they can't afford it. No-op if the guild has
 * no fee configured. */
async function chargeJoinFee(guild, userId) {
  if (!guild.joinFeeEnabled || guild.joinFeeAmount <= 0) return;
  const profile = await fetchProfile(userId);
  const balance = guild.joinFeeCurrency === 'gems' ? profile.gems : profile.gold;
  if (balance < guild.joinFeeAmount) { const e = new Error('insufficient_funds'); e.code = 'insufficient_funds'; throw e; }
  const newBalance = balance - guild.joinFeeAmount;
  const field = guild.joinFeeCurrency === 'gems' ? 'gems' : 'gold';
  if (!HAS_SUPABASE) {
    guestProfiles.get(userId)[field] = newBalance;
  } else {
    const { error } = await supabase.from('profiles').update({ [field]: newBalance }).eq('id', userId);
    if (error) throw error;
  }
}

/** Shared join logic (public join, accepted application, accepted invite):
 * re-checks capacity/membership/fee right before actually seating the
 * player, since time may have passed since the original request. */
async function seatNewMember(guildId, userId) {
  if (await getGuildMembership(userId)) { const e = new Error('already_in_guild'); e.code = 'already_in_guild'; throw e; }
  const guild = await getGuildById(guildId);
  if (!guild) { const e = new Error('guild_not_found'); e.code = 'guild_not_found'; throw e; }
  if ((await countGuildMembers(guildId)) >= GUILD_MAX_MEMBERS) { const e = new Error('guild_full'); e.code = 'guild_full'; throw e; }
  await chargeJoinFee(guild, userId);
  await addGuildMember(guildId, userId, 'member');
  recordQuestEvent(userId, Engine.QUEST_TRACK.GUILD_JOIN, 1).catch(e => console.error('[arena] guild_join quest failed', e));
  return guild;
}

/* ── Applications (private guilds: player asks, leader decides) ── */
async function getUserApplication(userId) {
  if (!HAS_SUPABASE) {
    const guildId = guestUserApplication.get(userId);
    return guildId ? { guildId } : null;
  }
  const { data, error } = await supabase.from('guild_applications').select('guild_id').eq('user_id', userId).maybeSingle();
  if (error) throw error;
  return data ? { guildId: data.guild_id } : null;
}

async function createApplication(guildId, userId) {
  if (!HAS_SUPABASE) {
    if (!guestGuildApplications.has(guildId)) guestGuildApplications.set(guildId, new Map());
    guestGuildApplications.get(guildId).set(userId, { createdAt: new Date().toISOString() });
    guestUserApplication.set(userId, guildId);
    return;
  }
  const { error } = await supabase.from('guild_applications').insert({ guild_id: guildId, user_id: userId });
  if (error) throw error;
}

async function deleteApplication(guildId, userId) {
  if (!HAS_SUPABASE) {
    guestGuildApplications.get(guildId)?.delete(userId);
    if (guestUserApplication.get(userId) === guildId) guestUserApplication.delete(userId);
    return;
  }
  const { error } = await supabase.from('guild_applications').delete().eq('guild_id', guildId).eq('user_id', userId);
  if (error) throw error;
}

async function listApplications(guildId) {
  let rows;
  if (!HAS_SUPABASE) {
    rows = [...(guestGuildApplications.get(guildId) || new Map())].map(([userId, a]) => ({ userId, createdAt: a.createdAt }));
  } else {
    const { data, error } = await supabase.from('guild_applications').select('user_id,created_at').eq('guild_id', guildId);
    if (error) throw error;
    rows = (data || []).map(r => ({ userId: r.user_id, createdAt: r.created_at }));
  }
  const summaries = await fetchProfileSummaries(rows.map(r => r.userId));
  return rows.map(r => ({ userId: r.userId, createdAt: r.createdAt, username: summaries.get(r.userId)?.username || 'Unknown', icon: summaries.get(r.userId)?.icon || 'star' }));
}

/* ── Invites (leader reaches out to a specific player) ── */
async function getUserInvite(userId) {
  if (!HAS_SUPABASE) {
    const guildId = guestUserInvite.get(userId);
    return guildId ? { guildId } : null;
  }
  const { data, error } = await supabase.from('guild_invites').select('guild_id,invited_by').eq('user_id', userId).maybeSingle();
  if (error) throw error;
  return data ? { guildId: data.guild_id, invitedBy: data.invited_by } : null;
}

async function createInvite(guildId, userId, invitedBy) {
  if (!HAS_SUPABASE) {
    if (!guestGuildInvites.has(guildId)) guestGuildInvites.set(guildId, new Map());
    guestGuildInvites.get(guildId).set(userId, { invitedBy, createdAt: new Date().toISOString() });
    guestUserInvite.set(userId, guildId);
    return;
  }
  const { error } = await supabase.from('guild_invites').insert({ guild_id: guildId, user_id: userId, invited_by: invitedBy });
  if (error) throw error;
}

async function deleteInvite(guildId, userId) {
  if (!HAS_SUPABASE) {
    guestGuildInvites.get(guildId)?.delete(userId);
    if (guestUserInvite.get(userId) === guildId) guestUserInvite.delete(userId);
    return;
  }
  const { error } = await supabase.from('guild_invites').delete().eq('guild_id', guildId).eq('user_id', userId);
  if (error) throw error;
}

/** Everyone a guild has outstanding invites out to right now, shown only
 * to the leader, so they can see (and cancel) invites they've sent instead
 * of them just silently sitting there until the invitee responds. */
async function listGuildInvites(guildId) {
  let rows;
  if (!HAS_SUPABASE) {
    rows = [...(guestGuildInvites.get(guildId) || new Map())].map(([userId, i]) => ({ userId, createdAt: i.createdAt }));
  } else {
    const { data, error } = await supabase.from('guild_invites').select('user_id,created_at').eq('guild_id', guildId);
    if (error) throw error;
    rows = (data || []).map(r => ({ userId: r.user_id, createdAt: r.created_at }));
  }
  const summaries = await fetchProfileSummaries(rows.map(r => r.userId));
  return rows.map(r => ({ userId: r.userId, createdAt: r.createdAt, username: summaries.get(r.userId)?.username || 'Unknown', icon: summaries.get(r.userId)?.icon || 'star' }));
}

/* ── Guild chat. Persisted, but pruned after 7 days (see the hourly
 * cleanupExpiredGuildChatMessages sweep near server startup below), the
 * read path also defensively re-filters to the last 7 days on every fetch,
 * so a delayed cleanup pass can never surface a stale message either. ── */
async function cleanupExpiredGuildChatMessages() {
  const cutoffIso = new Date(Date.now() - GUILD_CHAT_RETENTION_MS).toISOString();
  if (!HAS_SUPABASE) {
    for (const [guildId, msgs] of guestGuildChatMessages) {
      const kept = msgs.filter(m => m.createdAt >= cutoffIso);
      if (kept.length !== msgs.length) guestGuildChatMessages.set(guildId, kept);
    }
    return;
  }
  const { error } = await supabase.from('guild_chat_messages').delete().lt('created_at', cutoffIso);
  if (error) console.error('[arena] guild chat cleanup failed', error);
}

async function listGuildChatMessages(guildId) {
  const cutoffIso = new Date(Date.now() - GUILD_CHAT_RETENTION_MS).toISOString();
  let rows;
  if (!HAS_SUPABASE) {
    rows = (guestGuildChatMessages.get(guildId) || []).filter(m => m.createdAt >= cutoffIso).slice(-GUILD_CHAT_HISTORY_LIMIT);
  } else {
    const { data, error } = await supabase.from('guild_chat_messages').select('id,user_id,message,created_at')
      .eq('guild_id', guildId).gte('created_at', cutoffIso).order('created_at', { ascending: true }).limit(GUILD_CHAT_HISTORY_LIMIT);
    if (error) throw error;
    rows = (data || []).map(r => ({ id: r.id, userId: r.user_id, message: r.message, createdAt: r.created_at }));
  }
  const summaries = await fetchProfileSummaries(rows.map(r => r.userId));
  return rows.map(r => ({
    id: r.id, userId: r.userId, message: r.message, createdAt: r.createdAt,
    username: summaries.get(r.userId)?.username || 'Unknown', icon: summaries.get(r.userId)?.icon || 'star',
  }));
}

/** Inserts one message and returns it fully enriched (username/icon),
 * exactly the shape the client needs to render it immediately, whether
 * from guild_chat_history or a live guild_chat_message broadcast. */
async function sendGuildChatMessage(guildId, userId, text) {
  const message = String(text || '').trim().slice(0, GUILD_CHAT_MESSAGE_MAX);
  if (!message) { const e = new Error('guild_chat_empty'); e.code = 'guild_chat_empty'; throw e; }
  if (!isAllowedChatText(message)) { const e = new Error('invalid_chat_image'); e.code = 'invalid_chat_image'; throw e; }
  let row;
  if (!HAS_SUPABASE) {
    row = { id: crypto.randomUUID(), userId, message, createdAt: new Date().toISOString() };
    if (!guestGuildChatMessages.has(guildId)) guestGuildChatMessages.set(guildId, []);
    guestGuildChatMessages.get(guildId).push(row);
  } else {
    const { data, error } = await supabase.from('guild_chat_messages').insert({ guild_id: guildId, user_id: userId, message }).select('id,created_at').single();
    if (error) throw error;
    row = { id: data.id, userId, message, createdAt: data.created_at };
  }
  const summary = (await fetchProfileSummaries([userId])).get(userId);
  return { id: row.id, userId, message: row.message, createdAt: row.createdAt, username: summary?.username || 'Unknown', icon: summary?.icon || 'star' };
}

/** Browsable list for the "find a guild" screen: public guilds always show;
 * private guilds show too (so a name search can find them to apply to) but
 * the client is told `visibility` so it renders "Apply" instead of "Join". */
async function browseGuilds(search) {
  let rows;
  if (!HAS_SUPABASE) {
    rows = [...guestGuilds.values()];
    if (search) rows = rows.filter(g => g.name.toLowerCase().includes(search.toLowerCase()));
  } else {
    let q = supabase.from('guilds').select('*').limit(40);
    if (search) q = q.ilike('name', `%${search}%`);
    const { data, error } = await q;
    if (error) throw error;
    rows = (data || []).map(rowToGuild);
  }
  const counts = await Promise.all(rows.map(g => countGuildMembers(g.id)));
  return rows
    .map((g, i) => ({
      guildId: g.id, name: g.name, icon: g.icon, frame: g.frame, visibility: g.visibility,
      memberCount: counts[i], maxMembers: GUILD_MAX_MEMBERS,
      joinFeeEnabled: g.joinFeeEnabled, joinFeeCurrency: g.joinFeeCurrency, joinFeeAmount: g.joinFeeAmount,
    }))
    .sort((a, b) => b.memberCount - a.memberCount)
    .slice(0, 40);
}

/** Full state payload for the caller's own client: their guild (with full
 * roster + pending applications if they lead it), any invite waiting on
 * them, and their own outgoing application status. Exactly one of
 * guild/invite/application is meaningfully populated at a time, since you
 * can't be in a guild AND have a pending application/invite simultaneously. */
async function buildGuildState(userId) {
  const membership = await getGuildMembership(userId);
  if (membership) {
    const guild = await getGuildById(membership.guildId);
    const isLeader = membership.role === 'leader';
    const [members, applications, invitesSent] = await Promise.all([
      listGuildMembers(membership.guildId),
      isLeader ? listApplications(membership.guildId) : Promise.resolve([]),
      isLeader ? listGuildInvites(membership.guildId) : Promise.resolve([]),
    ]);
    return {
      guild: {
        guildId: guild.id, name: guild.name, icon: guild.icon, frame: guild.frame, visibility: guild.visibility,
        joinFeeEnabled: guild.joinFeeEnabled, joinFeeCurrency: guild.joinFeeCurrency, joinFeeAmount: guild.joinFeeAmount,
        myRole: membership.role, members, maxMembers: GUILD_MAX_MEMBERS,
        applications: isLeader ? applications : undefined,
        invitesSent: isLeader ? invitesSent : undefined,
      },
      invite: null, application: null,
    };
  }
  const [invite, application] = await Promise.all([getUserInvite(userId), getUserApplication(userId)]);
  let invitePayload = null, applicationPayload = null;
  if (invite) {
    const g = await getGuildById(invite.guildId);
    if (g) invitePayload = { guildId: g.id, name: g.name, icon: g.icon, frame: g.frame };
  }
  if (application) {
    const g = await getGuildById(application.guildId);
    if (g) applicationPayload = { guildId: g.id, name: g.name, icon: g.icon, frame: g.frame };
  }
  return { guild: null, invite: invitePayload, application: applicationPayload };
}

async function sendGuildState(userId) {
  const conn = connections.get(userId);
  if (conn) { try { conn.send({ type: 'guild_state', ...(await buildGuildState(userId)) }); } catch (e) { console.error('[arena] sendGuildState failed', e); } }
}

/** Pushes a fresh guild_state to every currently-connected member of a
 * guild, used after any join/leave/kick/disband/leadership-change so
 * every open client's roster stays in sync without polling. */
async function broadcastGuildState(guildId) {
  try {
    const members = await listGuildMembers(guildId);
    await Promise.all(members.map(m => sendGuildState(m.userId)));
  } catch (e) { console.error('[arena] broadcastGuildState failed', e); }
}

/* ══════════════════════════════════════════════════════════════════
 * MARKETPLACE + DIRECT MESSAGES
 *
 * A listing escrows the physical card off the seller's collection the
 * moment it's created, so it can't be double-listed, traded away, or
 * spent in a pack re-roll while up for sale. Cancelling/expiring gives
 * it back; a completed sale hands it straight to the buyer.
 *
 * Tax: 10% normal, 5% if buyer + seller share a guild at the moment the
 * deal locks in (bid time for auctions, purchase/offer-accept time for
 * price listings). The buyer pays price+tax; the seller receives
 * price-tax, both cuts use the same rate. Once locked into a listing
 * (tax_rate column), that rate is reused at settlement rather than
 * recomputed, so a bid made while sharing a guild doesn't retroactively
 * lose its discount if someone leaves the guild before the auction ends.
 * ══════════════════════════════════════════════════════════════════ */

function notifyUser(userId, payload) { connections.get(userId)?.send(payload); }

async function guildIdFor(userId) {
  const m = await getGuildMembership(userId);
  return m ? m.guildId : null;
}
function computeTaxRate(buyerGuildId, sellerGuildId) {
  return (buyerGuildId && sellerGuildId && buyerGuildId === sellerGuildId) ? MARKET_TAX_SAME_GUILD : MARKET_TAX_NORMAL;
}

function rowToListing(row) {
  return {
    id: row.id, sellerId: row.seller_id, cardId: row.card_id, listingType: row.listing_type, currency: row.currency,
    price: row.price, startingBid: row.starting_bid, buyoutPrice: row.buyout_price,
    currentBid: row.current_bid, currentBidderId: row.current_bidder_id, currentBidEscrow: row.current_bid_escrow,
    taxRate: row.tax_rate == null ? null : Number(row.tax_rate), status: row.status, durationDays: row.duration_days,
    createdAt: row.created_at, expiresAt: row.expires_at, settledAt: row.settled_at,
  };
}
function listingToRow(l) {
  return {
    id: l.id, seller_id: l.sellerId, card_id: l.cardId, listing_type: l.listingType, currency: l.currency,
    price: l.price, starting_bid: l.startingBid, buyout_price: l.buyoutPrice,
    current_bid: l.currentBid, current_bidder_id: l.currentBidderId, current_bid_escrow: l.currentBidEscrow,
    tax_rate: l.taxRate, status: l.status, duration_days: l.durationDays,
    created_at: l.createdAt, expires_at: l.expiresAt, settled_at: l.settledAt,
  };
}
/** Client-facing shape for a listing, adds display names/icons (from a
 * pre-fetched summaries Map) and never leaks internal escrow bookkeeping
 * (current_bid_escrow) beyond what the UI needs. */
function marketListingPayload(l, summaries) {
  const seller = summaries.get(l.sellerId) || { username: 'Unknown', icon: 'star' };
  const bidder = l.currentBidderId ? (summaries.get(l.currentBidderId) || { username: 'Unknown', icon: 'star' }) : null;
  return {
    id: l.id, sellerId: l.sellerId, sellerName: seller.username, sellerIcon: seller.icon,
    cardId: l.cardId, listingType: l.listingType, currency: l.currency,
    price: l.price, startingBid: l.startingBid, buyoutPrice: l.buyoutPrice,
    currentBid: l.currentBid, currentBidderId: l.currentBidderId,
    currentBidderName: bidder ? bidder.username : null,
    status: l.status, createdAt: l.createdAt, expiresAt: l.expiresAt,
  };
}

async function getListing(listingId) {
  if (!HAS_SUPABASE) return guestListings.get(listingId) || null;
  const { data, error } = await supabase.from('marketplace_listings').select('*').eq('id', listingId).maybeSingle();
  if (error) throw error;
  return data ? rowToListing(data) : null;
}
async function saveListing(listing) {
  if (!HAS_SUPABASE) { guestListings.set(listing.id, listing); return; }
  const { error } = await supabase.from('marketplace_listings').upsert(listingToRow(listing));
  if (error) throw error;
}
async function browseActiveListings(filter) {
  const nowIso = new Date().toISOString();
  if (!HAS_SUPABASE) {
    let rows = [...guestListings.values()].filter(l => l.status === 'active' && l.expiresAt > nowIso);
    if (filter && filter.currency) rows = rows.filter(l => l.currency === filter.currency);
    if (filter && filter.listingType) rows = rows.filter(l => l.listingType === filter.listingType);
    return rows.sort((a, b) => b.createdAt.localeCompare(a.createdAt)).slice(0, MARKET_BROWSE_LIMIT);
  }
  let q = supabase.from('marketplace_listings').select('*').eq('status', 'active').gt('expires_at', nowIso);
  if (filter && filter.currency) q = q.eq('currency', filter.currency);
  if (filter && filter.listingType) q = q.eq('listing_type', filter.listingType);
  const { data, error } = await q.order('created_at', { ascending: false }).limit(MARKET_BROWSE_LIMIT);
  if (error) throw error;
  return (data || []).map(rowToListing);
}
async function listingsBySeller(userId) {
  if (!HAS_SUPABASE) {
    return [...guestListings.values()].filter(l => l.sellerId === userId)
      .sort((a, b) => b.createdAt.localeCompare(a.createdAt)).slice(0, MARKET_MY_LISTINGS_LIMIT);
  }
  const { data, error } = await supabase.from('marketplace_listings').select('*').eq('seller_id', userId)
    .order('created_at', { ascending: false }).limit(MARKET_MY_LISTINGS_LIMIT);
  if (error) throw error;
  return (data || []).map(rowToListing);
}
async function recordBid(listingId, bidderId, amount) {
  if (!HAS_SUPABASE) {
    const arr = guestBids.get(listingId) || [];
    arr.push({ id: crypto.randomUUID(), bidderId, amount, createdAt: new Date().toISOString() });
    guestBids.set(listingId, arr);
    return;
  }
  const { error } = await supabase.from('marketplace_bids').insert({ listing_id: listingId, bidder_id: bidderId, amount });
  if (error) throw error;
}

/** Creates a listing: validates params, escrows one copy of the card off
 * the seller's collection, and persists the row. Returns { error } on any
 * validation failure, never throws for bad input. */
/* ── Marketplace quest hooks (Merchant/Shopping Spree/Flipper/etc.) ──
 * Small bits of bookkeeping that only exist to feed the quest system,
 * see game-engine.js's QUEST_TRACK for what each of these means. */
const guestCardBuyPrice = new Map(); // `${userId}|${cardId}` -> {price, currency}
async function setCardBuyPrice(userId, cardId, price, currency) {
  if (!HAS_SUPABASE) { guestCardBuyPrice.set(`${userId}|${cardId}`, { price, currency }); return; }
  await supabase.from('player_card_buy_price').upsert({ owner_id: userId, card_id: cardId, price, currency }, { onConflict: 'owner_id,card_id' });
}
async function getCardBuyPrice(userId, cardId) {
  if (!HAS_SUPABASE) return guestCardBuyPrice.get(`${userId}|${cardId}`) || null;
  const { data } = await supabase.from('player_card_buy_price').select('price,currency').eq('owner_id', userId).eq('card_id', cardId).maybeSingle();
  return data || null;
}
/** Recomputes a player's unique-card-id count and total owned-overlord
 * count straight from their live collection and feeds both into their
 * respective threshold quests (Collector/Archivist/Overlord Master). Safe
 * to call after any card-gaining event (pack open, market purchase). */
async function computeCollectionQuestThresholds(userId) {
  const profile = await fetchProfile(userId);
  const counts = collectionCounts(profile.collection);
  let overlordCount = 0;
  for (const [id, qty] of Object.entries(counts)) {
    const def = Engine.CardById[id];
    if (def && def.classification === 'overlord') overlordCount += qty;
  }
  await recordQuestThreshold(userId, Engine.QUEST_TRACK.CARDS_UNIQUE, Object.keys(counts).length);
  await recordQuestThreshold(userId, Engine.QUEST_TRACK.OVERLORDS_OWNED, overlordCount);
}
/** Shared by every marketplace settlement path (direct buy, auction
 * buyout, auction expiry-with-winner), everything quest-related that
 * happens once a sale actually completes. `grossAmount` is what the buyer
 * paid before marketplace tax (what "sold it for more/less than you paid"
 * should compare against, not the seller's after-tax take); `sellerNet`
 * (only meaningful when `currency === 'gold'`) is what the seller actually
 * received after tax, the real amount to credit toward Rich Guy/Golden Guy
 * (QUEST_TRACK.GOLD_EARNED), which is about coins actually gained, not the
 * gross sale price and not the seller's resulting balance. */
async function onMarketSaleCompleted({ buyerId, sellerId, cardId, grossAmount, sellerNet, currency }) {
  try {
    await recordQuestEvent(buyerId, Engine.QUEST_TRACK.MARKET_PURCHASE, 1);
    await recordQuestEvent(sellerId, Engine.QUEST_TRACK.MARKET_SALE, 1);
    await recordQuestEvent(buyerId, Engine.QUEST_TRACK.MARKET_TRANSACTION, 1);
    await recordQuestEvent(sellerId, Engine.QUEST_TRACK.MARKET_TRANSACTION, 1);
    await computeCollectionQuestThresholds(buyerId);
    if (currency === 'gold' && sellerNet) {
      await recordQuestEvent(sellerId, Engine.QUEST_TRACK.GOLD_EARNED, sellerNet);
    }
    // Flipper / Market Crash, compare against the seller's last known buy
    // price for this exact card id (a simplified cost basis, see schema).
    const prevBuy = await getCardBuyPrice(sellerId, cardId);
    if (prevBuy && prevBuy.currency === currency) {
      if (grossAmount > prevBuy.price) await recordQuestEvent(sellerId, Engine.QUEST_TRACK.MARKET_FLIP_PROFIT, 1);
      else if (grossAmount < prevBuy.price) await recordQuestEvent(sellerId, Engine.QUEST_TRACK.MARKET_SELL_LOSS, 1);
    }
    // The buyer now owns this card at this price, that's their new cost
    // basis if THEY resell it later.
    await setCardBuyPrice(buyerId, cardId, grossAmount, currency);
  } catch (e) { console.error('[arena] market sale quest hooks failed', e); }
}

async function createListing(userId, msg) {
  const cardId = typeof msg.cardId === 'string' ? msg.cardId : null;
  const cardDef = cardId && Engine.CardDB.find(c => c.id === cardId);
  if (!cardDef) return { error: 'card_not_found' };

  const listingType = msg.listingType === 'auction' ? 'auction' : (msg.listingType === 'price' ? 'price' : null);
  if (!listingType) return { error: 'invalid_listing_params' };
  const currency = msg.currency === 'gems' ? 'gems' : (msg.currency === 'gold' ? 'gold' : null);
  if (!currency) return { error: 'invalid_listing_params' };

  const durationDays = Math.floor(Number(msg.durationDays));
  if (!Number.isInteger(durationDays) || durationDays < MARKET_MIN_DURATION_DAYS || durationDays > MARKET_MAX_DURATION_DAYS) {
    return { error: 'invalid_duration' };
  }

  let price = null, startingBid = null, buyoutPrice = null;
  if (listingType === 'price') {
    price = Math.floor(Number(msg.price));
    if (!Number.isInteger(price) || price <= 0 || price > MARKET_MAX_AMOUNT) return { error: 'invalid_listing_params' };
  } else {
    startingBid = Math.floor(Number(msg.startingBid));
    if (!Number.isInteger(startingBid) || startingBid <= 0 || startingBid > MARKET_MAX_AMOUNT) return { error: 'invalid_listing_params' };
    if (msg.buyoutPrice !== undefined && msg.buyoutPrice !== null && msg.buyoutPrice !== '') {
      buyoutPrice = Math.floor(Number(msg.buyoutPrice));
      if (!Number.isInteger(buyoutPrice) || buyoutPrice <= startingBid || buyoutPrice > MARKET_MAX_AMOUNT) return { error: 'invalid_listing_params' };
    }
  }

  const profile = await fetchProfile(userId);
  const counts = collectionCounts(profile.collection);
  if (!counts[cardId]) return { error: 'card_not_owned' };

  await adjustCardQuantity(userId, cardId, -1);

  const now = Date.now();
  const listing = {
    id: crypto.randomUUID(), sellerId: userId, cardId, listingType, currency,
    price, startingBid, buyoutPrice, currentBid: null, currentBidderId: null, currentBidEscrow: null,
    taxRate: null, status: 'active', durationDays,
    createdAt: new Date(now).toISOString(), expiresAt: new Date(now + durationDays * 24 * 60 * 60 * 1000).toISOString(),
    settledAt: null,
  };
  await saveListing(listing);
  try {
    await recordQuestEvent(userId, Engine.QUEST_TRACK.MARKET_LISTING_CREATE, 1);
    const activeCount = (await listingsBySeller(userId)).filter(l => l.status === 'active').length;
    await recordQuestThreshold(userId, Engine.QUEST_TRACK.MARKET_LISTINGS_ACTIVE, activeCount);
  } catch (e) { console.error('[arena] listing quest hooks failed', e); }
  return { ok: true, listing };
}

async function cancelListing(userId, listingId) {
  const listing = await getListing(listingId);
  if (!listing) return { error: 'listing_not_found' };
  if (listing.sellerId !== userId) return { error: 'not_your_listing' };
  if (listing.status !== 'active') return { error: 'listing_not_active' };
  if (listing.currentBidderId && listing.currentBidEscrow) {
    await adjustWallet(listing.currentBidderId, listing.currency === 'gold' ? listing.currentBidEscrow : 0, listing.currency === 'gems' ? listing.currentBidEscrow : 0);
    notifyUser(listing.currentBidderId, { type: 'market_outbid', listingId: listing.id, reason: 'cancelled' });
  }
  await adjustCardQuantity(userId, listing.cardId, 1);
  listing.status = 'cancelled'; listing.settledAt = new Date().toISOString();
  await saveListing(listing);
  return { ok: true, listing };
}

/** Buy-it-now for a 'price' listing, or the buyout shortcut for an
 * 'auction' listing that has one set. Re-checks the buyer's live balance
 * right before moving anything. */
async function buyListing(userId, listingId) {
  const listing = await getListing(listingId);
  if (!listing) return { error: 'listing_not_found' };
  if (listing.status !== 'active' || new Date(listing.expiresAt).getTime() <= Date.now()) return { error: 'listing_not_active' };
  if (listing.sellerId === userId) return { error: 'cannot_buy_own_listing' };

  const buyerGuild = await guildIdFor(userId);
  const sellerGuild = await guildIdFor(listing.sellerId);
  const taxRate = computeTaxRate(buyerGuild, sellerGuild);

  if (listing.listingType === 'auction') {
    if (!listing.buyoutPrice) return { error: 'auction_requires_bid' };
    return executeAuctionBuyout(listing, userId, taxRate);
  }

  const amount = listing.price;
  const taxAmt = Math.round(amount * taxRate);
  const totalCost = amount + taxAmt;
  const profile = await fetchProfile(userId);
  const balance = listing.currency === 'gold' ? profile.gold : profile.gems;
  if (balance < totalCost) return { error: 'insufficient_funds' };
  const sellerNet = amount - taxAmt;

  await adjustWallet(userId, listing.currency === 'gold' ? -totalCost : 0, listing.currency === 'gems' ? -totalCost : 0);
  await adjustWallet(listing.sellerId, listing.currency === 'gold' ? sellerNet : 0, listing.currency === 'gems' ? sellerNet : 0);
  await adjustCardQuantity(userId, listing.cardId, 1);

  listing.status = 'sold'; listing.currentBidderId = userId; listing.currentBid = amount;
  listing.taxRate = taxRate; listing.settledAt = new Date().toISOString();
  await saveListing(listing);
  await onMarketSaleCompleted({ buyerId: userId, sellerId: listing.sellerId, cardId: listing.cardId, grossAmount: amount, sellerNet, currency: listing.currency });
  return { ok: true, listing, sellerNet, taxAmt };
}

/** Shared by (a) an auction bid that meets/exceeds the buyout price and
 * (b) buyListing() called directly on an auction with a buyout set.
 * Refunds whoever was previously winning, since they're being bought out
 * rather than merely outbid. */
async function executeAuctionBuyout(listing, buyerId, taxRate) {
  const amount = listing.buyoutPrice;
  const taxAmt = Math.round(amount * taxRate);
  const totalCost = amount + taxAmt;
  const profile = await fetchProfile(buyerId);
  const balance = listing.currency === 'gold' ? profile.gold : profile.gems;
  if (balance < totalCost) return { error: 'insufficient_funds' };

  const previousBidderId = listing.currentBidderId;
  if (previousBidderId && listing.currentBidEscrow) {
    await adjustWallet(previousBidderId, listing.currency === 'gold' ? listing.currentBidEscrow : 0, listing.currency === 'gems' ? listing.currentBidEscrow : 0);
    notifyUser(previousBidderId, { type: 'market_outbid', listingId: listing.id, reason: 'bought_out' });
  }
  await adjustWallet(buyerId, listing.currency === 'gold' ? -totalCost : 0, listing.currency === 'gems' ? -totalCost : 0);
  const sellerNet = amount - taxAmt;
  await adjustWallet(listing.sellerId, listing.currency === 'gold' ? sellerNet : 0, listing.currency === 'gems' ? sellerNet : 0);
  await adjustCardQuantity(buyerId, listing.cardId, 1);

  listing.status = 'sold'; listing.currentBid = amount; listing.currentBidderId = buyerId;
  listing.currentBidEscrow = totalCost; listing.taxRate = taxRate; listing.settledAt = new Date().toISOString();
  await saveListing(listing);
  await onMarketSaleCompleted({ buyerId, sellerId: listing.sellerId, cardId: listing.cardId, grossAmount: amount, sellerNet, currency: listing.currency });
  return { ok: true, listing, bought: true, previousBidderId };
}

/** Places (or raises) a bid on an auction. Escrows bid+tax from the
 * bidder immediately and refunds whoever it outbids, so the leaderboard
 * bidder's balance always reflects money that's actually spoken for. */
async function placeBid(userId, listingId, amountRaw) {
  const listing = await getListing(listingId);
  if (!listing) return { error: 'listing_not_found' };
  if (listing.status !== 'active' || new Date(listing.expiresAt).getTime() <= Date.now()) return { error: 'listing_not_active' };
  if (listing.listingType !== 'auction') return { error: 'not_an_auction' };
  if (listing.sellerId === userId) return { error: 'cannot_bid_own_listing' };

  const amount = Math.floor(Number(amountRaw));
  if (!Number.isInteger(amount) || amount <= 0 || amount > MARKET_MAX_AMOUNT) return { error: 'invalid_amount' };
  const minRequired = listing.currentBid ? listing.currentBid + 1 : listing.startingBid;
  if (amount < minRequired) return { error: 'bid_too_low', minRequired };

  const buyerGuild = await guildIdFor(userId);
  const sellerGuild = await guildIdFor(listing.sellerId);
  const taxRate = computeTaxRate(buyerGuild, sellerGuild);

  if (listing.buyoutPrice && amount >= listing.buyoutPrice) {
    return executeAuctionBuyout(listing, userId, taxRate);
  }

  const taxAmt = Math.round(amount * taxRate);
  const totalEscrow = amount + taxAmt;
  const profile = await fetchProfile(userId);
  const balance = listing.currency === 'gold' ? profile.gold : profile.gems;
  if (balance < totalEscrow) return { error: 'insufficient_funds' };

  const previousBidderId = listing.currentBidderId;
  const previousEscrow = listing.currentBidEscrow;
  await adjustWallet(userId, listing.currency === 'gold' ? -totalEscrow : 0, listing.currency === 'gems' ? -totalEscrow : 0);
  if (previousBidderId && previousBidderId !== userId && previousEscrow) {
    await adjustWallet(previousBidderId, listing.currency === 'gold' ? previousEscrow : 0, listing.currency === 'gems' ? previousEscrow : 0);
    notifyUser(previousBidderId, { type: 'market_outbid', listingId: listing.id, reason: 'outbid' });
  }

  listing.currentBid = amount; listing.currentBidderId = userId; listing.currentBidEscrow = totalEscrow; listing.taxRate = taxRate;
  await saveListing(listing);
  await recordBid(listingId, userId, amount);
  return { ok: true, listing, previousBidderId };
}

/** Settles every listing whose expires_at has passed: auctions with a bid
 * go to the highest bidder (their escrow already covers it in full, see
 * placeBid), everything else returns the card to the seller. */
async function settleExpiredListings() {
  let candidates;
  const nowIso = new Date().toISOString();
  if (!HAS_SUPABASE) {
    candidates = [...guestListings.values()].filter(l => l.status === 'active' && l.expiresAt <= nowIso);
  } else {
    const { data, error } = await supabase.from('marketplace_listings').select('*').eq('status', 'active').lte('expires_at', nowIso).limit(200);
    if (error) throw error;
    candidates = (data || []).map(rowToListing);
  }
  for (const listing of candidates) {
    try {
      if (listing.listingType === 'auction' && listing.currentBidderId) {
        const taxRate = listing.taxRate != null ? listing.taxRate : MARKET_TAX_NORMAL;
        const taxAmt = Math.round(listing.currentBid * taxRate);
        const sellerNet = listing.currentBid - taxAmt;
        await adjustWallet(listing.sellerId, listing.currency === 'gold' ? sellerNet : 0, listing.currency === 'gems' ? sellerNet : 0);
        await adjustCardQuantity(listing.currentBidderId, listing.cardId, 1);
        listing.status = 'sold'; listing.settledAt = new Date().toISOString();
        await saveListing(listing);
        await onMarketSaleCompleted({ buyerId: listing.currentBidderId, sellerId: listing.sellerId, cardId: listing.cardId, grossAmount: listing.currentBid, sellerNet, currency: listing.currency });
        notifyUser(listing.currentBidderId, { type: 'market_auction_won', listingId: listing.id, cardId: listing.cardId, amount: listing.currentBid, currency: listing.currency });
        notifyUser(listing.sellerId, { type: 'market_item_sold', listingId: listing.id, cardId: listing.cardId, amount: sellerNet, currency: listing.currency });
      } else {
        await adjustCardQuantity(listing.sellerId, listing.cardId, 1);
        listing.status = 'expired'; listing.settledAt = new Date().toISOString();
        await saveListing(listing);
        notifyUser(listing.sellerId, { type: 'market_listing_expired', listingId: listing.id, cardId: listing.cardId });
      }
    } catch (e) { console.error('[arena] failed to settle listing', listing.id, e); }
  }
}

/* ── Direct messages (marketplace negotiation only, not a general inbox) ── */
function rowToDM(row) {
  return {
    id: row.id, fromId: row.from_id, toId: row.to_id, listingId: row.listing_id, message: row.message,
    offerAmount: row.offer_amount, offerCurrency: row.offer_currency, offerStatus: row.offer_status,
    read: row.read, createdAt: row.created_at,
  };
}
async function saveDM(m) {
  if (!HAS_SUPABASE) {
    const key = pairKey(m.fromId, m.toId);
    const arr = guestDMs.get(key) || []; arr.push(m); guestDMs.set(key, arr);
    return m;
  }
  const { data, error } = await supabase.from('direct_messages').insert({
    from_id: m.fromId, to_id: m.toId, listing_id: m.listingId, message: m.message,
    offer_amount: m.offerAmount, offer_currency: m.offerCurrency, offer_status: m.offerStatus,
  }).select('*').single();
  if (error) throw error;
  return rowToDM(data);
}
async function getDMMessageById(messageId) {
  if (!HAS_SUPABASE) {
    for (const arr of guestDMs.values()) { const found = arr.find(m => m.id === messageId); if (found) return found; }
    return null;
  }
  const { data, error } = await supabase.from('direct_messages').select('*').eq('id', messageId).maybeSingle();
  if (error) throw error;
  return data ? rowToDM(data) : null;
}
async function setOfferStatus(messageId, status) {
  if (!HAS_SUPABASE) {
    for (const arr of guestDMs.values()) { const found = arr.find(m => m.id === messageId); if (found) { found.offerStatus = status; return; } }
    return;
  }
  const { error } = await supabase.from('direct_messages').update({ offer_status: status }).eq('id', messageId);
  if (error) throw error;
}
async function markMessagesRead(ids, guestRowsRef) {
  if (!ids.length) return;
  if (!HAS_SUPABASE) { for (const m of guestRowsRef) if (ids.includes(m.id)) m.read = true; return; }
  const { error } = await supabase.from('direct_messages').update({ read: true }).in('id', ids);
  if (error) throw error;
}
/** Per-viewer archive flag for a DM conversation, see the
 * player_dm_archived schema comment for the full "auto-un-archives on a
 * new message" behavior, implemented in dmConversations below by simply
 * comparing timestamps rather than needing to clear this on every send. */
async function getDMArchivedMap(userId) {
  if (!HAS_SUPABASE) {
    const map = new Map();
    for (const [key, archivedAt] of guestDMArchived) {
      const [owner, other] = key.split('|');
      if (owner === userId) map.set(other, archivedAt);
    }
    return map;
  }
  const { data } = await supabase.from('player_dm_archived').select('other_id,archived_at').eq('owner_id', userId);
  const map = new Map();
  for (const row of (data || [])) map.set(row.other_id, row.archived_at);
  return map;
}
async function archiveDMConversation(userId, otherId) {
  if (!HAS_SUPABASE) { guestDMArchived.set(`${userId}|${otherId}`, new Date().toISOString()); return; }
  await supabase.from('player_dm_archived').upsert({ owner_id: userId, other_id: otherId, archived_at: new Date().toISOString() }, { onConflict: 'owner_id,other_id' });
}

async function dmHistory(userId, otherId) {
  let rows;
  if (!HAS_SUPABASE) {
    rows = (guestDMs.get(pairKey(userId, otherId)) || []).slice(-DM_HISTORY_LIMIT);
  } else {
    const { data, error } = await supabase.from('direct_messages').select('*')
      .or(`and(from_id.eq.${userId},to_id.eq.${otherId}),and(from_id.eq.${otherId},to_id.eq.${userId})`)
      .order('created_at', { ascending: true }).limit(DM_HISTORY_LIMIT);
    if (error) throw error;
    rows = (data || []).map(rowToDM);
  }
  const unreadIds = rows.filter(m => m.toId === userId && !m.read).map(m => m.id);
  await markMessagesRead(unreadIds, rows);
  return rows;
}
async function dmConversations(userId) {
  let rows;
  if (!HAS_SUPABASE) {
    rows = [];
    for (const [key, arr] of guestDMs.entries()) {
      const [a, b] = key.split('|');
      if (a !== userId && b !== userId) continue;
      rows.push(...arr);
    }
  } else {
    const { data, error } = await supabase.from('direct_messages').select('*')
      .or(`from_id.eq.${userId},to_id.eq.${userId}`)
      .order('created_at', { ascending: false }).limit(1000);
    if (error) throw error;
    rows = (data || []).map(rowToDM);
  }
  const byOther = new Map();
  for (const m of rows) {
    const other = m.fromId === userId ? m.toId : m.fromId;
    const existing = byOther.get(other);
    if (!existing || m.createdAt > existing.lastMessage.createdAt) byOther.set(other, { lastMessage: m, unread: 0 });
  }
  for (const m of rows) {
    if (m.toId === userId && !m.read) {
      const entry = byOther.get(m.fromId);
      if (entry) entry.unread++;
    }
  }
  // A conversation this player archived stays hidden from their own list
  // (see the player_dm_archived schema comment) UNLESS its last message
  // came in/went out AFTER the archive, i.e. archiving only hides it
  // until something new happens, same as most chat apps' "archive" (not
  // "block" or "delete") behavior. This is also exactly what makes a
  // finished trade's conversation actually leave the list once archived,
  // rather than sitting there forever with nothing to do about it.
  const archived = await getDMArchivedMap(userId);
  return [...byOther.entries()]
    .filter(([otherId, v]) => {
      const archivedAt = archived.get(otherId);
      return !archivedAt || v.lastMessage.createdAt > archivedAt;
    })
    .map(([otherId, v]) => ({ userId: otherId, lastMessage: v.lastMessage, unread: v.unread }))
    .sort((a, b) => b.lastMessage.createdAt.localeCompare(a.lastMessage.createdAt))
    .slice(0, DM_CONVERSATIONS_LIMIT);
}

/** Sends a DM. `listingId`/`offerAmount`/`offerCurrency` are optional, an
 * offer is just a message that also carries a proposed price the *seller*
 * can accept to trigger an immediate sale via acceptOffer(). Only ever
 * used for 'price' listings; auctions negotiate through bids only. */
async function sendDM(fromId, toId, text, opts = {}) {
  if (typeof toId !== 'string' || toId === fromId) return { error: 'dm_invalid' };
  const message = typeof text === 'string' ? text.trim() : '';
  if (!message || message.length > DM_MESSAGE_MAX) return { error: 'dm_invalid' };
  if (!isAllowedChatText(message)) return { error: 'invalid_chat_image' };

  let listingId = null, offerAmount = null, offerCurrency = null, offerStatus = null;
  if (opts.listingId) {
    const listing = await getListing(opts.listingId);
    if (!listing || listing.listingType !== 'price') return { error: 'dm_invalid' };
    listingId = listing.id;
  }
  if (opts.offerAmount !== undefined && opts.offerAmount !== null && opts.offerAmount !== '') {
    const amt = Math.floor(Number(opts.offerAmount));
    if (!Number.isInteger(amt) || amt <= 0 || amt > MARKET_MAX_AMOUNT) return { error: 'dm_invalid' };
    const cur = opts.offerCurrency === 'gems' ? 'gems' : (opts.offerCurrency === 'gold' ? 'gold' : null);
    if (!cur) return { error: 'dm_invalid' };
    offerAmount = amt; offerCurrency = cur; offerStatus = 'pending';
  }

  const saved = await saveDM({
    id: crypto.randomUUID(), fromId, toId, listingId, message,
    offerAmount, offerCurrency, offerStatus, read: false, createdAt: new Date().toISOString(),
  });
  return { ok: true, message: saved };
}

/** The seller (or, if the seller sent the offer, the buyer) accepts a
 * pending price offer from a DM, executing the sale immediately at the
 * offered amount. Only the message's recipient can accept it. */
async function acceptOffer(userId, messageId) {
  const message = await getDMMessageById(messageId);
  if (!message) return { error: 'offer_not_found' };
  if (message.toId !== userId) return { error: 'not_your_offer' };
  if (message.offerStatus !== 'pending' || !message.offerAmount || !message.listingId) return { error: 'offer_not_pending' };

  const listing = await getListing(message.listingId);
  if (!listing || listing.status !== 'active' || listing.listingType !== 'price') return { error: 'listing_not_active' };

  let buyerId;
  if (listing.sellerId === message.fromId) buyerId = message.toId;
  else if (listing.sellerId === message.toId) buyerId = message.fromId;
  else return { error: 'listing_not_active' };
  if (buyerId === listing.sellerId) return { error: 'cannot_buy_own_listing' };

  const currency = message.offerCurrency || listing.currency;
  const amount = message.offerAmount;
  const buyerGuild = await guildIdFor(buyerId);
  const sellerGuild = await guildIdFor(listing.sellerId);
  const taxRate = computeTaxRate(buyerGuild, sellerGuild);
  const taxAmt = Math.round(amount * taxRate);
  const totalCost = amount + taxAmt;

  const profile = await fetchProfile(buyerId);
  const balance = currency === 'gold' ? profile.gold : profile.gems;
  if (balance < totalCost) { await setOfferStatus(messageId, 'declined'); return { error: 'buyer_cannot_afford' }; }

  const sellerNet = amount - taxAmt;
  await adjustWallet(buyerId, currency === 'gold' ? -totalCost : 0, currency === 'gems' ? -totalCost : 0);
  await adjustWallet(listing.sellerId, currency === 'gold' ? sellerNet : 0, currency === 'gems' ? sellerNet : 0);
  await adjustCardQuantity(buyerId, listing.cardId, 1);

  listing.status = 'sold'; listing.currentBidderId = buyerId; listing.currentBid = amount;
  listing.currency = currency; listing.taxRate = taxRate; listing.settledAt = new Date().toISOString();
  await saveListing(listing);
  await setOfferStatus(messageId, 'accepted');
  return { ok: true, listing, buyerId, sellerId: listing.sellerId };
}

/** Purges DM messages tied to a listing once that listing has been settled
 * (sold/expired/cancelled) for over an hour. The point of a listing-linked
 * message is negotiating THAT sale, once it's resolved, keeping the offer
 * back-and-forth around forever just clutters the thread the next time you
 * message the same seller about a different card. Plain messages with no
 * listingId (and messages on a still-active listing) are never touched. */
async function cleanupExpiredListingDMs() {
  const cutoffIso = new Date(Date.now() - DM_LISTING_MESSAGE_RETENTION_MS).toISOString();
  if (!HAS_SUPABASE) {
    const staleListingIds = new Set(
      [...guestListings.values()]
        .filter(l => l.status !== 'active' && l.settledAt && l.settledAt <= cutoffIso)
        .map(l => l.id)
    );
    if (!staleListingIds.size) return;
    for (const [key, arr] of guestDMs) {
      const kept = arr.filter(m => !m.listingId || !staleListingIds.has(m.listingId));
      if (kept.length !== arr.length) guestDMs.set(key, kept);
    }
    return;
  }
  const { data: staleListings, error: listErr } = await supabase.from('marketplace_listings')
    .select('id').neq('status', 'active').lte('settled_at', cutoffIso).limit(500);
  if (listErr) { console.error('[arena] DM cleanup: failed to find settled listings', listErr); return; }
  const ids = (staleListings || []).map(l => l.id);
  if (!ids.length) return;
  const { error } = await supabase.from('direct_messages').delete().in('listing_id', ids);
  if (error) console.error('[arena] DM cleanup failed', error);
}

/** Starter collection for brand-new accounts, exactly 12 card ids (matches
 * Engine.DECK_SIZE, so it doubles as a ready-to-save starter deck). This is
 * the one place to edit to change what new players start with — just swap
 * the ids below for whatever cards.json ids you want; nothing else needs
 * to know about it. Placeholder list, not final: 3 equipment + 9 creatures. */
const STARTER_CARD_IDS = [
  'crystal_sword', 'rainbow_blade', 'scythe',
  'obsidian_dragon', 'lightning_dragon', 'giant_imp', 'storm_cleaver',
  'sun_whisperer', 'moon_disciple', 'blizzard_king', 'crystal_king', 'rockfire_dragon',
];
function seedStarterIds() {
  // Guard against a hand-edited id typo silently shrinking someone's starter
  // collection: drop anything that isn't actually in cards.json.
  const validIds = new Set(Engine.CardDB.map(c => c.id));
  const ids = STARTER_CARD_IDS.filter(id => validIds.has(id));
  if (ids.length !== STARTER_CARD_IDS.length) {
    console.warn('[arena] seedStarterIds: one or more STARTER_CARD_IDS entries not found in cards.json, dropped:',
      STARTER_CARD_IDS.filter(id => !validIds.has(id)));
  }
  return ids;
}

async function saveDeck(userId, cardIds, ownedSet) {
  const ids = Array.isArray(cardIds) ? cardIds : [];
  const owned = ids.filter(id => ownedSet.has(id));
  if (!Engine.isDeckLegal(owned)) {
    const e = new Error('deck_illegal');
    e.code = owned.length !== Engine.DECK_SIZE ? 'deck_wrong_size' : 'deck_composition_invalid';
    e.deckSize = owned.length;
    e.requiredSize = Engine.DECK_SIZE;
    throw e;
  }
  const clean = owned;
  if (!HAS_SUPABASE) {
    const p = guestProfiles.get(userId); if (p) p.deck = clean;
    return clean;
  }
  const { error } = await supabase.from('player_decks').upsert({ owner_id: userId, card_ids: clean }, { onConflict: 'owner_id' });
  if (error) throw error;
  return clean;
}

async function grantPack(userId, packId) {
  const result = Engine.openPack(packId); // throws on bad packId, validated server-side, client can't fake odds
  const profile = await fetchProfile(userId);
  const balance = result.currency === 'gems' ? profile.gems : profile.gold;
  if (balance < result.cost) { const e = new Error('insufficient_funds'); e.code = 'insufficient_funds'; throw e; }

  const newBalance = balance - result.cost;
  if (!HAS_SUPABASE) {
    const p = guestProfiles.get(userId);
    if (result.currency === 'gems') p.gems = newBalance; else p.gold = newBalance;
    result.cards.forEach(c => p.collection.push(c.id));
  } else {
    const field = result.currency === 'gems' ? 'gems' : 'gold';
    const { error } = await supabase.from('profiles').update({ [field]: newBalance }).eq('id', userId);
    if (error) throw error;
    // bump quantities: one read + one write for the whole pack, regardless
    // of how many distinct cards it contained, all of it lives in the
    // single jsonb blob for this player now.
    const counts = {};
    result.cards.forEach(c => { counts[c.id] = (counts[c.id] || 0) + 1; });
    const { data: existing } = await supabase.from('player_cards').select('cards').eq('owner_id', userId).maybeSingle();
    const cards = { ...(existing?.cards || {}) };
    for (const [card_id, addQty] of Object.entries(counts)) cards[card_id] = (cards[card_id] || 0) + addQty;
    await supabase.from('player_cards').upsert({ owner_id: userId, cards }, { onConflict: 'owner_id' });
  }
  recordQuestEvent(userId, Engine.QUEST_TRACK.PACK_BUY, 1).catch(e => console.error('[arena] pack_buy quest event failed', e));
  (async () => {
    try {
      // Pack Variety, distinct pack ids ever opened, at least once.
      if (!HAS_SUPABASE) {
        let set = guestPackTypes.get(userId);
        if (!set) { set = new Set(); guestPackTypes.set(userId, set); }
        set.add(packId);
        await recordQuestThreshold(userId, Engine.QUEST_TRACK.PACK_VARIETY, set.size);
      } else {
        await supabase.from('player_pack_types').upsert({ owner_id: userId, pack_id: packId }, { onConflict: 'owner_id,pack_id', ignoreDuplicates: true });
        const { count } = await supabase.from('player_pack_types').select('pack_id', { count: 'exact', head: true }).eq('owner_id', userId);
        await recordQuestThreshold(userId, Engine.QUEST_TRACK.PACK_VARIETY, count || 0);
      }
      // Lucky Bastard / Fabled Pull, did this specific pack contain one?
      if (result.cards.some(c => c.rarity === 'mythic')) await recordQuestEvent(userId, Engine.QUEST_TRACK.PULL_RARITY_MYTHIC, 1);
      if (result.cards.some(c => c.classification === 'overlord')) await recordQuestEvent(userId, Engine.QUEST_TRACK.PULL_OVERLORD_CARD, 1);
      // Collector / Archivist / Overlord Master, recompute from the live collection.
      await computeCollectionQuestThresholds(userId);
    } catch (e) { console.error('[arena] pack quest hooks failed', e); }
  })();
  return { cards: result.cards, newBalance, currency: result.currency };
}
const guestPackTypes = new Map(); // userId -> Set(packId), guest-mode fallback for Pack Variety

/** Bot opponents get a `bot:<uuid>` userId, never a real profile row, so
 * nothing here should try to read/write one as if it belonged to a player. */
const isBotId = id => typeof id === 'string' && id.startsWith('bot:');

const RANK_POINTS_WIN = 2, RANK_POINTS_LOSS = -1;

async function applyMatchReward(winnerId, loserId, ranked = true) {
  let winnerGold = null, winnerRank = null, loserRank = null;
  if (!HAS_SUPABASE) {
    const w = guestProfiles.get(winnerId), l = guestProfiles.get(loserId);
    if (w) { w.wins++; w.gold += WIN_GOLD_REWARD; if (ranked) w.rankPoints = Math.max(0, (w.rankPoints || 0) + RANK_POINTS_WIN); winnerGold = w.gold; winnerRank = w.rankPoints; }
    if (l) { l.losses++; if (ranked) l.rankPoints = Math.max(0, (l.rankPoints || 0) + RANK_POINTS_LOSS); loserRank = l.rankPoints; }
  } else {
    // The winner's profile update, the loser's, and the match-history insert
    // are three entirely independent writes (different rows, no data one
    // depends on another for), so they run concurrently rather than as three
    // sequential round trips — this alone used to account for a good chunk
    // of the delay between a match actually ending and match_over reaching
    // the loser, most noticeably against a bot, where the winner's side of
    // this is normally skipped instantly (isBotId) and so didn't hide how
    // slow the loser's side alone already was.
    const [winnerResult, loserResult] = await Promise.all([
      (async () => {
        if (isBotId(winnerId)) return null;
        const { data: winner } = await supabase.from('profiles').select('gold,wins,rank_points').eq('id', winnerId).maybeSingle();
        if (!winner) return null;
        const update = { gold: winner.gold + WIN_GOLD_REWARD, wins: winner.wins + 1 };
        if (ranked) update.rank_points = Math.max(0, (winner.rank_points || 0) + RANK_POINTS_WIN);
        await supabase.from('profiles').update(update).eq('id', winnerId);
        return { gold: update.gold, rank: update.rank_points != null ? update.rank_points : winner.rank_points };
      })(),
      (async () => {
        if (isBotId(loserId)) return null;
        const { data: loser } = await supabase.from('profiles').select('losses,rank_points').eq('id', loserId).maybeSingle();
        if (!loser) return null;
        const update = { losses: loser.losses + 1 };
        if (ranked) update.rank_points = Math.max(0, (loser.rank_points || 0) + RANK_POINTS_LOSS);
        await supabase.from('profiles').update(update).eq('id', loserId);
        return { rank: update.rank_points != null ? update.rank_points : loser.rank_points };
      })(),
      // don't log fake matches against a bot into permanent match history
      (!isBotId(winnerId) && !isBotId(loserId))
        ? supabase.from('match_history').insert({ player_a: winnerId, player_b: loserId, winner: winnerId, reward_gold: WIN_GOLD_REWARD, reward_gems: 0, ranked })
        : Promise.resolve(null),
    ]);
    if (winnerResult) { winnerGold = winnerResult.gold; winnerRank = winnerResult.rank; }
    if (loserResult) loserRank = loserResult.rank;
  }

  // ── Quest hooks, every match, win or lose, ranked or not ──────────
  // Every one of these targets a different quest track (a different set of
  // DB rows), so none of them depend on any other's result — same
  // reasoning as above, run them together instead of one at a time. Each
  // call already handles its own errors internally (see recordQuestEvent/
  // recordQuestThreshold's per-def try/catch), so one failing can't take
  // the others down with it.
  const questHooks = [
    recordQuestEvent(winnerId, Engine.QUEST_TRACK.MATCH_PLAY, 1),
    recordQuestEvent(loserId, Engine.QUEST_TRACK.MATCH_PLAY, 1),
    recordQuestEvent(winnerId, Engine.QUEST_TRACK.MATCH_WIN, 1),
  ];
  if (ranked) {
    questHooks.push(
      recordQuestEvent(winnerId, Engine.QUEST_TRACK.RANKED_MATCH_PLAY, 1),
      recordQuestEvent(loserId, Engine.QUEST_TRACK.RANKED_MATCH_PLAY, 1),
      recordQuestEvent(winnerId, Engine.QUEST_TRACK.RANKED_MATCH_WIN, 1),
    );
    if (winnerRank != null) questHooks.push(recordQuestThreshold(winnerId, Engine.QUEST_TRACK.RANK_POINTS, winnerRank));
    if (loserRank != null) questHooks.push(recordQuestThreshold(loserId, Engine.QUEST_TRACK.RANK_POINTS, loserRank));
  }
  if (winnerGold != null) questHooks.push(recordQuestEvent(winnerId, Engine.QUEST_TRACK.GOLD_EARNED, WIN_GOLD_REWARD));
  // Nemesis: track the loser's worst per-opponent loss streak. Skipped for
  // bot matches, "the same player" doesn't mean much against a bot.
  if (!isBotId(winnerId) && !isBotId(loserId)) {
    questHooks.push((async () => {
      try {
        const streak = await bumpNemesisLoss(loserId, winnerId);
        await recordQuestThreshold(loserId, Engine.QUEST_TRACK.NEMESIS_LOSS, streak);
      } catch (e) { console.error('[arena] nemesis tracking failed', e); }
    })());
  }
  await Promise.all(questHooks);

  return { gold: WIN_GOLD_REWARD, gems: 0 };
}

/** Guest-mode fallback for per-opponent loss streaks (see bumpNemesisLoss).
 * Key is `${loserId}|${winnerId}` -> loss count against that specific
 * opponent. */
const guestNemesisLosses = new Map();
/** Increments and returns `loserId`'s loss count specifically against
 * `winnerId`, the "worst streak against any single opponent" the Nemesis
 * quest tracks is the max of these across all opponents (see
 * recordQuestThreshold, which only ever keeps the highest value it's
 * ever been given). */
async function bumpNemesisLoss(loserId, winnerId) {
  if (!HAS_SUPABASE) {
    const key = `${loserId}|${winnerId}`;
    const next = (guestNemesisLosses.get(key) || 0) + 1;
    guestNemesisLosses.set(key, next);
    return next;
  }
  const { data } = await supabase.from('player_nemesis').select('losses').eq('owner_id', loserId).eq('opponent_id', winnerId).maybeSingle();
  const next = (data ? data.losses : 0) + 1;
  await supabase.from('player_nemesis').upsert({ owner_id: loserId, opponent_id: winnerId, losses: next }, { onConflict: 'owner_id,opponent_id' });
  return next;
}

/* ══════════════════════════════════════════════════════════════════════
 * QUEST SYSTEM
 * Definitions (target/points/reward/track) are static data in game-engine.js
 * (Engine.QUEST_DEFS), everything here is just per-player PROGRESS,
 * persisted server-side and only ever mutated by these functions in
 * response to a real server-observed event, never a client-reported one.
 *
 * The one entry point every game-event hook below calls is
 * recordQuestEvent(userId, track, amount), it fans out to every currently
 * active quest (any scope) listening on that track. Nothing else needs to
 * know which quests exist or care about daily/weekly/permanent bookkeeping.
 * ══════════════════════════════════════════════════════════════════════ */

/** Adds a currency reward (gold/gems, either optional) to a player's
 * balance, the same small operation quest completions and quest-bar
 * milestones both need, factored out once. Silently no-ops for bot ids. */
async function grantQuestCurrency(userId, reward) {
  if (isBotId(userId) || !reward) return;
  const gold = reward.gold || 0, gems = reward.gems || 0;
  if (!gold && !gems) return;
  if (!HAS_SUPABASE) {
    const p = guestProfiles.get(userId);
    if (p) { p.gold += gold; p.gems += gems; }
    return;
  }
  const { data: profile } = await supabase.from('profiles').select('gold,gems').eq('id', userId).maybeSingle();
  if (!profile) return;
  await supabase.from('profiles').update({ gold: profile.gold + gold, gems: profile.gems + gems }).eq('id', userId);
}

/** One-time reward for going through the tutorial (see Tutorial.exit() client-side,
 * fires on any exit, early-quit included, not gated on finishing every step). The
 * dedupe (so it's only ever granted once per account) lives here, not the client. */
const TUTORIAL_BONUS_GEMS = 75;
async function grantTutorialBonus(userId) {
  if (isBotId(userId)) return { granted: false };
  if (!HAS_SUPABASE) {
    const p = guestProfiles.get(userId);
    if (!p || p.tutorialBonusClaimed) return { granted: false };
    p.tutorialBonusClaimed = true;
    p.gems += TUTORIAL_BONUS_GEMS;
    return { granted: true };
  }
  const { data: profile } = await supabase.from('profiles').select('gems,tutorial_bonus_claimed').eq('id', userId).maybeSingle();
  if (!profile || profile.tutorial_bonus_claimed) return { granted: false };
  await supabase.from('profiles').update({ gems: profile.gems + TUTORIAL_BONUS_GEMS, tutorial_bonus_claimed: true }).eq('id', userId);
  return { granted: true };
}

async function getQuestProgressRow(userId, questId, periodKey) {
  if (!HAS_SUPABASE) {
    return guestQuestProgress.get(`${userId}|${questId}|${periodKey}`) || { progress: 0, completed: false, claimed: false, rewarded: false };
  }
  const { data } = await supabase.from('player_quest_progress').select('progress,completed,claimed,rewarded')
    .eq('owner_id', userId).eq('quest_id', questId).eq('period_key', periodKey).maybeSingle();
  return data || { progress: 0, completed: false, claimed: false, rewarded: false };
}
async function putQuestProgressRow(userId, questId, periodKey, row) {
  if (!HAS_SUPABASE) { guestQuestProgress.set(`${userId}|${questId}|${periodKey}`, row); return; }
  await supabase.from('player_quest_progress').upsert(
    { owner_id: userId, quest_id: questId, period_key: periodKey, ...row },
    { onConflict: 'owner_id,quest_id,period_key' },
  );
}
async function getQuestBarRow(userId, scope, periodKey) {
  if (!HAS_SUPABASE) {
    return guestQuestBars.get(`${userId}|${scope}|${periodKey}`) || { points: 0, claimedMilestones: [] };
  }
  const { data } = await supabase.from('player_quest_bars').select('points,claimed_milestones')
    .eq('owner_id', userId).eq('scope', scope).eq('period_key', periodKey).maybeSingle();
  return data ? { points: data.points, claimedMilestones: data.claimed_milestones || [] } : { points: 0, claimedMilestones: [] };
}
async function putQuestBarRow(userId, scope, periodKey, row) {
  if (!HAS_SUPABASE) { guestQuestBars.set(`${userId}|${scope}|${periodKey}`, row); return; }
  await supabase.from('player_quest_bars').upsert(
    { owner_id: userId, scope, period_key: periodKey, points: row.points, claimed_milestones: row.claimedMilestones },
    { onConflict: 'owner_id,scope,period_key' },
  );
}

/** Adds `pointsToAdd` to `scope`'s ('daily'|'weekly') quest bar for the
 * CURRENT period. Purely arithmetic, does NOT resolve or pay out any
 * milestone crossed in the process; that's claim_milestone's job alone
 * (a milestone being numerically reachable and it being PAID are two
 * different things now, see claimBarMilestone). Only ever called from
 * claimQuestReward, i.e. only when the player has explicitly claimed a
 * completed daily/weekly quest, reaching a quest's target does not, by
 * itself, move the bar at all. */
async function addQuestBarPoints(userId, scope, pointsToAdd) {
  if (isBotId(userId) || pointsToAdd <= 0) return;
  const periodKey = Engine.periodKeyForScope(scope);
  const bar = await getQuestBarRow(userId, scope, periodKey);
  const newPoints = Math.min(100, bar.points + pointsToAdd);
  await putQuestBarRow(userId, scope, periodKey, { points: newPoints, claimedMilestones: bar.claimedMilestones });
}

/** The one entry point game-event hooks call. Fans `amount` progress out to
 * every currently-active quest def listening on `track`, regardless of
 * scope, each one independently handles its own period key. Reaching a
 * quest's target only flips `completed` here; for daily/weekly quests
 * nothing is paid out and the bar doesn't move until the player explicitly
 * claims it (see claimQuestReward). Permanent quests are the one
 * exception, their currency has always paid out immediately on
 * completion (see spec: "permanent quests should give you immediate
 * currency"), `claim_quest` for those just flips `claimed` for the UI.
 * Safe to call for a track nothing currently listens on (a no-op), and
 * safe to call for bot ids (also a no-op). */
/** Pushes a fresh quest_state to this player's live connection, if they
 * have one open right now, covers events a request/response round-trip
 * can't (like the global-chat/marketplace/rank watchers, which fire from
 * places with no originating WS request to reply to) so the Quest Journal
 * updates in real time instead of only on next manual open. */
async function pushLiveQuestState(userId) {
  const conn = connections.get(userId);
  if (!conn) return;
  try { conn.send({ type: 'quest_state', ...(await buildQuestState(userId)) }); }
  catch (e) { console.error('[arena] live quest_state push failed', e); }
}

/** The entry point for CUMULATIVE quest tracks (see the "two flavors"
 * doc in game-engine.js), fans `amount` progress out to every currently
 * active `kind !== 'threshold'` quest def listening on `track`, regardless
 * of scope. Reaching a quest's target only flips `completed` here; for
 * daily/weekly quests nothing is paid out and the bar doesn't move until
 * the player explicitly claims it (see claimQuestReward). Permanent quests
 * are the one exception, their currency has always paid out immediately
 * on completion, `claim_quest` for those just flips `claimed` for the UI.
 * Safe to call for a track nothing currently listens on (a no-op), and
 * safe to call for bot ids (also a no-op). */
async function recordQuestEvent(userId, track, amount = 1) {
  if (isBotId(userId) || !userId) return;
  const defs = Engine.questDefsForTrack(track).filter(d => d.kind !== 'threshold');
  if (!defs.length) return;
  // Every def here is a separate DB row (own quest id + period key), so
  // they don't need to wait on each other — see applyMatchReward's own
  // comment on why this matters for how long a losing player waits to see
  // the defeat screen. Each iteration already catches its own error, so a
  // single def failing can't take the others down with it.
  await Promise.all(defs.map(async def => {
    try {
      const periodKey = Engine.periodKeyForScope(def.scope);
      const row = await getQuestProgressRow(userId, def.id, periodKey);
      if (row.completed) return; // already done for this period, nothing further to add
      const newProgress = Math.min(def.target, row.progress + amount);
      const justCompleted = newProgress >= def.target;
      await putQuestProgressRow(userId, def.id, periodKey, {
        progress: newProgress, completed: justCompleted, claimed: row.claimed || false,
        rewarded: row.rewarded || (justCompleted && def.scope === 'permanent'),
      });
      if (justCompleted && def.scope === 'permanent') { await grantQuestCurrency(userId, def.reward); await grantQuestCosmetics(userId, def.reward); }
    } catch (e) { console.error(`[arena] quest progress failed (${def.id})`, e); }
  }));
  await pushLiveQuestState(userId);
}

/** The entry point for THRESHOLD quest tracks (see the "two flavors" doc
 * in game-engine.js), sets progress to max(existing, currentValue) for
 * every currently active `kind === 'threshold'` quest def listening on
 * `track`, instead of adding. For "reach/have/hold N" quests (rank points,
 * friends, unique cards, overlords owned, concurrent listings, worst
 * per-opponent loss streak) where what matters is the best value ever
 * observed, not a tally of discrete events — NOT for anything phrased as
 * "earn"/"gain" (e.g. Rich Guy/Golden Guy, QUEST_TRACK.GOLD_EARNED), which
 * wants recordQuestEvent instead: a threshold on a balance that can rise
 * for reasons unrelated to the quest's own period (already having gold
 * from a previous day, say) completes itself on nothing but a login, which
 * is exactly the bug this distinction exists to avoid. Same completion/
 * payout rules as recordQuestEvent otherwise. */
async function recordQuestThreshold(userId, track, currentValue) {
  if (isBotId(userId) || !userId) return;
  const defs = Engine.questDefsForTrack(track).filter(d => d.kind === 'threshold');
  if (!defs.length) return;
  // Same reasoning as recordQuestEvent: independent rows, run together.
  await Promise.all(defs.map(async def => {
    try {
      const periodKey = Engine.periodKeyForScope(def.scope);
      const row = await getQuestProgressRow(userId, def.id, periodKey);
      if (row.completed) return;
      const newProgress = Math.min(def.target, Math.max(row.progress, currentValue));
      const justCompleted = newProgress >= def.target;
      await putQuestProgressRow(userId, def.id, periodKey, {
        progress: newProgress, completed: justCompleted, claimed: row.claimed || false,
        rewarded: row.rewarded || (justCompleted && def.scope === 'permanent'),
      });
      if (justCompleted && def.scope === 'permanent') { await grantQuestCurrency(userId, def.reward); await grantQuestCosmetics(userId, def.reward); }
    } catch (e) { console.error(`[arena] quest threshold failed (${def.id})`, e); }
  }));
  await pushLiveQuestState(userId);
}

/** Handles a player explicitly claiming a completed quest (case
 * 'claim_quest'), this is what actually credits a daily/weekly quest's
 * points onto its bar (permanent quests already paid their currency at
 * completion time; this just flips `claimed` for those). Re-verifies
 * completion and non-double-claiming server-side regardless of what the
 * client thinks its own state is. */
async function claimQuestReward(userId, questId) {
  const def = Engine.QuestById[questId];
  if (!def) return { ok: false, reason: 'unknown_quest' };
  const periodKey = Engine.periodKeyForScope(def.scope);
  const row = await getQuestProgressRow(userId, questId, periodKey);
  if (!row.completed) return { ok: false, reason: 'not_completed' };
  if (row.claimed) return { ok: false, reason: 'already_claimed' };
  await putQuestProgressRow(userId, questId, periodKey, { progress: row.progress, completed: true, claimed: true, rewarded: row.rewarded });
  if (def.scope === 'daily' || def.scope === 'weekly') await addQuestBarPoints(userId, def.scope, def.points);
  return { ok: true };
}

/** Handles a player explicitly claiming a reached quest-bar milestone
 * (case 'claim_milestone'), this is what actually pays out the
 * milestone's currency reward; being numerically past the threshold
 * (`bar.points >= milestone`) alone pays nothing. Re-verifies both the
 * threshold and non-double-claiming server-side. */
async function claimBarMilestone(userId, scope, milestone) {
  if (scope !== 'daily' && scope !== 'weekly') return { ok: false, reason: 'bad_scope' };
  if (!Engine.QUEST_BAR_MILESTONES.includes(milestone)) return { ok: false, reason: 'bad_milestone' };
  const periodKey = Engine.periodKeyForScope(scope);
  const bar = await getQuestBarRow(userId, scope, periodKey);
  if (bar.points < milestone) return { ok: false, reason: 'not_reached' };
  if ((bar.claimedMilestones || []).includes(milestone)) return { ok: false, reason: 'already_claimed' };
  const reward = (Engine.QUEST_BAR_MILESTONE_REWARDS[scope] || {})[milestone] || {};
  const claimedMilestones = [...(bar.claimedMilestones || []), milestone].sort((a, b) => a - b);
  await putQuestBarRow(userId, scope, periodKey, { points: bar.points, claimedMilestones });
  await grantQuestCurrency(userId, reward);
  return { ok: true, reward };
}

/** Builds the full quest_state payload for a player: every active quest
 * def alongside that player's current progress for it, plus both bars.
 * Sent on demand (case 'quest_state') and after any quest-affecting
 * action, so the client never has to guess when something changed. */
async function buildQuestState(userId) {
  const dailyKey = Engine.dailyPeriodKey(), weeklyKey = Engine.weeklyPeriodKey();
  const periodFor = scope => scope === 'daily' ? dailyKey : scope === 'weekly' ? weeklyKey : Engine.PERMANENT_PERIOD_KEY;
  const quests = [];
  for (const def of Engine.QUEST_DEFS) {
    const row = await getQuestProgressRow(userId, def.id, periodFor(def.scope));
    quests.push({
      id: def.id, scope: def.scope, title: def.title, desc: def.desc, track: def.track,
      target: def.target, progress: row.progress, completed: row.completed, claimed: row.claimed,
      points: def.points || null, reward: def.reward || null, periodKey: periodFor(def.scope),
    });
  }
  const dailyBar = await getQuestBarRow(userId, 'daily', dailyKey);
  const weeklyBar = await getQuestBarRow(userId, 'weekly', weeklyKey);
  return {
    quests,
    // `rewards` mirrors QUEST_BAR_MILESTONE_REWARDS[scope] purely so the
    // client can render an accurate "+N gold" on each milestone diamond,
    // the actual currency isn't credited until claim_milestone is called
    // for it (see claimBarMilestone), this is display-only ahead of that.
    dailyBar: { points: dailyBar.points, claimedMilestones: dailyBar.claimedMilestones, milestones: Engine.QUEST_BAR_MILESTONES, rewards: Engine.QUEST_BAR_MILESTONE_REWARDS.daily, periodKey: dailyKey },
    weeklyBar: { points: weeklyBar.points, claimedMilestones: weeklyBar.claimedMilestones, milestones: Engine.QUEST_BAR_MILESTONES, rewards: Engine.QUEST_BAR_MILESTONE_REWARDS.weekly, periodKey: weeklyKey },
  };
}

/** Global chat is written client -> Supabase directly (see index.html's
 * GlobalChat.sb.from('global_chat_messages').insert), never through this
 * server's own WebSocket, so counting messages for the chat_message quest
 * track can't hook a WS message handler the way every other quest track
 * does. Instead the server subscribes to the table itself (service role,
 * same posture as everything else here: verification lives server-side
 * even though the write path doesn't funnel through server.js) and treats
 * each real INSERT row as the event. No-ops entirely without Supabase,
 * guest mode has no persisted global chat to observe anyway. */
function startGlobalChatQuestWatcher() {
  if (!HAS_SUPABASE) return;
  supabase
    .channel('quest-global-chat-watch')
    .on('postgres_changes', { event: 'INSERT', schema: 'public', table: 'global_chat_messages' }, payload => {
      // The table's actual sender column is `sender_id` (see index.html's
      // GlobalChat inserts / supabase-schema.sql), it's the same stable
      // guestId used to authenticate with this WS server, not a `user_id`
      // column (that name doesn't exist on this table).
      const userId = payload?.new?.sender_id;
      if (userId) recordQuestEvent(userId, Engine.QUEST_TRACK.CHAT_MESSAGE, 1).catch(e => console.error('[arena] chat quest event failed', e));
    })
    .subscribe();
}

/* ── CONNECTION WRAPPER ───────────────────────────────────────────── */
class Connection {
  constructor(ws) {
    this.ws = ws;
    this.userId = null;
    this.username = null;
    this.icon = null;
    this.cardLibraryHash = null;
    this.presenceOnline = false; // only true after the client's first explicit 'heartbeat'
    this.lastHeartbeat = 0;
    this.alive = true;
    ws.on('pong', () => { this.alive = true; });
  }
  send(msg) { if (this.ws.readyState === this.ws.OPEN) this.ws.send(JSON.stringify(msg)); }
}

/* ── MATCH ────────────────────────────────────────────────────────── */
class Match {
  constructor(userA, userB, deckA, deckB, ranked = true) {
    this.id = crypto.randomUUID();
    this.users = [userA, userB]; // side 0, side 1
    /** Whether a win/loss here moves rankPoints. Casual queue matches set
     * this false; duels, tournaments, and ranked-queue matches leave it at
     * the default true. Gold reward and win/loss counts are unaffected
     * either way, only the rank ladder cares about this flag. */
    this.ranked = ranked !== false;
    this.sides = [Engine.freshSide(deckA), Engine.freshSide(deckB)];
    this.actedThisTurn = [new Set(), new Set()];
    this.phase = 'SETUP';
    this.turn = 0; // side index whose turn it is (meaningless during SETUP)
    this.turnCounter = 0; // increments once per runTurnStart call (both sides combined), lets a per-card
                           // effect (e.g. a revive ability's one-turn cooldown) know "this side's next turn"
                           // as turnCounter+2, since turns strictly alternate 0,1,0,1,...
    this.readyForBattle = [false, false];
    this.timer = null;
    this.disconnectTimers = [null, null];
    /** userIds currently spectating this match, see addSpectator/removeSpectator. */
    this.spectators = new Set();
    matches.set(this.id, this);
    this.users.forEach(u => activeMatchByUser.set(u, this));
    // Tell every connected client these two are now "in a match" so their
    // avatar becomes the purple spectate-eye anywhere it's shown.
    broadcastAll({ type:'match_presence', userIds:this.users, inMatch:true });
  }

  otherSide(side) { return side === 0 ? 1 : 0; }
  sideOf(userId) { return this.users[0] === userId ? 0 : this.users[1] === userId ? 1 : -1; }

  conn(side) { return connections.get(this.users[side]) || null; }

  /** Relays a chat line to both participants in this match only, never
   * broadcast anywhere else. Silently drops empty/oversized text instead
   * of erroring, since a stray keystroke shouldn't need a round trip. */
  handleChat(userId, text) {
    const clean = String(text || '').trim().slice(0, 240);
    if (!clean) return;
    const side = this.sideOf(userId);
    if (side === -1) return;
    if (!isAllowedChatText(clean)) { this.conn(side)?.send({ type: 'error', reason: 'invalid_chat_image' }); return; }
    const payload = {
      type: 'battle_chat',
      matchId: this.id,
      from: userId,
      name: this.conn(side)?.username || 'Pestmaster',
      icon: this.conn(side)?.icon || 'star',
      text: clean,
      ts: Date.now(),
    };
    for (let s = 0; s < 2; s++) {
      const c = this.conn(s);
      if (c) c.send(payload);
    }
  }

  broadcastState(events) {
    for (let side = 0; side < 2; side++) {
      const c = this.conn(side);
      if (c) c.send({ type: 'state', matchId: this.id, phase: this.phase, turn: this.turn, you: side, state: this.perspective(side), events: events || [] });
    }
    this.broadcastToSpectators(events);
  }

  /** Public, hidden-hand-free view of both sides, spectators never see
   * either player's hand, only counts, matching how the opponent's hand is
   * already hidden from a normal player. */
  spectatorView() {
    const strip = s => ({
      hp: s.hp, maxHp: s.maxHp, activeCard: s.activeCard, activeCard2: s.activeCard2,
      weaponCard: s.weaponCard, defenseCard: s.defenseCard, deckCount: s.deck.length, handCount: s.hand.length,
      graveyardCount: s.graveyard.length, creaturesLeft: Engine.aliveCreatureCount(s),
    });
    return { sideA: strip(this.sides[0]), sideB: strip(this.sides[1]) };
  }

  addSpectator(userId) { this.spectators.add(userId); }
  removeSpectator(userId) { this.spectators.delete(userId); }

  broadcastToSpectators(events) {
    if (!this.spectators.size) return;
    const payload = {
      type: 'spectate_state', matchId: this.id, phase: this.phase, turn: this.turn,
      players: [
        { userId: this.users[0], username: this.usernames?.[0] || 'Player', icon: this.icons?.[0] || 'star', rank: this.ranks?.[0] || null },
        { userId: this.users[1], username: this.usernames?.[1] || 'Player', icon: this.icons?.[1] || 'star', rank: this.ranks?.[1] || null },
      ],
      state: this.spectatorView(), events: events || [],
    };
    for (const uid of this.spectators) connections.get(uid)?.send(payload);
  }

  /** Notify every current spectator the match is over, and forget them,
   * called right before the match itself is torn down. */
  clearSpectators(reason) {
    for (const uid of this.spectators) {
      connections.get(uid)?.send({ type:'spectate_ended', matchId:this.id, reason: reason || 'finished' });
      if (spectatingUserMatch.get(uid) === this.id) spectatingUserMatch.delete(uid);
    }
    this.spectators.clear();
  }

  /** Never leak the opponent's hand contents, only its count. Your own
   * graveyard is sent in full (you need to see it to pick a revive target);
   * the opponent's is just a count, same treatment as their hand. */
  perspective(side) {
    const opp = this.otherSide(side);
    const strip = s => ({
      hp: s.hp, maxHp: s.maxHp, activeCard: s.activeCard, activeCard2: s.activeCard2,
      weaponCard: s.weaponCard, defenseCard: s.defenseCard, deckCount: s.deck.length,
      creaturesLeft: Engine.aliveCreatureCount(s), reviveCharges: s.reviveCharges,
    });
    return {
      you: { ...strip(this.sides[side]), hand: this.sides[side].hand, graveyard: this.sides[side].graveyard },
      opponent: { ...strip(this.sides[opp]), handCount: this.sides[opp].hand.length, graveyardCount: this.sides[opp].graveyard.length },
      actedThisTurn: [...this.actedThisTurn[side]],
    };
  }

  clearTimer() { if (this.timer) { clearTimeout(this.timer); this.timer = null; } }

  armSetupTimer() {
    this.clearTimer();
    this.timer = setTimeout(() => this.forceBattleStart(), SETUP_TIME_MS);
  }
  armTurnTimer() {
    this.clearTimer();
    this.timer = setTimeout(() => this.autoEndTurn(), TURN_TIME_MS);
  }

  forceBattleStart() {
    if (this.phase !== 'SETUP') return;
    this.readyForBattle = [true, true];
    this.startBattle();
  }

  maybeStartBattle() {
    if (this.phase === 'SETUP' && this.readyForBattle[0] && this.readyForBattle[1]) this.startBattle();
  }

  startBattle() {
    this.clearTimer();
    const first = Math.random() < 0.5 ? 0 : 1;
    this.phase = 'MAIN';
    this.turn = first;
    this.actedThisTurn = [new Set(), new Set()];
    this.broadcastState([{ t:'coinflip', firstSide:first }]);
    this.runTurnStart(true);
  }

  /** onTurnStart effects + draw, mirroring startTurn() in the original client engine. */
  runTurnStart(isFirstTurnOfMatch) {
    this.turnCounter++;
    const side = this.turn;
    const entity = this.sides[side];
    if (!isFirstTurnOfMatch && entity.deck.length > 0 && entity.hand.length < 6) entity.hand.push(entity.deck.pop());
    const ctx = { events: [], skipTurn: false };
    if (entity.activeCard || entity.activeCard2) {
      Engine.processEffects(this, entity, 'onTurnStart', ctx, side);
      Engine.checkCardDeath(this, side, ctx.events);
    }
    Engine.decayHandEffects(entity, ctx.events, side);
    this.actedThisTurn = [new Set(), new Set()];
    const over = Engine.isMatchOver(this);
    if (over !== null) { this.broadcastState(ctx.events); if (over === 'draw') this.finishDraw(); else this.finish(over); return; }
    if (ctx.skipTurn) {
      this.broadcastState(ctx.events.concat([{ t:'turn_skip', side }]));
      setTimeout(() => this.endTurn(side, true), 1200);
      return;
    }
    this.broadcastState(ctx.events);
    this.armTurnTimer();
  }

  /** Scans a just-resolved action's events for `effect_applied` entries and
   * credits whichever side actually caused each one (a defense reflecting
   * an effect back onto the attacker is credited to the defender, not the
   * attacker, see the `side` on each effect_applied event, set in
   * game-engine.js's performHit/executeAbilityEffect) toward the Status
   * Report quest. Called after handleAttack, handleDeploy (rocks-on-swap
   * only, deploy itself never carries an ability anymore) and
   * handleUseAbility, since any of those can produce an on-hit or
   * on-ability effect. */
  recordStatusEffectQuests(events) {
    const bySide = {};
    for (const e of events) { if (e.t === 'effect_applied') bySide[e.side] = (bySide[e.side] || 0) + 1; }
    for (const [sideStr, count] of Object.entries(bySide)) {
      const uid = this.users[Number(sideStr)];
      if (uid) recordQuestEvent(uid, Engine.QUEST_TRACK.STATUS_EFFECT_APPLY, count).catch(e => console.error('[arena] status_effect_apply quest failed', e));
    }
  }

  handleDeploy(userId, msg) {
    const side = this.sideOf(userId); if (side === -1) return this.errTo(userId, 'not_in_match');
    if (this.phase !== 'SETUP' && !(this.phase === 'MAIN' && this.turn === side)) return this.errTo(userId, 'not_your_turn');
    const entity = this.sides[side];
    const idx = entity.hand.findIndex(c => c.instanceId === msg.instanceId);
    if (idx === -1) return this.errTo(userId, 'card_not_in_hand');
    const card = entity.hand[idx];
    const events = [];

    if (card.cardType === 'weapon') {
      const old = entity.weaponCard; entity.weaponCard = card; entity.hand.splice(idx, 1);
      if (old) entity.hand.push(old);
      events.push({ t:'deploy', side, slotType:'weapon', card });
    } else if (card.cardType === 'defense') {
      const old = entity.defenseCard; entity.defenseCard = card; entity.hand.splice(idx, 1);
      if (old) entity.hand.push(old);
      events.push({ t:'deploy', side, slotType:'defense', card });
    } else if (!entity.activeCard) {
      entity.activeCard = card; entity.hand.splice(idx, 1);
      Engine.applyRocksOnSwap(this, side, 'slot1', events);
      events.push({ t:'deploy', side, slotType:'slot1', card, swapped:false });
    } else if (!entity.activeCard2) {
      entity.activeCard2 = card; entity.hand.splice(idx, 1);
      Engine.applyRocksOnSwap(this, side, 'slot2', events);
      events.push({ t:'deploy', side, slotType:'slot2', card, swapped:false });
    } else {
      // Both slots are full, so this is really a swap. Which slot gets swapped is the
      // client's choice ('Swap Slot 1' vs 'Swap Slot 2' in the inspect panel), sent as
      // msg.slot; default to slot1 only if the client didn't specify one (older client).
      const swapSlot = (msg.slot === 'slot2') ? 'slot2' : 'slot1';
      const activeKey = swapSlot === 'slot2' ? 'activeCard2' : 'activeCard';
      // the outgoing card takes its own 'rocks' hit on the way out (if it's carrying
      // that effect), then the incoming card takes its own 'rocks' hit on the way in,
      // same effect, two independent triggers, exactly like swapping past any other
      // hazard would.
      events.push({ t:'deploy', side, slotType:swapSlot, card, swapped:true });
      const old = entity[activeKey]; // capture before the rocks check, since killCard would null this slot
      const oldSurvived = Engine.applyRocksOnSwap(this, side, swapSlot, events);
      entity[activeKey] = card; entity.hand.splice(idx, 1);
      if (oldSurvived) entity.hand.push(old);
      Engine.applyRocksOnSwap(this, side, swapSlot, events);
      Engine.checkCardDeath(this, side, events);
    }
    this.recordStatusEffectQuests(events);
    this.broadcastState(events);
    if (this.phase === 'SETUP') this.armSetupTimer();
  }

  handleReady(userId) {
    const side = this.sideOf(userId); if (side === -1) return;
    if (this.phase !== 'SETUP') return;
    this.readyForBattle[side] = true;
    this.conn(this.otherSide(side))?.send({ type:'opponent_ready' });
    this.maybeStartBattle();
  }

  handleAttack(userId, msg) {
    const side = this.sideOf(userId); if (side === -1) return this.errTo(userId, 'not_in_match');
    if (this.phase !== 'MAIN' || this.turn !== side) return this.errTo(userId, 'not_your_turn');
    const slot = msg.slot === 'slot2' ? 'slot2' : 'slot1';
    const target = msg.target === 'slot1' || msg.target === 'slot2' ? msg.target : null;
    const atkIndex = [0, 1].includes(msg.atkIndex) ? msg.atkIndex : 1;

    const result = Engine.executeAttack(this, side, slot, target, atkIndex);
    if (!result.ok) return this.errTo(userId, result.reason);
    this.recordStatusEffectQuests(result.events);
    this.broadcastState(result.events);

    const over = Engine.isMatchOver(this);
    if (over !== null) { if (over === 'draw') this.finishDraw(); else this.finish(over); return; }

    const slot1Done = !this.sides[side].activeCard || this.actedThisTurn[side].has('slot1');
    const slot2Done = !this.sides[side].activeCard2 || this.actedThisTurn[side].has('slot2');
    if (slot1Done && slot2Done) setTimeout(() => this.endTurn(side), 600);
    else this.armTurnTimer();
  }

  /** A card whose top effect is an `ability` carrying a `revive` block
   * (instead of e.g. an on-deploy curse/heal `ability`) can spend its turn
   * action reviving any one creature from this side's own graveyard,
   * player's choice of which, not automatic/earliest-first. `msg.target` is
   * the dead creature's instanceId. (A `passive`-type self-revive is a
   * completely separate, fully automatic mechanic, see
   * `Engine.tryPassiveRevive`, and never goes through this handler.) */
  handleUseAbility(userId, msg) {
    const side = this.sideOf(userId); if (side === -1) return this.errTo(userId, 'not_in_match');
    if (this.phase !== 'MAIN' || this.turn !== side) return this.errTo(userId, 'not_your_turn');
    const slot = msg.slot === 'slot2' ? 'slot2' : 'slot1';
    const actingCard = Engine.cardInSlot(this.sides[side], slot);
    if (!actingCard) return this.errTo(userId, 'no_card_in_slot');

    // Two entirely different ability shapes share this one message type:
    // a revive (msg.target is a dead card's instanceId, from the graveyard
    // picker) vs a heal/effects ability (msg.target is a board slot key,
    // 'slot1'/'slot2', from clicking a creature on the board — see
    // chooseAbilityTarget client-side). Which one a given card even offers
    // is entirely determined by its own topEffect, not by anything the
    // client claims, so dispatch off the card's own data, not msg shape.
    const result = actingCard.topEffect?.revive
      ? Engine.executeRevive(this, side, slot, msg.target)
      : Engine.executeAbilityEffect(this, side, slot, msg.target);
    if (!result.ok) return this.errTo(userId, result.reason);
    this.recordStatusEffectQuests(result.events);
    this.broadcastState(result.events);

    const slot1Done = !this.sides[side].activeCard || this.actedThisTurn[side].has('slot1');
    const slot2Done = !this.sides[side].activeCard2 || this.actedThisTurn[side].has('slot2');
    if (slot1Done && slot2Done) setTimeout(() => this.endTurn(side), 600);
    else this.armTurnTimer();
  }

  handleEndTurn(userId) {
    const side = this.sideOf(userId); if (side === -1) return;
    if (this.phase !== 'MAIN' || this.turn !== side) return;
    this.endTurn(side);
  }

  endTurn(side) {
    if (this.phase !== 'MAIN' || this.turn !== side) return;
    this.turn = this.otherSide(side);
    this.runTurnStart(false);
  }
  autoEndTurn() { if (this.phase === 'MAIN') this.endTurn(this.turn); }

  errTo(userId, reason) { connections.get(userId)?.send({ type:'error', reason }); }

  handleForfeit(userId) {
    const side = this.sideOf(userId); if (side === -1) return;
    this.finish(this.otherSide(side));
  }

  handleDisconnect(userId) {
    const side = this.sideOf(userId); if (side === -1) return;
    this.conn(this.otherSide(side))?.send({ type:'opponent_disconnected', graceMs: RECONNECT_GRACE_MS });
    this.disconnectTimers[side] = setTimeout(() => {
      if (matches.has(this.id)) this.finish(this.otherSide(side));
    }, RECONNECT_GRACE_MS);
  }
  handleReconnect(userId) {
    const side = this.sideOf(userId); if (side === -1) return;
    if (this.disconnectTimers[side]) { clearTimeout(this.disconnectTimers[side]); this.disconnectTimers[side] = null; }
    this.conn(this.otherSide(side))?.send({ type:'opponent_reconnected' });
    this.conn(side)?.send({ type:'match_found', matchId: this.id, youAre: side, opponentName: this.usernames?.[this.otherSide(side)] || 'Opponent', opponentIcon: this.icons?.[this.otherSide(side)] || 'star', opponentRank: this.ranks?.[this.otherSide(side)] || null, resumed: true });
    this.broadcastState([]);
  }

  async finish(winnerSide) {
    if (this.finished) return; this.finished = true;
    this.clearTimer();
    this.disconnectTimers.forEach(t => t && clearTimeout(t));
    this.clearSpectators('finished');
    broadcastAll({ type:'match_presence', userIds:this.users, inMatch:false });
    const winnerId = this.users[winnerSide], loserId = this.users[this.otherSide(winnerSide)];
    matches.delete(this.id);
    this.users.forEach(u => activeMatchByUser.delete(u));

    let reward = { gold: WIN_GOLD_REWARD, gems: 0 };
    try { reward = await applyMatchReward(winnerId, loserId, this.ranked); }
    catch (e) { console.error('[arena] reward write failed', e); }
    // (match_play/match_win/ranked/gold/rank/nemesis quest hooks all live
    // inside applyMatchReward now, it already has clean access to the
    // final gold/rank values needed for the threshold-style ones)

    let tournamentSummary = null;
    if (this.tournamentMeta) {
      try { tournamentSummary = await onTournamentMatchFinished(this, winnerId, loserId); }
      catch (e) { console.error('[arena] tournament advance failed', e); }
    }

    // The two sides' fetchProfile calls are independent reads, run them
    // together rather than one after another.
    await Promise.all([0, 1].map(async side => {
      const c = this.conn(side);
      if (!c) return;
      const won = this.users[side] === winnerId;
      let profile = null;
      try { profile = await fetchProfile(this.users[side]); } catch (e) { /* best effort */ }
      c.send({ type:'match_over', result: won ? 'win' : 'loss', reward: won ? reward : { gold:0, gems:0 }, profile, tournament: tournamentSummary, ranked: this.ranked });
    }));
  }

  /** Both sides ran out of creatures on the same exchange, e.g. a curse
   * recoil kills the attacker's last creature on the same swing that kills
   * the defender's last creature. Nobody wins; no reward either side. */
  async finishDraw() {
    if (this.finished) return; this.finished = true;
    this.clearTimer();
    this.disconnectTimers.forEach(t => t && clearTimeout(t));
    this.clearSpectators('finished');
    broadcastAll({ type:'match_presence', userIds:this.users, inMatch:false });
    matches.delete(this.id);
    this.users.forEach(u => activeMatchByUser.delete(u));
    if (this.tournamentMeta) {
      // A draw can't leave a bracket slot undecided, start an immediate
      // rematch instead of the normal no-reward draw teardown below.
      for (let side = 0; side < 2; side++) this.conn(side)?.send({ type:'tournament_rematch', bracketId: this.tournamentMeta.bracketId });
      try { await rematchTournamentMatch(this); } catch (e) { console.error('[arena] tournament rematch failed', e); }
      return;
    }
    await Promise.all([0, 1].map(async side => {
      const c = this.conn(side);
      if (!c) return;
      let profile = null;
      try { profile = await fetchProfile(this.users[side]); } catch (e) { /* best effort */ }
      c.send({ type:'match_over', result:'draw', reward:{ gold:0, gems:0 }, profile });
    }));
  }
}

/* ── MATCHMAKING ──────────────────────────────────────────────────── */
function clearQueueTimer(userId) {
  const t = queueTimers.get(userId);
  if (t) { clearTimeout(t); queueTimers.delete(userId); }
}

/** Arm (or re-arm) the randomized 13–17s window after which, if this user is
 * still waiting, they're matched against a bot instead of a real opponent. */
function armBotFallback(userId, mode) {
  clearQueueTimer(userId);
  const delay = BOT_FALLBACK_MIN_MS + Math.floor(Math.random() * (BOT_FALLBACK_MAX_MS - BOT_FALLBACK_MIN_MS));
  queueTimers.set(userId, setTimeout(() => startBotMatch(userId, mode), delay));
}

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const randMs = (min, max) => min + Math.floor(Math.random() * (max - min + 1));

/** Picks the next card the bot should play out of its hand: gear first (if it
 * doesn't already have a weapon/defense equipped), then a creature/wizard
 * card for whichever active slot is still open. Mirrors reasonable, if not
 * perfectly optimal, human setup order. */
function pickBotDeployCard(entity) {
  if (!entity.weaponCard) {
    const w = entity.hand.find(c => c.cardType === 'weapon');
    if (w) return w;
  }
  if (!entity.defenseCard) {
    const d = entity.hand.find(c => c.cardType === 'defense');
    if (d) return d;
  }
  if (!entity.activeCard) {
    const m = entity.hand.find(c => c.cardType !== 'weapon' && c.cardType !== 'defense');
    if (m) return m;
  }
  if (!entity.activeCard2) {
    const m = entity.hand.find(c => c.cardType !== 'weapon' && c.cardType !== 'defense');
    if (m) return m;
  }
  return null;
}

/** Drives a bot side through an otherwise-normal Match: sets up its board
 * during SETUP, then plays/attacks/ends turn during MAIN, all through the
 * same handle* methods a real client's messages would hit, just with
 * human-like pauses instead of instant, robotic timing. */
function attachBotAI(match) {
  const botSide = match.botSide;
  const humanSide = match.otherSide(botSide);
  let acting = false;

  async function runSetup() {
    while (!match.finished && match.phase === 'SETUP') {
      const entity = match.sides[botSide];
      const card = pickBotDeployCard(entity);
      if (!card) break;
      await sleep(randMs(700, 1700));
      if (match.finished || match.phase !== 'SETUP') return;
      match.handleDeploy(match.botUserId, { instanceId: card.instanceId });
    }
    if (match.finished || match.phase !== 'SETUP') return;
    await sleep(randMs(500, 1300));
    if (match.finished || match.phase !== 'SETUP') return;
    match.handleReady(match.botUserId);
  }

  async function runMainTurn() {
    if (acting) return;
    acting = true;
    try {
      const entity = match.sides[botSide];
      const stillBotsTurn = () => !match.finished && match.phase === 'MAIN' && match.turn === botSide;

      // fill any open gear/creature slots before attacking, same priority as setup
      let guard = 0;
      let toDeploy = pickBotDeployCard(entity);
      while (toDeploy && stillBotsTurn() && guard < 4) {
        await sleep(randMs(500, 1300));
        if (!stillBotsTurn()) break;
        match.handleDeploy(match.botUserId, { instanceId: toDeploy.instanceId });
        toDeploy = pickBotDeployCard(entity);
        guard++;
      }

      for (const slotKey of ['slot1', 'slot2']) {
        if (!stillBotsTurn()) break;
        const card = slotKey === 'slot1' ? entity.activeCard : entity.activeCard2;
        if (!card || match.actedThisTurn[botSide].has(slotKey)) continue;
        await sleep(randMs(700, 1900));
        if (!stillBotsTurn()) break;
        const oppEntity = match.sides[humanSide];
        if (card.topEffect?.type === 'ability' && card.topEffect?.revive && entity.graveyard.length && Math.random() < 0.7) {
          // Boss/Overlord graveyard entries aren't legal revive targets (see
          // executeRevive), filter them out of the bot's candidate pool so
          // it doesn't waste its whole turn on a guaranteed-rejected attempt.
          const revivable = entity.graveyard.filter(c => {
            const def = Engine.CardById[c.baseId];
            return !def || (def.classification !== 'boss' && def.classification !== 'overlord');
          });
          if (revivable.length) {
            const target = revivable[Math.floor(Math.random() * revivable.length)];
            match.handleUseAbility(match.botUserId, { slot: slotKey, target: target.instanceId });
            continue;
          }
        }
        // Non-revive ability (heal/effects): the bot uses it about as often
        // as it would otherwise attack with that slot, same as a human
        // choosing between the two from the inspect panel. Which target it
        // needs (a board slot key, or none at all) depends entirely on the
        // ability's own shape, exactly like executeAbilityEffect requires.
        if (card.topEffect?.type === 'ability' && !card.topEffect?.revive && Math.random() < 0.6) {
          const heal = card.topEffect.heal;
          const effects = card.topEffect.effects;
          if (heal && heal.target === 'ally') {
            const candidates = ['slot1', 'slot2'].filter(sk => (sk === 'slot1' ? entity.activeCard : entity.activeCard2));
            if (candidates.length) {
              // heal whichever of its own two creatures is proportionally lower on HP, itself included
              candidates.sort((a, b) => {
                const ca = a === 'slot1' ? entity.activeCard : entity.activeCard2;
                const cb = b === 'slot1' ? entity.activeCard : entity.activeCard2;
                return (ca.currentHp / ca.maxHp) - (cb.currentHp / cb.maxHp);
              });
              match.handleUseAbility(match.botUserId, { slot: slotKey, target: candidates[0] });
              continue;
            }
          } else if (heal) { // 'self' or 'side', no target needed at all
            match.handleUseAbility(match.botUserId, { slot: slotKey });
            continue;
          } else if (effects && effects.length) {
            const targetSlot = oppEntity.activeCard ? 'slot1' : (oppEntity.activeCard2 ? 'slot2' : null);
            if (targetSlot) {
              match.handleUseAbility(match.botUserId, { slot: slotKey, target: targetSlot });
              continue;
            }
          }
        }
        const targetSlot = oppEntity.activeCard ? 'slot1' : (oppEntity.activeCard2 ? 'slot2' : null);
        const atkIndex = (card.topEffect?.type === 'attack' && Math.random() < 0.5) ? 0 : 1;
        match.handleAttack(match.botUserId, { slot: slotKey, target: targetSlot, atkIndex });
      }

      if (stillBotsTurn()) {
        await sleep(randMs(400, 1000));
        if (stillBotsTurn()) match.handleEndTurn(match.botUserId);
      }
    } finally {
      acting = false;
    }
  }

  runSetup();
  const watcher = setInterval(() => {
    if (match.finished) { clearInterval(watcher); return; }
    if (match.phase === 'MAIN' && match.turn === botSide && !acting) runMainTurn();
  }, 500);
}

/** Pulled from the queue once its randomized search window has elapsed with
 * no real opponent found. Builds a normal two-sided Match, the human's
 * client only ever sees a `match_found` with a human-sounding opponent name
 * and never learns the other "player" is server-controlled. */
async function startBotMatch(userId, mode) {
  queueTimers.delete(userId);
  const q = queueForMode(mode);
  const i = q.indexOf(userId);
  if (i === -1) return; // already matched with a real opponent, or left the queue
  q.splice(i, 1);
  queueMode.delete(userId);
  const ranked = mode !== 'casual';

  const conn = connections.get(userId);
  if (!conn || conn.ws.readyState !== conn.ws.OPEN) return;

  try {
    const profile = await fetchProfile(userId, conn.username);
    // Deck legality is already checked at queue_join time, but re-check here too,
    // this fires after a randomized delay, so the deck could have been edited (or a
    // stale one desynced) in the meantime. Block rather than silently handing out a
    // random deck.
    if (!Engine.isDeckLegal(profile.deck)) {
      removeFromQueues(userId);
      conn.send({ type: 'error', reason: 'invalid_deck', deckSize: (profile.deck || []).length, requiredSize: Engine.DECK_SIZE });
      return;
    }
    const humanDeck = Engine.buildDeckFromIds(profile.deck);
    const botDeck = Engine.buildDeckFromIds(null); // the bot's own deck is always freshly randomized, this isn't a fallback
    const botUserId = `bot:${crypto.randomUUID()}`;
    const botName = pickBotName();
    const humanSide = Math.random() < 0.5 ? 0 : 1;

    const uA = humanSide === 0 ? userId : botUserId;
    const uB = humanSide === 0 ? botUserId : userId;
    const dA = humanSide === 0 ? humanDeck : botDeck;
    const dB = humanSide === 0 ? botDeck : humanDeck;

    const match = new Match(uA, uB, dA, dB, ranked);
    match.usernames = humanSide === 0 ? [profile.username, botName] : [botName, profile.username];
    match.icons = humanSide === 0 ? [profile.icon || 'star', 'skull'] : ['skull', profile.icon || 'star'];
    match.ranks = humanSide === 0 ? [profile.rank, null] : [null, profile.rank];
    match.botSide = humanSide === 0 ? 1 : 0;
    match.botUserId = botUserId;

    conn.send({ type: 'match_found', matchId: match.id, youAre: humanSide, opponentName: botName, opponentIcon: 'skull', opponentRank: null, ranked });
    match.broadcastState([]);
    attachBotAI(match);
  } catch (e) {
    console.error('[arena] bot match failed', e);
    conn.send({ type: 'error', reason: 'matchmaking_failed' });
  }
}

/** Starts a direct match between two friends who both agreed to a duel,
 * same match-creation shape as tryMatch/startBotMatch, just without the
 * queue or bot-fallback machinery around it. */
async function startDuelMatch(uA, uB) {
  const connA = connections.get(uA), connB = connections.get(uB);
  try {
    const [profileA, profileB] = await Promise.all([fetchProfile(uA), fetchProfile(uB)]);
    const legalA = Engine.isDeckLegal(profileA.deck), legalB = Engine.isDeckLegal(profileB.deck);
    if (!legalA || !legalB) {
      if (!legalA) connA?.send({ type:'error', reason:'invalid_deck', deckSize:(profileA.deck||[]).length, requiredSize:Engine.DECK_SIZE });
      if (!legalB) connB?.send({ type:'error', reason:'invalid_deck', deckSize:(profileB.deck||[]).length, requiredSize:Engine.DECK_SIZE });
      return;
    }
    const deckA = Engine.buildDeckFromIds(profileA.deck);
    const deckB = Engine.buildDeckFromIds(profileB.deck);
    const match = new Match(uA, uB, deckA, deckB);
    match.usernames = [profileA.username, profileB.username];
    match.icons = [profileA.icon || 'star', profileB.icon || 'star'];
    match.ranks = [profileA.rank, profileB.rank];
    connA?.send({ type: 'match_found', matchId: match.id, youAre: 0, opponentName: profileB.username, opponentIcon: profileB.icon || 'star', opponentRank: profileB.rank });
    connB?.send({ type: 'match_found', matchId: match.id, youAre: 1, opponentName: profileA.username, opponentIcon: profileA.icon || 'star', opponentRank: profileA.rank });
    match.broadcastState([]);
    // This path only ever runs for the friends-only duel_request/duel_respond
    // flow (see the case handlers above), never for random matchmaking,
    // so both sides count toward the "Duel a friend" quest.
    recordQuestEvent(uA, Engine.QUEST_TRACK.FRIEND_DUEL, 1).catch(e => console.error('[arena] friend_duel quest failed', e));
    recordQuestEvent(uB, Engine.QUEST_TRACK.FRIEND_DUEL, 1).catch(e => console.error('[arena] friend_duel quest failed', e));
  } catch (e) {
    console.error('[arena] duel match failed', e);
    connA?.send({ type: 'error', reason: 'duel_match_failed' });
    connB?.send({ type: 'error', reason: 'duel_match_failed' });
  }
}

/* ── TOURNAMENTS ──────────────────────────────────────────────────────
 * An "event" is what players register (and pay an entry fee) into ahead
 * of time, either a recurring official Daily/Weekly slot the server
 * maintains for itself (ensureUpcomingOfficialEvents) or a player-hosted
 * lobby (createUnofficialTournament). At its scheduled start_at,
 * tournamentSweep locks the event, keeps only the registrants who were
 * actually online right that instant (that's the whole "automatically
 * disqualified for not showing up" rule, see lockAndShardEvent), and
 * splits the rest into one or more brackets: official events shard into
 * groups of at most TOURNAMENT_BRACKET_SIZE, unofficial events are always
 * exactly one bracket (registration itself is capped at the host's chosen
 * player count). Both kinds share the exact same downstream machinery.
 *
 * A bracket match is just a normal Match (see the Match class above),
 * tagged with `tournamentMeta`, turn timers, reconnect grace, and normal
 * win/loss rewards all keep working unmodified. The only two places this
 * file's match code needs to know tournaments exist are the two hooks in
 * Match#finish()/#finishDraw() below.
 */

/* -- row <-> object mappers (camelCase objects are what the rest of this
 * section and the client both work with) -------------------------------- */
function rowToTournamentEvent(row) {
  return {
    id: row.id, kind: row.kind, name: row.name, hostId: row.host_id,
    bracketSizeCap: row.bracket_size_cap, entryCurrency: row.entry_currency,
    entryAmount: row.entry_amount, prizePoolPercent: row.prize_pool_percent,
    startAt: row.start_at, status: row.status, createdAt: row.created_at,
  };
}
function rowToRegistration(row) {
  return {
    eventId: row.event_id, userId: row.user_id, paidAmount: row.paid_amount,
    paidCurrency: row.paid_currency, checkedIn: row.checked_in, refunded: row.refunded,
    registeredAt: row.registered_at,
  };
}
function rowToBracket(row) {
  return {
    id: row.id, eventId: row.event_id, eventName: row.event_name, eventKind: row.event_kind,
    prizeCurrency: row.prize_currency, prizePool: row.prize_pool, winnerPayout: row.winner_payout,
    hostId: row.host_id || null, hostPayout: row.host_payout || 0,
    status: row.status, winnerId: row.winner_id,
    participants: row.participants || [], rounds: row.rounds || [],
    createdAt: row.created_at, completedAt: row.completed_at,
  };
}
function bracketToRow(b) {
  return {
    id: b.id, event_id: b.eventId, event_name: b.eventName, event_kind: b.eventKind,
    prize_currency: b.prizeCurrency, prize_pool: b.prizePool, winner_payout: b.winnerPayout,
    host_id: b.hostId || null, host_payout: b.hostPayout || 0,
    status: b.status, winner_id: b.winnerId || null,
    participants: b.participants, rounds: b.rounds,
    completed_at: b.completedAt || null,
  };
}

/* -- event CRUD (dual-path, same convention as guilds/marketplace) ------ */
async function createTournamentEventRow(fields) {
  if (!HAS_SUPABASE) {
    const id = crypto.randomUUID();
    const event = { id, kind: fields.kind, name: fields.name, hostId: fields.hostId || null,
      bracketSizeCap: fields.bracketSizeCap, entryCurrency: fields.entryCurrency, entryAmount: fields.entryAmount,
      prizePoolPercent: fields.prizePoolPercent, startAt: fields.startAt, status: 'scheduled', createdAt: new Date().toISOString() };
    guestTournamentEvents.set(id, event);
    guestTournamentRegistrations.set(id, new Map());
    return event;
  }
  const insert = {
    kind: fields.kind, name: fields.name, host_id: fields.hostId || null,
    bracket_size_cap: fields.bracketSizeCap, entry_currency: fields.entryCurrency, entry_amount: fields.entryAmount,
    prize_pool_percent: fields.prizePoolPercent, start_at: fields.startAt,
  };
  const { data, error } = await supabase.from('tournament_events').insert(insert).select('*').single();
  if (error) throw error;
  return rowToTournamentEvent(data);
}
async function getTournamentEvent(eventId) {
  if (!HAS_SUPABASE) return guestTournamentEvents.get(eventId) || null;
  const { data, error } = await supabase.from('tournament_events').select('*').eq('id', eventId).maybeSingle();
  if (error) throw error;
  return data ? rowToTournamentEvent(data) : null;
}
async function setTournamentEventStatus(eventId, status) {
  if (!HAS_SUPABASE) { const e = guestTournamentEvents.get(eventId); if (e) e.status = status; return; }
  await supabase.from('tournament_events').update({ status }).eq('id', eventId);
}
/** Latest (by start_at) event of a given kind, any status, used to decide whether a fresh official slot needs creating yet. */
async function eventsByStatus(statuses, { kinds = null, limit = 100 } = {}) {
  if (!HAS_SUPABASE) {
    let list = [...guestTournamentEvents.values()].filter(e => statuses.includes(e.status) && (!kinds || kinds.includes(e.kind)));
    list.sort((a, b) => a.startAt.localeCompare(b.startAt));
    return list.slice(0, limit);
  }
  let q = supabase.from('tournament_events').select('*').in('status', statuses).order('start_at', { ascending: true }).limit(limit);
  if (kinds) q = q.in('kind', kinds);
  const { data, error } = await q;
  if (error) throw error;
  return (data || []).map(rowToTournamentEvent);
}

/* -- registration CRUD --------------------------------------------------- */
async function getRegistration(eventId, userId) {
  if (!HAS_SUPABASE) return guestTournamentRegistrations.get(eventId)?.get(userId) || null;
  const { data, error } = await supabase.from('tournament_registrations').select('*').eq('event_id', eventId).eq('user_id', userId).maybeSingle();
  if (error) throw error;
  return data ? rowToRegistration(data) : null;
}
async function listRegistrations(eventId) {
  if (!HAS_SUPABASE) return [...(guestTournamentRegistrations.get(eventId)?.values() || [])];
  const { data, error } = await supabase.from('tournament_registrations').select('*').eq('event_id', eventId);
  if (error) throw error;
  return (data || []).map(rowToRegistration);
}
async function countRegistrations(eventId) {
  if (!HAS_SUPABASE) return guestTournamentRegistrations.get(eventId)?.size || 0;
  const { count, error } = await supabase.from('tournament_registrations').select('*', { count: 'exact', head: true }).eq('event_id', eventId);
  if (error) throw error;
  return count || 0;
}
async function createRegistration(eventId, userId, amount, currency) {
  const reg = { eventId, userId, paidAmount: amount, paidCurrency: currency, checkedIn: null, refunded: false, registeredAt: new Date().toISOString() };
  if (!HAS_SUPABASE) {
    if (!guestTournamentRegistrations.has(eventId)) guestTournamentRegistrations.set(eventId, new Map());
    guestTournamentRegistrations.get(eventId).set(userId, reg);
    return reg;
  }
  const { error } = await supabase.from('tournament_registrations').insert({ event_id: eventId, user_id: userId, paid_amount: amount, paid_currency: currency });
  if (error) throw error;
  return reg;
}
async function deleteRegistration(eventId, userId) {
  if (!HAS_SUPABASE) { guestTournamentRegistrations.get(eventId)?.delete(userId); return; }
  await supabase.from('tournament_registrations').delete().eq('event_id', eventId).eq('user_id', userId);
}
async function setRegistrationCheckedIn(eventId, userId, checkedIn) {
  if (!HAS_SUPABASE) { const r = guestTournamentRegistrations.get(eventId)?.get(userId); if (r) r.checkedIn = checkedIn; return; }
  await supabase.from('tournament_registrations').update({ checked_in: checkedIn }).eq('event_id', eventId).eq('user_id', userId);
}
async function markRegistrationRefunded(eventId, userId) {
  if (!HAS_SUPABASE) { const r = guestTournamentRegistrations.get(eventId)?.get(userId); if (r) r.refunded = true; return; }
  await supabase.from('tournament_registrations').update({ refunded: true }).eq('event_id', eventId).eq('user_id', userId);
}
/** Every event id (most recent first) this user has ever registered for, powers the "My Tournaments" list. */
async function listUserRegisteredEventIds(userId, limit) {
  if (!HAS_SUPABASE) {
    const ids = [];
    for (const [eventId, regs] of guestTournamentRegistrations.entries()) if (regs.has(userId)) ids.push(eventId);
    return ids.slice(-limit).reverse();
  }
  const { data, error } = await supabase.from('tournament_registrations').select('event_id, registered_at').eq('user_id', userId).order('registered_at', { ascending: false }).limit(limit);
  if (error) throw error;
  return (data || []).map(r => r.event_id);
}

/* -- bracket CRUD --------------------------------------------------------- */
async function createBracketRow(bracket) {
  if (!HAS_SUPABASE) { guestTournamentBrackets.set(bracket.id, bracket); return bracket; }
  const { error } = await supabase.from('tournament_brackets').insert(bracketToRow(bracket));
  if (error) throw error;
  return bracket;
}
async function saveBracket(bracket) {
  if (!HAS_SUPABASE) { guestTournamentBrackets.set(bracket.id, bracket); return; }
  const { error } = await supabase.from('tournament_brackets').update(bracketToRow(bracket)).eq('id', bracket.id);
  if (error) throw error;
}
async function getBracket(bracketId) {
  if (!HAS_SUPABASE) return guestTournamentBrackets.get(bracketId) || null;
  const { data, error } = await supabase.from('tournament_brackets').select('*').eq('id', bracketId).maybeSingle();
  if (error) throw error;
  return data ? rowToBracket(data) : null;
}
async function listBracketsForEvent(eventId) {
  if (!HAS_SUPABASE) return [...guestTournamentBrackets.values()].filter(b => b.eventId === eventId);
  const { data, error } = await supabase.from('tournament_brackets').select('*').eq('event_id', eventId);
  if (error) throw error;
  return (data || []).map(rowToBracket);
}
async function findParticipantBracket(eventId, userId) {
  const brackets = await listBracketsForEvent(eventId);
  return brackets.find(b => b.participants.some(p => p.userId === userId)) || null;
}
function bracketPayload(bracket) {
  return {
    id: bracket.id, eventId: bracket.eventId, eventName: bracket.eventName, eventKind: bracket.eventKind,
    prizeCurrency: bracket.prizeCurrency, prizePool: bracket.prizePool, winnerPayout: bracket.winnerPayout,
    hostId: bracket.hostId || null, hostPayout: bracket.hostPayout || 0,
    status: bracket.status, winnerId: bracket.winnerId,
    participants: bracket.participants, rounds: bracket.rounds,
  };
}
async function pushBracketUpdate(bracket, type = 'tournament_bracket_update') {
  const payload = { type, bracket: bracketPayload(bracket) };
  for (const p of bracket.participants) notifyUser(p.userId, payload);
}

/* -- official schedule ---------------------------------------------------- */
function officialEntryFor(kind) {
  return kind === 'official_daily'
    ? { currency: 'gold', amount: TOURNAMENT_DAILY_ENTRY_GOLD }
    : { currency: 'gems', amount: TOURNAMENT_WEEKLY_ENTRY_GEMS };
}
function nextDailySlot(now) {
  const d = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate(), TOURNAMENT_DAILY_HOUR_UTC, 0, 0, 0));
  if (d.getTime() <= now.getTime()) d.setUTCDate(d.getUTCDate() + 1);
  return d;
}
function nextWeeklySlot(now) {
  const d = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate(), TOURNAMENT_WEEKLY_HOUR_UTC, 0, 0, 0));
  const daysAhead = (TOURNAMENT_WEEKLY_DAY_UTC - d.getUTCDay() + 7) % 7;
  d.setUTCDate(d.getUTCDate() + daysAhead);
  if (d.getTime() <= now.getTime()) d.setUTCDate(d.getUTCDate() + 7);
  return d;
}
function formatEventTime(date) {
  return new Intl.DateTimeFormat('en-US', { timeZone: 'UTC', month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit' }).format(date) + ' UTC';
}
function officialEventName(kind, date) {
  return `${kind === 'official_daily' ? 'Daily Tournament' : 'Weekly Championship'}, ${formatEventTime(date)}`;
}
/** Keeps exactly one upcoming (status='scheduled') event of each official
 * kind available to register for at all times. Cheap to call often, it's
 * a no-op unless the currently-latest event of that kind isn't the slot
 * that should exist right now (i.e. the previous one just locked). */
async function ensureUpcomingOfficialEvents() {
  const now = new Date();
  for (const [kind, slotFn] of [['official_daily', nextDailySlot], ['official_weekly', nextWeeklySlot]]) {
    const slot = slotFn(now);
    const slotIso = slot.toISOString();
    // Fetch every currently-'scheduled' event of this kind, not just the
    // latest, so a duplicate created by a race (two server processes, or
    // two overlapping sweeps, both seeing "none exists yet" and both
    // inserting) gets cleaned up here instead of quietly persisting as two
    // simultaneous joinable daily/weekly tournaments.
    const scheduled = await eventsByStatus(['scheduled'], { kinds: [kind] });
    const forCorrectSlot = scheduled.filter(e => e.startAt === slotIso);
    if (forCorrectSlot.length > 1) {
      // Keep the oldest (first created), refund+cancel the rest.
      const [keep, ...extras] = forCorrectSlot.sort((a, b) => a.createdAt.localeCompare(b.createdAt));
      for (const dupe of extras) await cancelDuplicateOfficialEvent(dupe);
    }
    if (forCorrectSlot.length >= 1) {
      // Also clean up any stray 'scheduled' rows of this kind that don't
      // match the correct slot (e.g. left over from a clock/config change),
      // there should only ever be one joinable event per kind at a time.
      for (const stray of scheduled.filter(e => e.startAt !== slotIso)) await cancelDuplicateOfficialEvent(stray);
      continue;
    }
    const entry = officialEntryFor(kind);
    try {
      await createTournamentEventRow({
        kind, name: officialEventName(kind, slot), hostId: null,
        bracketSizeCap: TOURNAMENT_BRACKET_SIZE, entryCurrency: entry.currency, entryAmount: entry.amount,
        prizePoolPercent: TOURNAMENT_OFFICIAL_PRIZE_PERCENT, startAt: slotIso,
      });
    } catch (e) {
      // A unique constraint (tournament_events_one_scheduled_per_kind, see
      // supabase-schema.sql) rejects a second concurrent insert for the same
      // kind, that means another process just created it a moment ago,
      // which is fine; nothing to do here.
      if (!isUniqueViolation(e)) throw e;
    }
  }
}
async function cancelDuplicateOfficialEvent(event) {
  for (const reg of await listRegistrations(event.id)) {
    if (reg.paidAmount > 0) await adjustWallet(reg.userId, reg.paidCurrency === 'gold' ? reg.paidAmount : 0, reg.paidCurrency === 'gems' ? reg.paidAmount : 0);
    await markRegistrationRefunded(event.id, reg.userId);
    notifyUser(reg.userId, { type: 'tournament_refunded', eventId: event.id, reason: 'cancelled', profile: await fetchProfile(reg.userId) });
  }
  await setTournamentEventStatus(event.id, 'cancelled');
}
function isUniqueViolation(e) {
  return e && (e.code === '23505' || /duplicate key value/i.test(e.message || ''));
}

/* -- player-facing actions ------------------------------------------------- */
async function registerForTournament(userId, eventId) {
  const event = await getTournamentEvent(eventId);
  if (!event) return { error: 'tournament_not_found' };
  if (event.status !== 'scheduled' || new Date(event.startAt).getTime() <= Date.now()) return { error: 'tournament_locked' };
  if (await getRegistration(eventId, userId)) return { error: 'already_registered' };
  if (event.kind === 'unofficial' && (await countRegistrations(eventId)) >= event.bracketSizeCap) return { error: 'tournament_full' };
  const profile = await fetchProfile(userId);
  const balance = event.entryCurrency === 'gold' ? profile.gold : profile.gems;
  if (balance < event.entryAmount) return { error: 'tournament_insufficient_funds' };
  if (event.entryAmount > 0) await adjustWallet(userId, event.entryCurrency === 'gold' ? -event.entryAmount : 0, event.entryCurrency === 'gems' ? -event.entryAmount : 0);
  const registration = await createRegistration(eventId, userId, event.entryAmount, event.entryCurrency);
  recordQuestEvent(userId, Engine.QUEST_TRACK.TOURNAMENT_JOIN, 1).catch(e => console.error('[arena] tournament_join quest event failed', e));
  return { event, registration };
}
async function unregisterFromTournament(userId, eventId) {
  const event = await getTournamentEvent(eventId);
  if (!event) return { error: 'tournament_not_found' };
  if (event.status !== 'scheduled') return { error: 'tournament_locked' };
  const reg = await getRegistration(eventId, userId);
  if (!reg) return { error: 'not_registered' };
  if (reg.paidAmount > 0) await adjustWallet(userId, reg.paidCurrency === 'gold' ? reg.paidAmount : 0, reg.paidCurrency === 'gems' ? reg.paidAmount : 0);
  await deleteRegistration(eventId, userId);
  return { event };
}
function sanitizeTournamentName(name) {
  const trimmed = String(name || '').trim().replace(/\s+/g, ' ');
  if (trimmed.length < TOURNAMENT_NAME_MIN || trimmed.length > TOURNAMENT_NAME_MAX) return null;
  return trimmed;
}
async function countActiveHostedEvents(userId) {
  if (!HAS_SUPABASE) {
    let n = 0;
    for (const e of guestTournamentEvents.values()) if (e.hostId === userId && e.status === 'scheduled') n++;
    return n;
  }
  const { count, error } = await supabase.from('tournament_events').select('*', { count: 'exact', head: true }).eq('host_id', userId).eq('status', 'scheduled');
  if (error) throw error;
  return count || 0;
}
async function createUnofficialTournament(userId, msg) {
  const name = sanitizeTournamentName(msg.name);
  if (!name) return { error: 'invalid_name' };
  const maxPlayers = Math.round(Number(msg.maxPlayers));
  if (!Number.isFinite(maxPlayers) || maxPlayers < TOURNAMENT_UNOFFICIAL_MIN_PLAYERS || maxPlayers > TOURNAMENT_UNOFFICIAL_MAX_PLAYERS) return { error: 'invalid_max_players' };
  const prizePoolPercent = Math.round(Number(msg.prizePoolPercent));
  if (!Number.isFinite(prizePoolPercent) || prizePoolPercent < TOURNAMENT_UNOFFICIAL_PRIZE_PERCENT_MIN || prizePoolPercent > TOURNAMENT_UNOFFICIAL_PRIZE_PERCENT_MAX) return { error: 'invalid_prize_percent' };
  const entryCurrency = msg.entryCurrency === 'gems' ? 'gems' : msg.entryCurrency === 'gold' ? 'gold' : null;
  if (!entryCurrency) return { error: 'invalid_entry_currency' };
  const entryAmount = Math.round(Number(msg.entryAmount));
  const entryMax = entryCurrency === 'gold' ? TOURNAMENT_UNOFFICIAL_ENTRY_MAX_GOLD : TOURNAMENT_UNOFFICIAL_ENTRY_MAX_GEMS;
  if (!Number.isFinite(entryAmount) || entryAmount < 0 || entryAmount > entryMax) return { error: 'invalid_entry_amount' };
  const startAtMs = new Date(msg.startAt).getTime();
  const now = Date.now();
  if (!Number.isFinite(startAtMs) || startAtMs < now + TOURNAMENT_UNOFFICIAL_MIN_LEAD_MS || startAtMs > now + TOURNAMENT_UNOFFICIAL_MAX_LEAD_MS) return { error: 'invalid_start_time' };
  if ((await countActiveHostedEvents(userId)) >= TOURNAMENT_MAX_HOSTED_ACTIVE) return { error: 'too_many_hosted_tournaments' };
  const event = await createTournamentEventRow({
    kind: 'unofficial', name, hostId: userId, bracketSizeCap: maxPlayers,
    entryCurrency, entryAmount, prizePoolPercent, startAt: new Date(startAtMs).toISOString(),
  });
  return { event };
}
async function cancelUnofficialTournament(userId, eventId) {
  const event = await getTournamentEvent(eventId);
  if (!event) return { error: 'tournament_not_found' };
  if (event.hostId !== userId) return { error: 'not_host' };
  if (event.status !== 'scheduled') return { error: 'tournament_already_started' };
  for (const reg of await listRegistrations(eventId)) {
    if (reg.paidAmount > 0) await adjustWallet(reg.userId, reg.paidCurrency === 'gold' ? reg.paidAmount : 0, reg.paidCurrency === 'gems' ? reg.paidAmount : 0);
    await markRegistrationRefunded(eventId, reg.userId);
    notifyUser(reg.userId, { type: 'tournament_refunded', eventId, reason: 'cancelled', profile: await fetchProfile(reg.userId) });
  }
  await setTournamentEventStatus(eventId, 'cancelled');
  return { event };
}

/* -- single-elimination bracket, with byes for non-power-of-2 sizes.
 * "Seed" here just means "slot position", assigned by a random shuffle
 * (see shuffleParticipants), not a skill ranking. It still uses the
 * standard recursive tournament-seeding construction real sports brackets
 * use (so slot 1 and slot 2 can only ever meet in the final, slots 1-4
 * can't meet before the semifinal, etc.), but purely because that
 * construction is what guarantees byes never collide, not for
 * competitive fairness, which isn't a goal here. The load-bearing
 * property for byes: because bracketSize is always the *smallest* power of
 * two >= the real player count n, the number of byes (bracketSize-n) is
 * always strictly less than bracketSize/2, which makes it mathematically
 * impossible for two byes to ever land in the same first-round pair (a
 * bye-vs-bye pair would need two seed numbers x, y with x+y=bracketSize+1
 * and both x,y>n, which requires n<=bracketSize/2, the opposite of what's
 * guaranteed). So a bye can never produce a dead "nobody vs nobody" match,
 * every bye is always real-player-vs-empty-slot and auto-resolves clean. */
function nextPow2(n) { let p = 1; while (p < n) p *= 2; return p; }
function seedOrder(size) {
  let order = [1, 2];
  while (order.length < size) {
    const len = order.length * 2 + 1;
    const next = [];
    for (const s of order) { next.push(s); next.push(len - s); }
    order = next;
  }
  return order;
}
function roundLabel(roundIdx, totalRounds) {
  const fromEnd = totalRounds - roundIdx;
  if (fromEnd === 1) return 'Final';
  if (fromEnd === 2) return 'Semifinal';
  if (fromEnd === 3) return 'Quarterfinal';
  return `Round of ${Math.pow(2, fromEnd)}`;
}
/** participants: array sorted best-seed-first (participants[0] is seed 1).
 * Round 0 gets real players/byes dropped in from the seeding; every later
 * round starts empty and fills in as winners advance. */
function buildInitialRounds(participants) {
  const n = participants.length;
  const bracketSize = nextPow2(n);
  const order = seedOrder(bracketSize);
  const slots = order.map(seedNum => (seedNum <= n ? { ...participants[seedNum - 1] } : null));
  const numRounds = Math.log2(bracketSize);
  const rounds = [];
  const round0 = [];
  for (let i = 0; i < slots.length; i += 2) round0.push({ a: slots[i] || null, b: slots[i + 1] || null, winnerId: null, loserId: null, isBye: false, matchId: null });
  rounds.push(round0);
  let matchesInRound = round0.length;
  for (let r = 1; r < numRounds; r++) {
    matchesInRound = matchesInRound / 2;
    const round = [];
    for (let i = 0; i < matchesInRound; i++) round.push({ a: null, b: null, winnerId: null, loserId: null, isBye: false, matchId: null });
    rounds.push(round);
  }
  return rounds;
}
function participantByUserId(match, userId) {
  if (match.a && match.a.userId === userId) return match.a;
  if (match.b && match.b.userId === userId) return match.b;
  return null;
}
/** Records a decided result and, if there's a next round, advances the
 * winner into it. Returns true if this was the bracket's last match. Pure
 * and synchronous, never touches the DB or network. */
function recordBracketResult(bracket, roundIdx, matchIdx, winnerId, loserId, isBye) {
  const m = bracket.rounds[roundIdx][matchIdx];
  const winner = participantByUserId(m, winnerId) || { userId: winnerId };
  m.winnerId = winnerId; m.loserId = loserId; m.isBye = !!isBye; m.matchId = null;
  const nextRound = bracket.rounds[roundIdx + 1];
  if (!nextRound) return true;
  const nextMatch = nextRound[Math.floor(matchIdx / 2)];
  if (matchIdx % 2 === 0) nextMatch.a = winner; else nextMatch.b = winner;
  return false;
}
/** Auto-resolves every real-player-vs-empty-bye match. Byes only ever
 * exist in round 0, that's the only round whose empty slots come
 * directly from the seeding (a missing seed number), so it's the only
 * round where "one side filled" reliably means "bye" rather than "this
 * match just hasn't been decided by an earlier round yet". Scanning any
 * later round for a lone-filled slot would incorrectly auto-advance a
 * real player before their actual opponent is even decided, round 0 is
 * the whole story, no cascading needed (see the big comment above for why
 * a bye can never chain into producing another bye downstream). */
function resolveByesAndAdvance(bracket) {
  let finished = false;
  const round0 = bracket.rounds[0];
  for (let i = 0; i < round0.length; i++) {
    const m = round0[i];
    if (m.winnerId) continue;
    if (m.a && !m.b) { if (recordBracketResult(bracket, 0, i, m.a.userId, null, true)) finished = true; }
    else if (m.b && !m.a) { if (recordBracketResult(bracket, 0, i, m.b.userId, null, true)) finished = true; }
  }
  return finished;
}

/* -- match integration ----------------------------------------------------- */
function isPlayerAvailable(userId) {
  const c = connections.get(userId);
  return !!c && c.ws && c.ws.readyState === c.ws.OPEN && !activeMatchByUser.has(userId);
}
/** Turns a bracket slot with both players filled into a real live Match
 * (reusing the Match class real duels use, unmodified), UNLESS one or
 * both players have gone AWOL since their previous round, in which case
 * it's an instant walkover: the same "didn't show up = disqualified" rule
 * applied every round, not just at the very start of the event. */
async function startTournamentBracketMatch(bracket, roundIdx, matchIdx) {
  const m = bracket.rounds[roundIdx][matchIdx];
  const uA = m.a.userId, uB = m.b.userId;
  const aOnline = isPlayerAvailable(uA), bOnline = isPlayerAvailable(uB);
  if (!aOnline || !bOnline) {
    let winnerId, loserId;
    if (!aOnline && !bOnline) { // both AWOL, deterministic walkover (lower slot number) so this can never be gamed, not a skill judgment
      const aSeed = m.a.seed ?? 999, bSeed = m.b.seed ?? 999;
      winnerId = aSeed <= bSeed ? uA : uB; loserId = winnerId === uA ? uB : uA;
    } else if (!aOnline) { winnerId = uB; loserId = uA; }
    else { winnerId = uA; loserId = uB; }
    await finishBracketMatch(bracket, roundIdx, matchIdx, winnerId, loserId, false);
    return;
  }
  try {
    const [profileA, profileB] = await Promise.all([fetchProfile(uA), fetchProfile(uB)]);
    const deckA = Engine.buildDeckFromIds(Engine.isDeckLegal(profileA.deck) ? profileA.deck : null);
    const deckB = Engine.buildDeckFromIds(Engine.isDeckLegal(profileB.deck) ? profileB.deck : null);
    const match = new Match(uA, uB, deckA, deckB);
    match.usernames = [profileA.username, profileB.username];
    match.icons = [profileA.icon || 'star', profileB.icon || 'star'];
    match.ranks = [profileA.rank, profileB.rank];
    match.tournamentMeta = { bracketId: bracket.id, eventId: bracket.eventId, roundIdx, matchIdx };
    m.matchId = match.id;
    await saveBracket(bracket);
    const roundInfo = { bracketId: bracket.id, eventName: bracket.eventName, roundIndex: roundIdx, totalRounds: bracket.rounds.length, roundLabel: roundLabel(roundIdx, bracket.rounds.length) };
    connections.get(uA)?.send({ type: 'match_found', matchId: match.id, youAre: 0, opponentName: profileB.username, opponentIcon: profileB.icon || 'star', opponentRank: profileB.rank, tournament: roundInfo });
    connections.get(uB)?.send({ type: 'match_found', matchId: match.id, youAre: 1, opponentName: profileA.username, opponentIcon: profileA.icon || 'star', opponentRank: profileA.rank, tournament: roundInfo });
    match.broadcastState([]);
  } catch (e) {
    console.error('[arena] tournament match start failed', e);
    // don't strand the bracket, matchId was never set, so the next sweep's startReadyMatches() pass retries this slot
  }
}
/** Scans every round for a match that's ready to play (both slots filled,
 * no winner, not already running) and starts it. Called right after a
 * bracket is created and again every time a match finishes. */
async function startReadyMatches(bracket) {
  for (let r = 0; r < bracket.rounds.length; r++) {
    for (let i = 0; i < bracket.rounds[r].length; i++) {
      const m = bracket.rounds[r][i];
      if (m.a && m.b && !m.winnerId && !m.matchId) await startTournamentBracketMatch(bracket, r, i);
    }
  }
}
/** Shared tail-end for "a bracket match just got decided", whether that
 * decision came from a real completed Match, a bye, or a walkover. Pays
 * the champion the instant the final match resolves. */
async function finishBracketMatch(bracket, roundIdx, matchIdx, winnerId, loserId, isBye) {
  const finishedHere = recordBracketResult(bracket, roundIdx, matchIdx, winnerId, loserId, isBye);
  if (finishedHere) bracket.winnerId = winnerId;
  const finishedByCascade = resolveByesAndAdvance(bracket);
  if ((finishedHere || finishedByCascade) && bracket.status !== 'completed') {
    bracket.status = 'completed';
    if (!bracket.winnerId) bracket.winnerId = winnerId;
    bracket.completedAt = new Date().toISOString();
    if (bracket.winnerPayout > 0) await adjustWallet(bracket.winnerId, bracket.prizeCurrency === 'gold' ? bracket.winnerPayout : 0, bracket.prizeCurrency === 'gems' ? bracket.winnerPayout : 0);
    // Pay the host their cut of an unofficial tournament's pool (everything the winner didn't take).
    if (bracket.hostId && bracket.hostPayout > 0) {
      await adjustWallet(bracket.hostId, bracket.prizeCurrency === 'gold' ? bracket.hostPayout : 0, bracket.prizeCurrency === 'gems' ? bracket.hostPayout : 0);
      notifyUser(bracket.hostId, { type: 'tournament_host_payout', bracket: bracketPayload(bracket), prize: { currency: bracket.prizeCurrency, amount: bracket.hostPayout }, profile: await fetchProfile(bracket.hostId) });
    }
    await saveBracket(bracket);
    await pushBracketUpdate(bracket);
    notifyUser(bracket.winnerId, { type: 'tournament_won', bracket: bracketPayload(bracket), prize: { currency: bracket.prizeCurrency, amount: bracket.winnerPayout }, profile: await fetchProfile(bracket.winnerId) });
    await maybeCompleteEvent(bracket.eventId);
    return;
  }
  await saveBracket(bracket);
  await pushBracketUpdate(bracket);
  await startReadyMatches(bracket);
}
async function maybeCompleteEvent(eventId) {
  const event = await getTournamentEvent(eventId);
  if (!event || event.status === 'completed' || event.status === 'cancelled') return;
  const brackets = await listBracketsForEvent(eventId);
  if (brackets.length > 0 && brackets.every(b => b.status === 'completed')) await setTournamentEventStatus(eventId, 'completed');
}
/** Called from Match#finish() whenever a tournament-tagged match concludes
 * for real. Returns a small summary the caller merges into the match_over
 * payload so the in-battle UI can say e.g. "Advancing to the Final!"
 * without a separate round-trip. */
async function onTournamentMatchFinished(match, winnerId, loserId) {
  const { bracketId, roundIdx, matchIdx } = match.tournamentMeta;
  const bracket = await getBracket(bracketId);
  if (!bracket) return null;
  const totalRounds = bracket.rounds.length;
  await finishBracketMatch(bracket, roundIdx, matchIdx, winnerId, loserId, false);
  const tournamentComplete = bracket.status === 'completed';
  return {
    bracketId: bracket.id, eventName: bracket.eventName, tournamentComplete,
    championId: tournamentComplete ? bracket.winnerId : null,
    nextRoundLabel: tournamentComplete ? null : roundLabel(roundIdx + 1, totalRounds),
  };
}
/** A draw can't be allowed to leave a bracket slot undecided, instead of
 * the normal no-reward draw teardown, immediately start a fresh Match for
 * the same slot. Called from Match#finishDraw(). */
async function rematchTournamentMatch(match) {
  const { bracketId, roundIdx, matchIdx } = match.tournamentMeta;
  const bracket = await getBracket(bracketId);
  if (!bracket) return;
  bracket.rounds[roundIdx][matchIdx].matchId = null;
  await saveBracket(bracket);
  await startTournamentBracketMatch(bracket, roundIdx, matchIdx);
}

/* -- locking + sharding at start time --------------------------------------- */
/** Plain Fisher-Yates shuffle, brackets are seeded fully at random, not by
 * rank. Single elimination needs exactly n-1 matches to produce a champion
 * from n players no matter how those n are grouped or ordered, so there's
 * no match-count cost to this; it's purely "who plays who" that's random. */
function shuffleParticipants(list) {
  for (let i = list.length - 1; i > 0; i--) { const j = Math.floor(Math.random() * (i + 1)); [list[i], list[j]] = [list[j], list[i]]; }
  return list;
}
/** Chops an already-shuffled list into groups of at most `bracketSizeCap`
 *, the "as many brackets as needed" part for an official event that
 * draws more than one bracket's worth of players. No attempt to balance
 * who ends up in which group beyond the initial shuffle. */
function shardParticipants(shuffled, bracketSizeCap) {
  const shards = [];
  for (let i = 0; i < shuffled.length; i += bracketSizeCap) shards.push(shuffled.slice(i, i + bracketSizeCap));
  return shards.map(shard => shard.map((p, i) => ({ ...p, seed: i + 1 })));
}
let tournamentSweepBusy = false;
async function lockAndShardEvent(event) {
  const current = await getTournamentEvent(event.id); // re-check right before locking, cheap compare-and-set against a double-processed event
  if (!current || current.status !== 'scheduled') return;
  await setTournamentEventStatus(event.id, 'locked');

  const regs = (await listRegistrations(event.id)).filter(r => !r.refunded);
  const checkedIn = [], noShows = [];
  for (const reg of regs) (isPlayerAvailable(reg.userId) ? checkedIn : noShows).push(reg);
  await Promise.all(checkedIn.map(r => setRegistrationCheckedIn(event.id, r.userId, true)));
  await Promise.all(noShows.map(r => setRegistrationCheckedIn(event.id, r.userId, false)));
  // no-shows forfeit their entry fee, never refunded, same convention as any forfeited deposit
  noShows.forEach(r => notifyUser(r.userId, { type: 'tournament_disqualified', eventId: event.id, reason: 'no_show' }));

  if (checkedIn.length < TOURNAMENT_MIN_PLAYERS_TO_RUN) {
    for (const reg of checkedIn) {
      if (reg.paidAmount > 0) await adjustWallet(reg.userId, reg.paidCurrency === 'gold' ? reg.paidAmount : 0, reg.paidCurrency === 'gems' ? reg.paidAmount : 0);
      await markRegistrationRefunded(event.id, reg.userId);
      notifyUser(reg.userId, { type: 'tournament_refunded', eventId: event.id, reason: 'not_enough_players', profile: await fetchProfile(reg.userId) });
    }
    await setTournamentEventStatus(event.id, 'cancelled');
    return;
  }

  const summaries = await fetchProfileSummaries(checkedIn.map(r => r.userId));
  const shuffled = shuffleParticipants(checkedIn.map(r => ({
    userId: r.userId,
    username: summaries.get(r.userId)?.username || 'Player',
    icon: summaries.get(r.userId)?.icon || 'star',
  })));

  for (const shard of shardParticipants(shuffled, event.bracketSizeCap)) {
    const prizePool = shard.length * event.entryAmount;
    const winnerPayout = Math.floor(prizePool * event.prizePoolPercent / 100);
    // Official tournaments burn the remainder as a currency sink (see
    // TOURNAMENT_OFFICIAL_PRIZE_PERCENT). Unofficial (player-hosted) events
    // instead route the remainder to the host, they picked the prize % and
    // put the tournament together, so anything not paid to the winner is
    // their compensation for hosting, not a waste.
    const isUnofficial = event.kind === 'unofficial' && event.hostId;
    const hostPayout = isUnofficial ? (prizePool - winnerPayout) : 0;
    const bracket = {
      id: crypto.randomUUID(), eventId: event.id, eventName: event.name, eventKind: event.kind,
      prizeCurrency: event.entryCurrency, prizePool, winnerPayout,
      hostId: isUnofficial ? event.hostId : null, hostPayout,
      status: 'in_progress', winnerId: null,
      participants: shard, rounds: buildInitialRounds(shard),
      createdAt: new Date().toISOString(), completedAt: null,
    };
    resolveByesAndAdvance(bracket);
    await createBracketRow(bracket);
    for (const p of shard) notifyUser(p.userId, { type: 'tournament_bracket_assigned', bracket: bracketPayload(bracket) });
    await startReadyMatches(bracket);
  }
  await setTournamentEventStatus(event.id, 'running');
}
async function tournamentSweep() {
  if (tournamentSweepBusy) return; // single-process guard, avoid two overlapping sweeps double-processing the same event
  tournamentSweepBusy = true;
  try {
    await ensureUpcomingOfficialEvents();
    const now = Date.now();
    for (const event of await eventsByStatus(['scheduled'])) {
      if (new Date(event.startAt).getTime() <= now) await lockAndShardEvent(event);
    }
  } catch (e) { console.error('[arena] tournament sweep failed', e); }
  finally { tournamentSweepBusy = false; }
}

/* -- client-facing list/detail payloads ------------------------------------- */
async function tournamentEventPayload(event, userId, summaries) {
  const registeredCount = await countRegistrations(event.id);
  const myReg = userId ? await getRegistration(event.id, userId) : null;
  let myBracketId = null;
  if (myReg && event.status !== 'scheduled') {
    const bracket = await findParticipantBracket(event.id, userId);
    myBracketId = bracket ? bracket.id : null;
  }
  return {
    id: event.id, kind: event.kind, name: event.name,
    hostId: event.hostId, hostName: event.hostId ? (summaries?.get(event.hostId)?.username || 'Player') : null,
    bracketSizeCap: event.bracketSizeCap, entryCurrency: event.entryCurrency, entryAmount: event.entryAmount,
    prizePoolPercent: event.prizePoolPercent, startAt: event.startAt, status: event.status,
    registeredCount, youRegistered: !!myReg, youCheckedIn: myReg ? myReg.checkedIn : null, myBracketId,
  };
}
async function buildTournamentListPayload(userId) {
  // Official tab shows only the single currently-joinable event of each kind
  // (ensureUpcomingOfficialEvents guarantees exactly one 'scheduled' event
  // per kind exists at a time). A previous daily/weekly that's already
  // locked/running is intentionally NOT included here, showing it
  // alongside the new joinable one made it look like two daily/weekly
  // tournaments were open at once. Anyone registered in that in-progress
  // one can still find it under "My Tournaments".
  const [officialDaily, officialWeekly, unofficialOpen, mineIds] = await Promise.all([
    eventsByStatus(['scheduled'], { kinds: ['official_daily'], limit: 1 }),
    eventsByStatus(['scheduled'], { kinds: ['official_weekly'], limit: 1 }),
    eventsByStatus(['scheduled'], { kinds: ['unofficial'], limit: TOURNAMENT_LIST_LIMIT }),
    listUserRegisteredEventIds(userId, TOURNAMENT_MINE_LIMIT),
  ]);
  const mineEvents = (await Promise.all(mineIds.map(id => getTournamentEvent(id)))).filter(Boolean);
  const hostIds = [...new Set([...unofficialOpen, ...mineEvents].map(e => e.hostId).filter(Boolean))];
  const summaries = hostIds.length ? await fetchProfileSummaries(hostIds) : new Map();
  return {
    officialDaily: await Promise.all(officialDaily.map(e => tournamentEventPayload(e, userId, summaries))),
    officialWeekly: await Promise.all(officialWeekly.map(e => tournamentEventPayload(e, userId, summaries))),
    unofficial: await Promise.all(unofficialOpen.map(e => tournamentEventPayload(e, userId, summaries))),
    mine: await Promise.all(mineEvents.map(e => tournamentEventPayload(e, userId, summaries))),
  };
}

/* ── TRADING ──────────────────────────────────────────────────────
 * A trade is a live negotiation between two connected players: each side
 * builds an "offer" (some cards + gold + gems taken from their own
 * collection/wallet), both sides must explicitly mark themselves ready,
 * and then both sides must explicitly *confirm*, matching the client's
 * "are you sure?" prompt, before anything is actually moved. Every offer
 * is re-validated server-side against a fresh profile snapshot both when
 * it's submitted and again right before the swap executes, so a stale
 * client (or a spent-in-between-messages race, like buying a pack mid
 * trade) can never move cards/currency the player doesn't actually have. */

/** {cardId: quantity} tally of a flat collection array (which stores one
 * entry per copy owned, same shape fetchProfile always returns). */
function collectionCounts(collection) {
  const out = {};
  for (const id of collection || []) out[id] = (out[id] || 0) + 1;
  return out;
}

/** Clamps a client-submitted offer down to what's actually legal: only
 * owned card ids, only positive integer quantities no greater than what's
 * owned, and gold/gems clamped to [0, balance]. Never trusts the client's
 * numbers directly. */
function sanitizeTradeOffer(raw, ownedCounts, gold, gems) {
  const cards = {};
  if (raw && typeof raw.cards === 'object' && raw.cards) {
    for (const [cardId, qtyRaw] of Object.entries(raw.cards)) {
      const qty = Math.floor(Number(qtyRaw));
      const owned = ownedCounts[cardId] || 0;
      if (!Number.isFinite(qty) || qty <= 0 || owned <= 0) continue;
      cards[cardId] = Math.min(qty, owned);
    }
  }
  let goldOffer = Math.floor(Number(raw && raw.gold));
  let gemsOffer = Math.floor(Number(raw && raw.gems));
  if (!Number.isFinite(goldOffer) || goldOffer < 0) goldOffer = 0;
  if (!Number.isFinite(gemsOffer) || gemsOffer < 0) gemsOffer = 0;
  return { cards, gold: Math.min(goldOffer, gold), gems: Math.min(gemsOffer, gems) };
}

/** Final, authoritative check right before cards/currency actually move,
 * re-checks against a *fresh* profile fetch, not whatever was true when the
 * offer was last submitted. */
function tradeOfferIsValid(offer, profile) {
  const counts = collectionCounts(profile.collection);
  for (const [cardId, qty] of Object.entries(offer.cards || {})) {
    if (!Number.isInteger(qty) || qty <= 0) return false;
    if (qty > (counts[cardId] || 0)) return false;
  }
  if (!Number.isInteger(offer.gold) || offer.gold < 0 || offer.gold > profile.gold) return false;
  if (!Number.isInteger(offer.gems) || offer.gems < 0 || offer.gems > profile.gems) return false;
  return true;
}

function tradeStatePayload(session) {
  return { type: 'trade_state', tradeId: session.id, users: session.users,
    offers: session.offers, ready: session.ready, confirmed: session.confirmed };
}
function broadcastTradeState(session) {
  const payload = tradeStatePayload(session);
  for (const uid of session.users) connections.get(uid)?.send(payload);
}
/** Any offer change invalidates both sides' ready/confirm state, same
 * "if terms change, everyone has to re-agree" rule real trade UIs use. */
function resetTradeProgress(session) {
  for (const uid of session.users) { session.ready[uid] = false; session.confirmed[uid] = false; }
}
function endTradeSession(session) {
  tradeSessions.delete(session.id);
  for (const uid of session.users) if (activeTradeByUser.get(uid) === session) activeTradeByUser.delete(uid);
}
function cancelTrade(session, byUserId, reason = 'cancelled') {
  endTradeSession(session);
  for (const uid of session.users) connections.get(uid)?.send({ type: 'trade_cancelled', tradeId: session.id, byUserId, reason });
}

/** Starts a live trade session between two already-agreed players, same
 * request/response shape as startDuelMatch, just opening a negotiation
 * instead of a battle. */
async function startTradeSession(uA, uB) {
  const connA = connections.get(uA), connB = connections.get(uB);
  try {
    const [profileA, profileB] = await Promise.all([fetchProfile(uA), fetchProfile(uB)]);
    const session = {
      id: crypto.randomUUID(),
      users: [uA, uB],
      offers: { [uA]: { cards: {}, gold: 0, gems: 0 }, [uB]: { cards: {}, gold: 0, gems: 0 } },
      ready: { [uA]: false, [uB]: false },
      confirmed: { [uA]: false, [uB]: false },
    };
    tradeSessions.set(session.id, session);
    activeTradeByUser.set(uA, session); activeTradeByUser.set(uB, session);
    connA?.send({ type: 'trade_started', tradeId: session.id,
      opponent: { userId: uB, username: profileB.username, icon: profileB.icon || 'star' },
      yourCollection: collectionCounts(profileA.collection), yourGold: profileA.gold, yourGems: profileA.gems });
    connB?.send({ type: 'trade_started', tradeId: session.id,
      opponent: { userId: uA, username: profileA.username, icon: profileA.icon || 'star' },
      yourCollection: collectionCounts(profileB.collection), yourGold: profileB.gold, yourGems: profileB.gems });
    broadcastTradeState(session);
  } catch (e) {
    console.error('[arena] trade session failed', e);
    connA?.send({ type: 'error', reason: 'trade_start_failed' });
    connB?.send({ type: 'error', reason: 'trade_start_failed' });
  }
}

/** +delta gives copies to userId, -delta removes them, used for both
 * sides of a trade swap. Guest mode mutates the in-memory flat array;
 * Supabase mode updates one key inside the player's single jsonb cards row. */
async function adjustCardQuantity(userId, cardId, delta) {
  if (!delta) return;
  if (!HAS_SUPABASE) {
    const p = guestProfiles.get(userId); if (!p) return;
    if (delta > 0) { for (let i = 0; i < delta; i++) p.collection.push(cardId); }
    else {
      let n = -delta;
      for (let i = p.collection.length - 1; i >= 0 && n > 0; i--) {
        if (p.collection[i] === cardId) { p.collection.splice(i, 1); n--; }
      }
    }
    return;
  }
  const { data: existing } = await supabase.from('player_cards').select('cards').eq('owner_id', userId).maybeSingle();
  const cards = { ...(existing?.cards || {}) };
  const newQty = (cards[cardId] || 0) + delta;
  if (newQty <= 0) delete cards[cardId]; else cards[cardId] = newQty;
  await supabase.from('player_cards').upsert({ owner_id: userId, cards }, { onConflict: 'owner_id' });
}

async function adjustWallet(userId, goldDelta, gemsDelta) {
  if (!goldDelta && !gemsDelta) return;
  if (!HAS_SUPABASE) {
    const p = guestProfiles.get(userId); if (p) { p.gold += goldDelta; p.gems += gemsDelta; }
    return;
  }
  const { data } = await supabase.from('profiles').select('gold,gems').eq('id', userId).maybeSingle();
  if (!data) return;
  await supabase.from('profiles').update({ gold: data.gold + goldDelta, gems: data.gems + gemsDelta }).eq('id', userId);
}

/** The actual swap, only ever called once both sides have confirmed.
 * Re-validates both offers against fresh profiles first (defends against
 * e.g. spending gold on a pack mid-negotiation), and throws rather than
 * moving anything if either side no longer checks out. */
async function executeTrade(session) {
  const [uA, uB] = session.users;
  const [profileA, profileB] = await Promise.all([fetchProfile(uA), fetchProfile(uB)]);
  const offerA = session.offers[uA], offerB = session.offers[uB];
  if (!tradeOfferIsValid(offerA, profileA) || !tradeOfferIsValid(offerB, profileB)) {
    const e = new Error('trade_invalid'); e.code = 'trade_invalid'; throw e;
  }
  for (const [cardId, qty] of Object.entries(offerA.cards)) { await adjustCardQuantity(uA, cardId, -qty); await adjustCardQuantity(uB, cardId, qty); }
  for (const [cardId, qty] of Object.entries(offerB.cards)) { await adjustCardQuantity(uB, cardId, -qty); await adjustCardQuantity(uA, cardId, qty); }
  await adjustWallet(uA, offerB.gold - offerA.gold, offerB.gems - offerA.gems);
  await adjustWallet(uB, offerA.gold - offerB.gold, offerA.gems - offerB.gems);
  if (HAS_SUPABASE) {
    try {
      await supabase.from('trade_history').insert({
        player_a: uA, player_b: uB,
        offer_a: { cards: offerA.cards, gold: offerA.gold, gems: offerA.gems },
        offer_b: { cards: offerB.cards, gold: offerB.gold, gems: offerB.gems },
      });
    } catch (e) { /* history logging is best-effort, never blocks the trade itself */ }
  }
}

async function tryMatch(mode) {
  const q = queueForMode(mode);
  const ranked = mode !== 'casual';
  while (q.length >= 2) {
    const uA = q.shift(), uB = q.shift();
    queueMode.delete(uA); queueMode.delete(uB);
    clearQueueTimer(uA); clearQueueTimer(uB);
    const connA = connections.get(uA), connB = connections.get(uB);
    if (!connA || connA.ws.readyState !== connA.ws.OPEN) { if (connB) { q.unshift(uB); queueMode.set(uB, mode); armBotFallback(uB, mode); } continue; }
    if (!connB || connB.ws.readyState !== connB.ws.OPEN) { q.unshift(uA); queueMode.set(uA, mode); armBotFallback(uA, mode); continue; }
    try {
      const [profileA, profileB] = await Promise.all([fetchProfile(uA), fetchProfile(uB)]);
      // Same re-check as startBotMatch: deck legality was already gated at queue_join,
      // this just covers a deck edited out from under a still-queued player. Whoever's
      // deck is invalid gets blocked and told why; whoever's deck is fine goes back to
      // the front of the queue instead of losing their spot.
      const legalA = Engine.isDeckLegal(profileA.deck), legalB = Engine.isDeckLegal(profileB.deck);
      if (!legalA || !legalB) {
        if (!legalA) connA.send({ type:'error', reason:'invalid_deck', deckSize:(profileA.deck||[]).length, requiredSize:Engine.DECK_SIZE });
        else { q.unshift(uA); queueMode.set(uA, mode); armBotFallback(uA, mode); }
        if (!legalB) connB.send({ type:'error', reason:'invalid_deck', deckSize:(profileB.deck||[]).length, requiredSize:Engine.DECK_SIZE });
        else { q.unshift(uB); queueMode.set(uB, mode); armBotFallback(uB, mode); }
        continue;
      }
      const deckA = Engine.buildDeckFromIds(profileA.deck);
      const deckB = Engine.buildDeckFromIds(profileB.deck);
      const match = new Match(uA, uB, deckA, deckB, ranked);
      match.usernames = [profileA.username, profileB.username];
      match.icons = [profileA.icon || 'star', profileB.icon || 'star'];
      match.ranks = [profileA.rank, profileB.rank];
      connA.send({ type:'match_found', matchId: match.id, youAre: 0, opponentName: profileB.username, opponentIcon: profileB.icon || 'star', opponentRank: profileB.rank, ranked });
      connB.send({ type:'match_found', matchId: match.id, youAre: 1, opponentName: profileA.username, opponentIcon: profileA.icon || 'star', opponentRank: profileA.rank, ranked });
      match.broadcastState([]);
    } catch (e) {
      console.error('[arena] matchmaking failed', e);
      connA?.send({ type:'error', reason:'matchmaking_failed' });
      connB?.send({ type:'error', reason:'matchmaking_failed' });
    }
  }
}

/* ── WS SERVER ────────────────────────────────────────────────────── */
const server = http.createServer((req, res) => {
  if (req.url === '/health') { res.writeHead(200, {'content-type':'application/json'}); res.end(JSON.stringify({ ok:true, matches: matches.size, queue: rankedQueue.length + casualQueue.length, rankedQueue: rankedQueue.length, casualQueue: casualQueue.length })); return; }
  if (req.url === '/cards.hash') {
    // Tiny endpoint for the "have I already got this?" check, a client with
    // a cached copy in localStorage hits this instead of re-downloading the
    // whole library on every load. Full body only comes down from /cards.json
    // when this hash doesn't match what's cached.
    res.writeHead(200, {
      'content-type': 'application/json',
      'access-control-allow-origin': '*',
      'cache-control': 'no-cache',
    });
    res.end(JSON.stringify({ hash: Engine.CARD_LIBRARY_HASH }));
    return;
  }
  if (req.url === '/cards.json') {
    // The single canonical card library, the client fetches this instead of keeping
    // its own hardcoded copy, so there's only ever one place "god card" stats could live.
    res.writeHead(200, {
      'content-type': 'application/json',
      'access-control-allow-origin': '*',
      'cache-control': 'no-cache',
      'etag': Engine.CARD_LIBRARY_HASH,
    });
    res.end(Engine.CARD_LIBRARY_RAW);
    return;
  }
  res.writeHead(404); res.end();
});
const wss = new WebSocketServer({ server, perMessageDeflate: false });

wss.on('connection', (ws) => {
  const conn = new Connection(ws);

  ws.on('message', async (raw) => {
    let msg; try { msg = JSON.parse(raw); } catch { return; }
    if (!msg || typeof msg.type !== 'string') return;

    // ── auth must come first ──
    if (msg.type === 'auth') {
      try {
        // Every client must be running the exact same card library we are, this is the
        // one gate that keeps a modified/forked client from ever getting to matchmake or
        // play with buffed "god card" stats: if the hash of its cards.json doesn't match
        // ours byte-for-byte, it never gets far enough to send a deploy/attack at all.
        if (msg.cardLibraryHash !== Engine.CARD_LIBRARY_HASH) {
          return conn.send({ type:'error', reason:'card_library_mismatch', expectedHash: Engine.CARD_LIBRARY_HASH });
        }
        let userId, username;
        if (HAS_SUPABASE && msg.token) {
          const { data, error } = await supabase.auth.getUser(msg.token);
          if (error || !data?.user) return conn.send({ type:'error', reason:'bad_token' });
          userId = data.user.id;
          username = data.user.user_metadata?.username || data.user.email || `Player${userId.slice(0,6)}`;
        } else {
          // guest path, stable id per socket session, not persisted server-restart
          userId = msg.guestId && typeof msg.guestId === 'string' ? msg.guestId : crypto.randomUUID();
          username = (msg.name || 'Guest').slice(0, 24);
        }
        // if this user already has a live connection (dupe tab), boot the old one
        const existing = connections.get(userId);
        if (existing && existing.ws !== ws) existing.ws.close(4000, 'replaced');
        conn.userId = userId; conn.username = username;
        conn.cardLibraryHash = msg.cardLibraryHash;
        connections.set(userId, conn);

        const inMatch = activeMatchByUser.get(userId);
        if (inMatch) { inMatch.handleReconnect(userId); }

        const profile = await fetchProfile(userId, username);
        conn.icon = profile.icon || 'star';
        // Threshold quests that depend on "what you currently have" (not a
        // discrete event) need a checkpoint even if nothing changed this
        // session, a returning player who already owns 60 unique cards
        // should see Collector etc. reflect that on login, not only the
        // next time a pack/match/sale nudges it. Gold isn't in this group
        // any more (Rich Guy / Golden Guy track actual earnings now, see
        // QUEST_TRACK.GOLD_EARNED, not standing balance), so there's
        // nothing gold-related to checkpoint here.
        computeCollectionQuestThresholds(userId).catch(e => console.error('[arena] login collection quest check failed', e));
        if (profile.rankPoints) recordQuestThreshold(userId, Engine.QUEST_TRACK.RANK_POINTS, profile.rankPoints).catch(e => console.error('[arena] login rank quest check failed', e));
        conn.send({ type:'auth_ok', userId, profile, profileOptions: { icons: PROFILE_ICONS, banners: PROFILE_BANNERS, bioMax: BIO_MAX, usernameMax: USERNAME_MAX, favoritesMax: FAVORITES_MAX }, guildOptions: { icons: GUILD_ICONS, frames: GUILD_FRAMES, nameMin: GUILD_NAME_MIN, nameMax: GUILD_NAME_MAX, maxMembers: GUILD_MAX_MEMBERS, createCostGems: GUILD_CREATE_COST_GEMS, joinFeeMaxGold: GUILD_JOIN_FEE_MAX_GOLD, joinFeeMaxGems: GUILD_JOIN_FEE_MAX_GEMS, chatMessageMax: GUILD_CHAT_MESSAGE_MAX, chatRetentionDays: GUILD_CHAT_RETENTION_MS / (24*60*60*1000) }, tournamentOptions: { bracketSize: TOURNAMENT_BRACKET_SIZE, officialPrizePercent: TOURNAMENT_OFFICIAL_PRIZE_PERCENT, dailyEntryGold: TOURNAMENT_DAILY_ENTRY_GOLD, weeklyEntryGems: TOURNAMENT_WEEKLY_ENTRY_GEMS, unofficialMinPlayers: TOURNAMENT_UNOFFICIAL_MIN_PLAYERS, unofficialMaxPlayers: TOURNAMENT_UNOFFICIAL_MAX_PLAYERS, unofficialPrizePercentMin: TOURNAMENT_UNOFFICIAL_PRIZE_PERCENT_MIN, unofficialPrizePercentMax: TOURNAMENT_UNOFFICIAL_PRIZE_PERCENT_MAX, unofficialEntryMaxGold: TOURNAMENT_UNOFFICIAL_ENTRY_MAX_GOLD, unofficialEntryMaxGems: TOURNAMENT_UNOFFICIAL_ENTRY_MAX_GEMS, unofficialMinLeadMs: TOURNAMENT_UNOFFICIAL_MIN_LEAD_MS, unofficialMaxLeadMs: TOURNAMENT_UNOFFICIAL_MAX_LEAD_MS, nameMin: TOURNAMENT_NAME_MIN, nameMax: TOURNAMENT_NAME_MAX }, inMatchUserIds: [...activeMatchByUser.keys()] });
      } catch (e) {
        console.error('[arena] auth failed', e);
        conn.send({ type:'error', reason:'auth_failed' });
      }
      return;
    }

    if (!conn.userId) return conn.send({ type:'error', reason:'not_authenticated' });
    const userId = conn.userId;

    switch (msg.type) {
      case 'queue_join': {
        if (activeMatchByUser.has(userId)) return conn.send({ type:'error', reason:'already_in_match' });
        // Re-check now, not just at auth: covers a server-side cards.json hot-reload that
        // happened mid-session, and applies identically whether this queue_join ends up
        // pairing with a real opponent or falling back to a bot, same gate, same code path.
        if (conn.cardLibraryHash !== Engine.CARD_LIBRARY_HASH) {
          return conn.send({ type:'error', reason:'card_library_mismatch', expectedHash: Engine.CARD_LIBRARY_HASH });
        }
        // A deck must be exactly DECK_SIZE legal cards to queue at all, no more handing
        // out a random deck as a silent stand-in. Block here with a clear reason so the
        // client can point the player at the deck builder instead of starting a match
        // they never actually built.
        let joinProfile;
        try { joinProfile = await fetchProfile(userId, conn.username); }
        catch (e) { console.error('[arena] queue_join profile fetch failed', e); return conn.send({ type:'error', reason:'matchmaking_failed' }); }
        if (!Engine.isDeckLegal(joinProfile.deck)) {
          return conn.send({ type:'error', reason:'invalid_deck', deckSize: (joinProfile.deck || []).length, requiredSize: Engine.DECK_SIZE });
        }
        const mode = msg.mode === 'casual' ? 'casual' : 'ranked';
        removeFromQueues(userId); // in case they were sitting in the other mode's queue
        const q = queueForMode(mode);
        if (!q.includes(userId)) q.push(userId);
        queueMode.set(userId, mode);
        armBotFallback(userId, mode);
        conn.send({ type:'queue_status', inQueue:true, mode });
        tryMatch(mode);
        break;
      }
      case 'queue_leave': {
        removeFromQueues(userId);
        clearQueueTimer(userId);
        conn.send({ type:'queue_status', inQueue:false });
        break;
      }
      case 'deploy': {
        activeMatchByUser.get(userId)?.handleDeploy(userId, msg);
        break;
      }
      case 'ready_battle': {
        activeMatchByUser.get(userId)?.handleReady(userId);
        break;
      }
      case 'attack': {
        activeMatchByUser.get(userId)?.handleAttack(userId, msg);
        break;
      }
      case 'use_ability': {
        activeMatchByUser.get(userId)?.handleUseAbility(userId, msg);
        break;
      }
      case 'end_turn': {
        activeMatchByUser.get(userId)?.handleEndTurn(userId);
        break;
      }
      case 'forfeit': {
        activeMatchByUser.get(userId)?.handleForfeit(userId);
        break;
      }
      case 'battle_chat': {
        activeMatchByUser.get(userId)?.handleChat(userId, msg.text);
        break;
      }
      case 'get_profile': {
        try { conn.send({ type:'profile', profile: await fetchProfile(userId, conn.username) }); }
        catch (e) { conn.send({ type:'error', reason:'profile_fetch_failed' }); }
        break;
      }
      case 'view_profile': {
        // Read-only lookup of any player's profile (self or an opponent/
        // friend), strips wallet balances and the full collection/deck,
        // since only the requesting player's own client should ever see
        // those for themselves via `get_profile`/`auth_ok`.
        try {
          const targetId = typeof msg.userId === 'string' && msg.userId ? msg.userId : userId;
          const target = await fetchProfile(targetId, targetId === userId ? conn.username : undefined);
          const { gold, gems, deck, ...publicFields } = target; // wallet + active deck stay private
          let friendship = null;
          if (targetId !== userId && !isBotId(targetId)) {
            const rel = await getFriendship(userId, targetId);
            friendship = !rel ? 'none' : rel.status === 'accepted' ? 'friends' : (rel.requestedBy === userId ? 'outgoing' : 'incoming');
          }
          conn.send({ type:'player_profile', profile: publicFields, friendship, inMatch: activeMatchByUser.has(targetId) });
        } catch (e) {
          conn.send({ type:'error', reason:'profile_fetch_failed' });
        }
        break;
      }
      case 'update_profile': {
        try {
          const profile = await fetchProfile(userId, conn.username);
          const owned = new Set(profile.collection);
          const updated = await updateProfile(userId, msg, owned);
          if (updated.username) conn.username = updated.username;
          if (updated.icon) conn.icon = updated.icon;
          conn.send({ type:'profile_updated', profile: updated });
        } catch (e) {
          console.error('[arena] update_profile failed', e);
          conn.send({ type:'error', reason: e.code || 'update_profile_failed' });
        }
        break;
      }
      case 'save_deck': {
        try {
          const profile = await fetchProfile(userId, conn.username);
          const owned = new Set(profile.collection);
          const saved = await saveDeck(userId, msg.cardIds, owned);
          conn.send({ type:'deck_saved', cardIds: saved });
        } catch (e) { conn.send({ type:'error', reason: e.code || 'save_deck_failed', deckSize: e.deckSize, requiredSize: e.requiredSize }); }
        break;
      }
      case 'buy_pack': {
        try {
          const result = await grantPack(userId, msg.packId);
          conn.send({ type:'pack_result', packId: msg.packId, cards: result.cards.map(c => ({ id:c.id, name:c.name, rarity:c.rarity, image:c.image })), currency: result.currency, newBalance: result.newBalance });
        } catch (e) {
          conn.send({ type:'error', reason: e.code || 'buy_pack_failed' });
        }
        break;
      }
      case 'quest_state': {
        try { conn.send({ type:'quest_state', ...(await buildQuestState(userId)) }); }
        catch (e) { console.error('[arena] quest_state failed', e); conn.send({ type:'error', reason: 'quest_state_failed' }); }
        break;
      }
      case 'claim_quest': {
        try {
          const result = await claimQuestReward(userId, msg.questId);
          conn.send({ type:'quest_state', ...(await buildQuestState(userId)) });
          if (!result.ok) conn.send({ type:'error', reason: result.reason || 'claim_failed' });
        } catch (e) { console.error('[arena] claim_quest failed', e); conn.send({ type:'error', reason:'claim_failed' }); }
        break;
      }
      case 'claim_milestone': {
        try {
          const result = await claimBarMilestone(userId, msg.scope, msg.milestone);
          conn.send({ type:'quest_state', ...(await buildQuestState(userId)) });
          if (!result.ok) conn.send({ type:'error', reason: result.reason || 'claim_failed' });
        } catch (e) { console.error('[arena] claim_milestone failed', e); conn.send({ type:'error', reason:'claim_failed' }); }
        break;
      }

      /* ── SOCIAL: friends + presence (data lives in Supabase; every
       * mutation is still an ordinary WS request/response like everything
       * above) ── */
      case 'tutorial_bonus': {
        try {
          const result = await grantTutorialBonus(userId);
          if (result.granted) conn.send({ type:'tutorial_bonus_granted', gems: TUTORIAL_BONUS_GEMS, profile: await fetchProfile(userId) });
        } catch (e) { console.error('[arena] tutorial_bonus failed', e); }
        break;
      }
      case 'heartbeat': {
        // The client only ever sends this after it has explicitly "gotten
        // on", auth alone never implies presence, by design.
        try {
          const wasOnline = conn.presenceOnline;
          conn.presenceOnline = true;
          conn.lastHeartbeat = Date.now();
          await markPresenceOnline(userId);
          if (!wasOnline) broadcastPresence(userId, true);
        } catch (e) { console.error('[arena] heartbeat failed', e); }
        break;
      }
      case 'friends_list': {
        try { conn.send({ type:'friends_list', ...(await buildFriendsList(userId)) }); }
        catch (e) { console.error('[arena] friends_list failed', e); conn.send({ type:'error', reason:'friends_list_failed' }); }
        break;
      }
      case 'friend_request': {
        try {
          let targetId = typeof msg.userId === 'string' && msg.userId ? msg.userId : null;
          if (!targetId && typeof msg.username === 'string') targetId = await findUserIdByUsername(msg.username);
          if (!targetId) return conn.send({ type:'error', reason:'user_not_found' });
          if (targetId === userId) return conn.send({ type:'error', reason:'cannot_friend_self' });
          if (isBotId(targetId)) return conn.send({ type:'error', reason:'cannot_friend_bot' });
          const existing = await getFriendship(userId, targetId);
          if (existing) return conn.send({ type:'error', reason: existing.status === 'accepted' ? 'already_friends' : 'request_already_pending' });
          await createFriendRequest(userId, targetId);
          conn.send({ type:'friends_list', ...(await buildFriendsList(userId)) });
          const targetConn = connections.get(targetId);
          if (targetConn) {
            const me = await fetchProfileSummaries([userId]);
            targetConn.send({ type:'friend_request_received', userId, username: me.get(userId)?.username, icon: me.get(userId)?.icon });
          }
        } catch (e) { console.error('[arena] friend_request failed', e); conn.send({ type:'error', reason:'friend_request_failed' }); }
        break;
      }
      case 'friend_respond': {
        try {
          const otherId = msg.userId;
          if (typeof otherId !== 'string') return conn.send({ type:'error', reason:'bad_request' });
          const existing = await getFriendship(userId, otherId);
          if (!existing || existing.status !== 'pending' || existing.requestedBy === userId) {
            return conn.send({ type:'error', reason:'no_pending_request' });
          }
          if (msg.accept) {
            await acceptFriendRequest(userId, otherId);
            const otherConn = connections.get(otherId);
            const myList = await buildFriendsList(userId);
            const theirList = await buildFriendsList(otherId);
            conn.send({ type:'friends_list', ...myList });
            if (otherConn) otherConn.send({ type:'friends_list', ...theirList });
            recordQuestThreshold(userId, Engine.QUEST_TRACK.FRIENDS_COUNT, myList.friends.length).catch(e => console.error('[arena] friends_count quest failed', e));
            recordQuestThreshold(otherId, Engine.QUEST_TRACK.FRIENDS_COUNT, theirList.friends.length).catch(e => console.error('[arena] friends_count quest failed', e));
          } else {
            await deleteFriendship(userId, otherId);
            conn.send({ type:'friends_list', ...(await buildFriendsList(userId)) });
          }
        } catch (e) { console.error('[arena] friend_respond failed', e); conn.send({ type:'error', reason:'friend_respond_failed' }); }
        break;
      }
      case 'friend_remove': {
        try {
          const otherId = msg.userId;
          if (typeof otherId !== 'string') return conn.send({ type:'error', reason:'bad_request' });
          await deleteFriendship(userId, otherId);
          const otherConn = connections.get(otherId);
          conn.send({ type:'friends_list', ...(await buildFriendsList(userId)) });
          if (otherConn) otherConn.send({ type:'friends_list', ...(await buildFriendsList(otherId)) });
        } catch (e) { console.error('[arena] friend_remove failed', e); conn.send({ type:'error', reason:'friend_remove_failed' }); }
        break;
      }

      /* ── SOCIAL: guilds. Same request/response posture as everything
       * above, every mutation re-validates from scratch server-side
       * (membership, capacity, funds) rather than trusting client state. ── */
      case 'guild_state': {
        try { conn.send({ type:'guild_state', ...(await buildGuildState(userId)) }); }
        catch (e) { console.error('[arena] guild_state failed', e); conn.send({ type:'error', reason:'guild_state_failed' }); }
        break;
      }
      case 'guild_browse': {
        try {
          const search = typeof msg.search === 'string' ? msg.search.trim().slice(0, GUILD_NAME_MAX) : '';
          conn.send({ type:'guild_browse_result', guilds: await browseGuilds(search) });
        } catch (e) { console.error('[arena] guild_browse failed', e); conn.send({ type:'error', reason:'guild_browse_failed' }); }
        break;
      }
      case 'guild_create': {
        try {
          const guildId = await createGuild(userId, msg);
          conn.send({ type:'guild_created', guildId, ...(await buildGuildState(userId)) });
        } catch (e) {
          if (!['guild_name_invalid','guild_name_taken','already_in_guild','insufficient_funds'].includes(e.code)) console.error('[arena] guild_create failed', e);
          conn.send({ type:'error', reason: e.code || 'guild_create_failed', guildCreateCost: GUILD_CREATE_COST_GEMS });
        }
        break;
      }
      case 'guild_join': {
        try {
          const guildId = msg.guildId;
          if (typeof guildId !== 'string') return conn.send({ type:'error', reason:'bad_request' });
          const guild = await getGuildById(guildId);
          if (!guild) return conn.send({ type:'error', reason:'guild_not_found' });
          if (guild.visibility !== 'public') return conn.send({ type:'error', reason:'guild_not_public' });
          await seatNewMember(guildId, userId);
          await sendGuildState(userId);
          await broadcastGuildState(guildId);
        } catch (e) {
          if (!['already_in_guild','guild_not_found','guild_full','insufficient_funds'].includes(e.code)) console.error('[arena] guild_join failed', e);
          conn.send({ type:'error', reason: e.code || 'guild_join_failed' });
        }
        break;
      }
      case 'guild_apply': {
        try {
          const guildId = msg.guildId;
          if (typeof guildId !== 'string') return conn.send({ type:'error', reason:'bad_request' });
          if (await getGuildMembership(userId)) return conn.send({ type:'error', reason:'already_in_guild' });
          if (await getUserApplication(userId)) return conn.send({ type:'error', reason:'application_already_pending' });
          if (await getUserInvite(userId)) return conn.send({ type:'error', reason:'invite_already_pending' });
          const guild = await getGuildById(guildId);
          if (!guild) return conn.send({ type:'error', reason:'guild_not_found' });
          if (guild.visibility !== 'private') return conn.send({ type:'error', reason:'guild_not_private' });
          if ((await countGuildMembers(guildId)) >= GUILD_MAX_MEMBERS) return conn.send({ type:'error', reason:'guild_full' });
          await createApplication(guildId, userId);
          await sendGuildState(userId);
          // notify the leader (and only the leader, no officer role yet) if online
          const leaderConn = connections.get(guild.leaderId);
          if (leaderConn) sendGuildState(guild.leaderId);
        } catch (e) { console.error('[arena] guild_apply failed', e); conn.send({ type:'error', reason:'guild_apply_failed' }); }
        break;
      }
      case 'guild_application_cancel': {
        try {
          const app = await getUserApplication(userId);
          if (!app) return conn.send({ type:'error', reason:'no_pending_application' });
          await deleteApplication(app.guildId, userId);
          await sendGuildState(userId);
          const guild = await getGuildById(app.guildId);
          if (guild) { const leaderConn = connections.get(guild.leaderId); if (leaderConn) sendGuildState(guild.leaderId); }
        } catch (e) { console.error('[arena] guild_application_cancel failed', e); conn.send({ type:'error', reason:'guild_application_cancel_failed' }); }
        break;
      }
      case 'guild_application_respond': {
        try {
          const membership = await getGuildMembership(userId);
          if (!membership || membership.role !== 'leader') return conn.send({ type:'error', reason:'not_guild_leader' });
          const applicantId = msg.userId;
          if (typeof applicantId !== 'string') return conn.send({ type:'error', reason:'bad_request' });
          const apps = await listApplications(membership.guildId);
          if (!apps.some(a => a.userId === applicantId)) return conn.send({ type:'error', reason:'no_pending_application' });
          await deleteApplication(membership.guildId, applicantId);
          if (msg.accept) {
            try {
              await seatNewMember(membership.guildId, applicantId);
              await broadcastGuildState(membership.guildId);
            } catch (e) {
              // applicant can no longer be seated (guild filled up, or they can't
              // afford the fee anymore), tell them plainly instead of silently
              // dropping their application.
              connections.get(applicantId)?.send({ type:'error', reason: e.code === 'insufficient_funds' ? 'guild_application_accepted_but_underfunded' : (e.code || 'guild_application_accept_failed') });
            }
          }
          await sendGuildState(userId);
          await sendGuildState(applicantId);
        } catch (e) { console.error('[arena] guild_application_respond failed', e); conn.send({ type:'error', reason:'guild_application_respond_failed' }); }
        break;
      }
      case 'guild_invite': {
        try {
          const membership = await getGuildMembership(userId);
          if (!membership || membership.role !== 'leader') return conn.send({ type:'error', reason:'not_guild_leader' });
          let targetId = typeof msg.userId === 'string' && msg.userId ? msg.userId : null;
          if (!targetId && typeof msg.username === 'string') targetId = await findUserIdByUsername(msg.username);
          if (!targetId) return conn.send({ type:'error', reason:'user_not_found' });
          if (targetId === userId) return conn.send({ type:'error', reason:'cannot_invite_self' });
          if (await getGuildMembership(targetId)) return conn.send({ type:'error', reason:'user_already_in_guild' });
          if (await getUserInvite(targetId)) return conn.send({ type:'error', reason:'invite_already_pending' });
          if (await getUserApplication(targetId)) return conn.send({ type:'error', reason:'application_already_pending' });
          if ((await countGuildMembers(membership.guildId)) >= GUILD_MAX_MEMBERS) return conn.send({ type:'error', reason:'guild_full' });
          await createInvite(membership.guildId, targetId, userId);
          conn.send({ type:'guild_invite_sent', userId: targetId });
          await sendGuildState(targetId);
          await sendGuildState(userId); // so the leader's own "invites sent" list updates immediately
        } catch (e) { console.error('[arena] guild_invite failed', e); conn.send({ type:'error', reason:'guild_invite_failed' }); }
        break;
      }
      case 'guild_invite_respond': {
        try {
          const invite = await getUserInvite(userId);
          if (!invite || invite.guildId !== msg.guildId) return conn.send({ type:'error', reason:'no_pending_invite' });
          const guild = await getGuildById(invite.guildId);
          await deleteInvite(invite.guildId, userId);
          if (msg.accept) {
            try {
              await seatNewMember(invite.guildId, userId);
              await broadcastGuildState(invite.guildId); // leader (an existing member) gets refreshed as part of this
            } catch (e) {
              conn.send({ type:'error', reason: e.code || 'guild_invite_accept_failed' });
            }
          } else if (guild) {
            await sendGuildState(guild.leaderId); // so the declined invite drops off the leader's "invites sent" list
          }
          await sendGuildState(userId);
        } catch (e) { console.error('[arena] guild_invite_respond failed', e); conn.send({ type:'error', reason:'guild_invite_respond_failed' }); }
        break;
      }
      case 'guild_invite_cancel': {
        try {
          const membership = await getGuildMembership(userId);
          if (!membership || membership.role !== 'leader') return conn.send({ type:'error', reason:'not_guild_leader' });
          const targetId = msg.userId;
          if (typeof targetId !== 'string') return conn.send({ type:'error', reason:'bad_request' });
          await deleteInvite(membership.guildId, targetId);
          await sendGuildState(targetId);
          await sendGuildState(userId); // so the invite disappears from the leader's own list immediately
        } catch (e) { console.error('[arena] guild_invite_cancel failed', e); conn.send({ type:'error', reason:'guild_invite_cancel_failed' }); }
        break;
      }
      case 'guild_leave': {
        try {
          const membership = await getGuildMembership(userId);
          if (!membership) return conn.send({ type:'error', reason:'not_in_guild' });
          await removeGuildMember(membership.guildId, userId);
          if (membership.role === 'leader') {
            const remaining = await listGuildMembers(membership.guildId);
            if (remaining.length === 0) {
              await deleteGuild(membership.guildId);
            } else {
              // hand leadership to whoever's been there longest
              const next = [...remaining].sort((a, b) => new Date(a.joinedAt) - new Date(b.joinedAt))[0];
              await setGuildLeader(membership.guildId, next.userId);
            }
          }
          await sendGuildState(userId);
          await broadcastGuildState(membership.guildId);
        } catch (e) { console.error('[arena] guild_leave failed', e); conn.send({ type:'error', reason:'guild_leave_failed' }); }
        break;
      }
      case 'guild_kick': {
        try {
          const membership = await getGuildMembership(userId);
          if (!membership || membership.role !== 'leader') return conn.send({ type:'error', reason:'not_guild_leader' });
          const targetId = msg.userId;
          if (typeof targetId !== 'string') return conn.send({ type:'error', reason:'bad_request' });
          if (targetId === userId) return conn.send({ type:'error', reason:'cannot_kick_self' });
          const targetMembership = await getGuildMembership(targetId);
          if (!targetMembership || targetMembership.guildId !== membership.guildId) return conn.send({ type:'error', reason:'user_not_in_guild' });
          await removeGuildMember(membership.guildId, targetId);
          await sendGuildState(targetId);
          await broadcastGuildState(membership.guildId);
        } catch (e) { console.error('[arena] guild_kick failed', e); conn.send({ type:'error', reason:'guild_kick_failed' }); }
        break;
      }
      case 'guild_disband': {
        try {
          const membership = await getGuildMembership(userId);
          if (!membership || membership.role !== 'leader') return conn.send({ type:'error', reason:'not_guild_leader' });
          const members = await listGuildMembers(membership.guildId);
          await deleteGuild(membership.guildId);
          await Promise.all(members.map(m => sendGuildState(m.userId)));
        } catch (e) { console.error('[arena] guild_disband failed', e); conn.send({ type:'error', reason:'guild_disband_failed' }); }
        break;
      }
      case 'guild_chat_history': {
        try {
          const membership = await getGuildMembership(userId);
          if (!membership) return conn.send({ type:'error', reason:'not_in_guild' });
          const messages = await listGuildChatMessages(membership.guildId);
          conn.send({ type:'guild_chat_history', guildId: membership.guildId, messages });
        } catch (e) { console.error('[arena] guild_chat_history failed', e); conn.send({ type:'error', reason:'guild_chat_history_failed' }); }
        break;
      }
      case 'guild_chat_send': {
        try {
          const membership = await getGuildMembership(userId);
          if (!membership) return conn.send({ type:'error', reason:'not_in_guild' });
          const now = Date.now();
          if (now - (guildChatLastSentAt.get(userId) || 0) < GUILD_CHAT_RATE_LIMIT_MS) {
            return conn.send({ type:'error', reason:'guild_chat_rate_limited' });
          }
          if (typeof msg.message !== 'string' || !msg.message.trim()) return conn.send({ type:'error', reason:'guild_chat_empty' });
          guildChatLastSentAt.set(userId, now);
          const message = await sendGuildChatMessage(membership.guildId, userId, msg.message);
          const members = await listGuildMembers(membership.guildId);
          for (const m of members) connections.get(m.userId)?.send({ type:'guild_chat_message', guildId: membership.guildId, message });
        } catch (e) {
          if (e.code !== 'guild_chat_empty') console.error('[arena] guild_chat_send failed', e);
          conn.send({ type:'error', reason: e.code || 'guild_chat_send_failed' });
        }
        break;
      }

      /* ── SOCIAL: duels (1v1 challenges), plain WS request/response,
       * exactly like matchmaking; nothing about a duel invite is persisted. ── */
      case 'duel_request': {
        const targetId = msg.userId;
        if (typeof targetId !== 'string') { conn.send({ type:'error', reason:'bad_request' }); break; }
        if (targetId === userId) { conn.send({ type:'error', reason:'cannot_duel_self' }); break; }
        if (activeMatchByUser.has(userId)) { conn.send({ type:'error', reason:'already_in_match' }); break; }
        const targetConn = connections.get(targetId);
        if (!targetConn) { conn.send({ type:'error', reason:'friend_offline' }); break; }
        if (activeMatchByUser.has(targetId)) { conn.send({ type:'error', reason:'friend_busy' }); break; }
        try {
          const rel = await getFriendship(userId, targetId);
          if (!rel || rel.status !== 'accepted') { conn.send({ type:'error', reason:'not_friends' }); break; }
        } catch (e) { conn.send({ type:'error', reason:'duel_request_failed' }); break; }
        pendingDuels.set(targetId, userId);
        targetConn.send({ type:'duel_request_received', userId, username: conn.username, icon: conn.icon });
        conn.send({ type:'duel_request_sent', userId: targetId });
        break;
      }
      case 'duel_respond': {
        const fromId = msg.userId;
        if (pendingDuels.get(userId) !== fromId) { conn.send({ type:'error', reason:'no_pending_duel' }); break; }
        pendingDuels.delete(userId);
        const fromConn = connections.get(fromId);
        if (!msg.accept) {
          if (fromConn) fromConn.send({ type:'duel_declined', userId });
          break;
        }
        if (activeMatchByUser.has(userId) || activeMatchByUser.has(fromId) || !fromConn) {
          conn.send({ type:'error', reason:'duel_unavailable' });
          break;
        }
        [userId, fromId].forEach(id => {
          removeFromQueues(id);
          clearQueueTimer(id);
        });
        await startDuelMatch(fromId, userId);
        break;
      }

      /* ── SOCIAL: trading, a live negotiation, not a one-shot request
       * like a duel. Anybody currently connected can be traded with (no
       * friendship requirement), same as pressing "Trade" from any
       * profile view client-side. ── */
      case 'trade_request': {
        const targetId = msg.userId;
        if (typeof targetId !== 'string') { conn.send({ type:'error', reason:'bad_request' }); break; }
        if (targetId === userId) { conn.send({ type:'error', reason:'cannot_trade_self' }); break; }
        if (isBotId(targetId)) { conn.send({ type:'error', reason:'cannot_trade_bot' }); break; }
        if (activeMatchByUser.has(userId)) { conn.send({ type:'error', reason:'already_in_match' }); break; }
        if (activeTradeByUser.has(userId)) { conn.send({ type:'error', reason:'already_trading' }); break; }
        const targetConn = connections.get(targetId);
        if (!targetConn) { conn.send({ type:'error', reason:'user_offline' }); break; }
        if (activeMatchByUser.has(targetId) || activeTradeByUser.has(targetId)) { conn.send({ type:'error', reason:'user_busy' }); break; }
        pendingTrades.set(targetId, userId);
        targetConn.send({ type:'trade_request_received', userId, username: conn.username, icon: conn.icon });
        conn.send({ type:'trade_request_sent', userId: targetId });
        break;
      }
      case 'trade_respond': {
        const fromId = msg.userId;
        if (pendingTrades.get(userId) !== fromId) { conn.send({ type:'error', reason:'no_pending_trade' }); break; }
        pendingTrades.delete(userId);
        const fromConn = connections.get(fromId);
        if (!msg.accept) {
          if (fromConn) fromConn.send({ type:'trade_declined', userId });
          break;
        }
        if (activeMatchByUser.has(userId) || activeMatchByUser.has(fromId) ||
            activeTradeByUser.has(userId) || activeTradeByUser.has(fromId) || !fromConn) {
          conn.send({ type:'error', reason:'trade_unavailable' });
          break;
        }
        await startTradeSession(fromId, userId);
        break;
      }
      case 'trade_update_offer': {
        const session = activeTradeByUser.get(userId);
        if (!session) { conn.send({ type:'error', reason:'no_active_trade' }); break; }
        try {
          const profile = await fetchProfile(userId, conn.username);
          const counts = collectionCounts(profile.collection);
          session.offers[userId] = sanitizeTradeOffer(msg.offer, counts, profile.gold, profile.gems);
          resetTradeProgress(session);
          broadcastTradeState(session);
        } catch (e) { conn.send({ type:'error', reason:'trade_update_failed' }); }
        break;
      }
      case 'trade_set_ready': {
        const session = activeTradeByUser.get(userId);
        if (!session) { conn.send({ type:'error', reason:'no_active_trade' }); break; }
        session.ready[userId] = !!msg.ready;
        if (!msg.ready) session.confirmed[userId] = false;
        broadcastTradeState(session);
        break;
      }
      case 'trade_confirm': {
        const session = activeTradeByUser.get(userId);
        if (!session) { conn.send({ type:'error', reason:'no_active_trade' }); break; }
        const [uA, uB] = session.users;
        if (!session.ready[uA] || !session.ready[uB]) { conn.send({ type:'error', reason:'not_ready' }); break; }
        session.confirmed[userId] = true;
        broadcastTradeState(session);
        if (session.confirmed[uA] && session.confirmed[uB]) {
          try {
            await executeTrade(session);
            const [freshA, freshB] = await Promise.all([fetchProfile(uA), fetchProfile(uB)]);
            endTradeSession(session);
            connections.get(uA)?.send({ type:'trade_complete', tradeId: session.id, profile: freshA });
            connections.get(uB)?.send({ type:'trade_complete', tradeId: session.id, profile: freshB });
          } catch (e) {
            console.error('[arena] trade execution failed', e);
            endTradeSession(session);
            connections.get(uA)?.send({ type:'error', reason:'trade_failed' });
            connections.get(uB)?.send({ type:'error', reason:'trade_failed' });
          }
        }
        break;
      }
      case 'trade_cancel': {
        const session = activeTradeByUser.get(userId);
        if (session) cancelTrade(session, userId);
        break;
      }

      /* ── SPECTATING: read-only live view of someone else's match, entered
       * by tapping their purple "in a match" indicator anywhere their
       * avatar shows up. Never leaks either player's hand. ── */
      case 'spectate_request': {
        const targetId = msg.userId;
        if (typeof targetId !== 'string') { conn.send({ type:'error', reason:'bad_request' }); break; }
        const match = activeMatchByUser.get(targetId);
        if (!match) { conn.send({ type:'error', reason:'not_in_match' }); break; }
        const prevMatchId = spectatingUserMatch.get(userId);
        if (prevMatchId && prevMatchId !== match.id) matches.get(prevMatchId)?.removeSpectator(userId);
        match.addSpectator(userId);
        spectatingUserMatch.set(userId, match.id);
        conn.send({
          type: 'spectate_started', matchId: match.id,
          players: [
            { userId: match.users[0], username: match.usernames?.[0] || 'Player', icon: match.icons?.[0] || 'star', rank: match.ranks?.[0] || null },
            { userId: match.users[1], username: match.usernames?.[1] || 'Player', icon: match.icons?.[1] || 'star', rank: match.ranks?.[1] || null },
          ],
          phase: match.phase, turn: match.turn, state: match.spectatorView(),
        });
        break;
      }
      case 'spectate_leave': {
        const matchId = spectatingUserMatch.get(userId);
        if (matchId) { matches.get(matchId)?.removeSpectator(userId); spectatingUserMatch.delete(userId); }
        break;
      }

      /* ── MARKETPLACE ──────────────────────────────────────────── */
      case 'market_browse': {
        try {
          const filter = {};
          if (msg.currency === 'gold' || msg.currency === 'gems') filter.currency = msg.currency;
          if (msg.listingType === 'price' || msg.listingType === 'auction') filter.listingType = msg.listingType;
          const listings = await browseActiveListings(filter);
          const summaries = await fetchProfileSummaries(listings.flatMap(l => [l.sellerId, l.currentBidderId].filter(Boolean)));
          conn.send({ type: 'market_listings', listings: listings.map(l => marketListingPayload(l, summaries)) });
        } catch (e) { console.error('[arena] market_browse failed', e); conn.send({ type: 'error', reason: 'market_browse_failed' }); }
        break;
      }
      case 'market_my_listings': {
        try {
          const listings = await listingsBySeller(userId);
          const summaries = await fetchProfileSummaries(listings.flatMap(l => [l.sellerId, l.currentBidderId].filter(Boolean)));
          conn.send({ type: 'market_my_listings', listings: listings.map(l => marketListingPayload(l, summaries)) });
        } catch (e) { console.error('[arena] market_my_listings failed', e); conn.send({ type: 'error', reason: 'market_my_listings_failed' }); }
        break;
      }
      case 'market_list_card': {
        try {
          const result = await createListing(userId, msg);
          if (result.error) { conn.send({ type: 'error', reason: result.error }); break; }
          const summaries = await fetchProfileSummaries([userId]);
          conn.send({ type: 'market_listing_created', listing: marketListingPayload(result.listing, summaries), profile: await fetchProfile(userId) });
        } catch (e) { console.error('[arena] market_list_card failed', e); conn.send({ type: 'error', reason: 'market_list_failed' }); }
        break;
      }
      case 'market_cancel_listing': {
        try {
          const result = await cancelListing(userId, msg.listingId);
          if (result.error) { conn.send({ type: 'error', reason: result.error }); break; }
          conn.send({ type: 'market_listing_cancelled', listingId: result.listing.id, profile: await fetchProfile(userId) });
        } catch (e) { console.error('[arena] market_cancel_listing failed', e); conn.send({ type: 'error', reason: 'market_cancel_failed' }); }
        break;
      }
      case 'market_buy_listing': {
        try {
          const before = await getListing(msg.listingId);
          const result = await buyListing(userId, msg.listingId);
          if (result.error) { conn.send({ type: 'error', reason: result.error }); break; }
          const buyerProfile = await fetchProfile(userId);
          conn.send({ type: 'market_purchase_complete', listingId: result.listing.id, cardId: result.listing.cardId, profile: buyerProfile });
          if (before) {
            notifyUser(before.sellerId, { type: 'market_item_sold', listingId: result.listing.id, cardId: result.listing.cardId, amount: result.sellerNet ?? (result.listing.currentBid - Math.round(result.listing.currentBid * result.listing.taxRate)), currency: result.listing.currency, buyerName: conn.username });
            if (result.previousBidderId) notifyUser(result.previousBidderId, { type: 'market_outbid', listingId: result.listing.id, reason: 'bought_out' });
          }
        } catch (e) { console.error('[arena] market_buy_listing failed', e); conn.send({ type: 'error', reason: 'market_buy_failed' }); }
        break;
      }
      case 'market_place_bid': {
        try {
          const result = await placeBid(userId, msg.listingId, msg.amount);
          if (result.error) { conn.send({ type: 'error', reason: result.error, minRequired: result.minRequired }); break; }
          const summaries = await fetchProfileSummaries([result.listing.sellerId, result.listing.currentBidderId].filter(Boolean));
          const payload = marketListingPayload(result.listing, summaries);
          if (result.bought) {
            conn.send({ type: 'market_purchase_complete', listingId: result.listing.id, cardId: result.listing.cardId, profile: await fetchProfile(userId) });
          } else {
            conn.send({ type: 'market_bid_placed', listing: payload });
          }
          notifyUser(result.listing.sellerId, { type: 'market_new_bid', listing: payload });
        } catch (e) { console.error('[arena] market_place_bid failed', e); conn.send({ type: 'error', reason: 'market_bid_failed' }); }
        break;
      }

      /* ── TOURNAMENTS ──────────────────────────────────────────────── */
      case 'tournament_list': {
        try { conn.send({ type: 'tournament_list', ...(await buildTournamentListPayload(userId)) }); }
        catch (e) { console.error('[arena] tournament_list failed', e); conn.send({ type: 'error', reason: 'tournament_list_failed' }); }
        break;
      }
      case 'tournament_create': {
        try {
          const result = await createUnofficialTournament(userId, msg);
          if (result.error) { conn.send({ type: 'error', reason: result.error }); break; }
          conn.send({ type: 'tournament_created', event: await tournamentEventPayload(result.event, userId, new Map()) });
        } catch (e) { console.error('[arena] tournament_create failed', e); conn.send({ type: 'error', reason: 'tournament_create_failed' }); }
        break;
      }
      case 'tournament_join': {
        try {
          const result = await registerForTournament(userId, msg.eventId);
          if (result.error) { conn.send({ type: 'error', reason: result.error }); break; }
          conn.send({ type: 'tournament_joined', event: await tournamentEventPayload(result.event, userId, new Map()), profile: await fetchProfile(userId) });
        } catch (e) { console.error('[arena] tournament_join failed', e); conn.send({ type: 'error', reason: 'tournament_join_failed' }); }
        break;
      }
      case 'tournament_leave': {
        try {
          const result = await unregisterFromTournament(userId, msg.eventId);
          if (result.error) { conn.send({ type: 'error', reason: result.error }); break; }
          conn.send({ type: 'tournament_left', event: await tournamentEventPayload(result.event, userId, new Map()), profile: await fetchProfile(userId) });
        } catch (e) { console.error('[arena] tournament_leave failed', e); conn.send({ type: 'error', reason: 'tournament_leave_failed' }); }
        break;
      }
      case 'tournament_cancel': {
        try {
          const result = await cancelUnofficialTournament(userId, msg.eventId);
          if (result.error) { conn.send({ type: 'error', reason: result.error }); break; }
          conn.send({ type: 'tournament_cancelled', event: await tournamentEventPayload(result.event, userId, new Map()) });
        } catch (e) { console.error('[arena] tournament_cancel failed', e); conn.send({ type: 'error', reason: 'tournament_cancel_failed' }); }
        break;
      }
      case 'tournament_bracket': {
        try {
          const bracket = await getBracket(msg.bracketId);
          if (!bracket) { conn.send({ type: 'error', reason: 'tournament_not_found' }); break; }
          conn.send({ type: 'tournament_bracket', bracket: bracketPayload(bracket) });
        } catch (e) { console.error('[arena] tournament_bracket failed', e); conn.send({ type: 'error', reason: 'tournament_bracket_failed' }); }
        break;
      }

      /* ── DIRECT MESSAGES (marketplace negotiation) ───────────────── */
      case 'dm_conversations': {
        try {
          const conversations = await dmConversations(userId);
          const summaries = await fetchProfileSummaries(conversations.map(c => c.userId));
          conn.send({ type: 'dm_conversations', conversations: conversations.map(c => ({ ...c, username: summaries.get(c.userId)?.username || 'Unknown', icon: summaries.get(c.userId)?.icon || 'star' })) });
        } catch (e) { console.error('[arena] dm_conversations failed', e); conn.send({ type: 'error', reason: 'dm_failed' }); }
        break;
      }
      case 'dm_archive_conversation': {
        try {
          const otherId = typeof msg.userId === 'string' ? msg.userId : null;
          if (!otherId) { conn.send({ type: 'error', reason: 'dm_failed' }); break; }
          await archiveDMConversation(userId, otherId);
          const conversations = await dmConversations(userId);
          const summaries = await fetchProfileSummaries(conversations.map(c => c.userId));
          conn.send({ type: 'dm_conversations', conversations: conversations.map(c => ({ ...c, username: summaries.get(c.userId)?.username || 'Unknown', icon: summaries.get(c.userId)?.icon || 'star' })) });
        } catch (e) { console.error('[arena] dm_archive_conversation failed', e); conn.send({ type: 'error', reason: 'dm_failed' }); }
        break;
      }
      case 'dm_history': {
        try {
          const otherId = msg.userId;
          if (typeof otherId !== 'string') { conn.send({ type: 'error', reason: 'bad_request' }); break; }
          const messages = await dmHistory(userId, otherId);
          const summaries = await fetchProfileSummaries([userId, otherId]);
          conn.send({ type: 'dm_history', userId: otherId, username: summaries.get(otherId)?.username || 'Unknown', icon: summaries.get(otherId)?.icon || 'star', messages });
        } catch (e) { console.error('[arena] dm_history failed', e); conn.send({ type: 'error', reason: 'dm_failed' }); }
        break;
      }
      case 'dm_send': {
        try {
          if (isBotId(msg.toId)) { conn.send({ type: 'error', reason: 'dm_invalid' }); break; }
          const result = await sendDM(userId, msg.toId, msg.text, { listingId: msg.listingId, offerAmount: msg.offerAmount, offerCurrency: msg.offerCurrency });
          if (result.error) { conn.send({ type: 'error', reason: result.error }); break; }
          conn.send({ type: 'dm_message', message: result.message });
          notifyUser(msg.toId, { type: 'dm_message', message: result.message, fromUsername: conn.username, fromIcon: conn.icon });
        } catch (e) { console.error('[arena] dm_send failed', e); conn.send({ type: 'error', reason: 'dm_failed' }); }
        break;
      }
      case 'dm_accept_offer': {
        try {
          const result = await acceptOffer(userId, msg.messageId);
          if (result.error) { conn.send({ type: 'error', reason: result.error }); break; }
          const [buyerProfile, sellerProfile] = await Promise.all([fetchProfile(result.buyerId), fetchProfile(result.sellerId)]);
          notifyUser(result.buyerId, { type: 'market_purchase_complete', listingId: result.listing.id, cardId: result.listing.cardId, profile: buyerProfile });
          notifyUser(result.sellerId, { type: 'profile', profile: sellerProfile });
          notifyUser(result.sellerId, { type: 'market_item_sold', listingId: result.listing.id, cardId: result.listing.cardId, amount: result.listing.currentBid - Math.round(result.listing.currentBid * result.listing.taxRate), currency: result.listing.currency });
        } catch (e) { console.error('[arena] dm_accept_offer failed', e); conn.send({ type: 'error', reason: 'dm_failed' }); }
        break;
      }

      default:
        conn.send({ type:'error', reason:'unknown_message_type' });
    }
  });

  ws.on('close', () => {
    if (conn.userId && connections.get(conn.userId) === conn) {
      connections.delete(conn.userId);
      removeFromQueues(conn.userId);
      clearQueueTimer(conn.userId);
      activeMatchByUser.get(conn.userId)?.handleDisconnect(conn.userId);
      if (conn.presenceOnline) {
        conn.presenceOnline = false;
        markPresenceOffline(conn.userId).catch(e => console.error('[arena] markPresenceOffline failed', e));
        broadcastPresence(conn.userId, false);
      }
      pendingDuels.delete(conn.userId);
      for (const [target, requester] of pendingDuels) if (requester === conn.userId) pendingDuels.delete(target);
      pendingTrades.delete(conn.userId);
      for (const [target, requester] of pendingTrades) if (requester === conn.userId) pendingTrades.delete(target);
      const tradeSession = activeTradeByUser.get(conn.userId);
      if (tradeSession) cancelTrade(tradeSession, conn.userId, 'disconnected');
      const specMatchId = spectatingUserMatch.get(conn.userId);
      if (specMatchId) { matches.get(specMatchId)?.removeSpectator(conn.userId); spectatingUserMatch.delete(conn.userId); }
    }
  });
});

/* heartbeat: drop dead sockets so matches don't wait forever on a ghost */
const heartbeat = setInterval(() => {
  for (const conn of connections.values()) {
    if (!conn.alive) { conn.ws.terminate(); continue; }
    conn.alive = false;
    try { conn.ws.ping(); } catch {}
  }
}, 30_000);
wss.on('close', () => clearInterval(heartbeat));

/* presence sweep: a connection that stops sending app-level 'heartbeat'
 * messages (tab backgrounded, app minimized, etc.) still counts as
 * offline for friends even if the raw socket is technically alive. */
const presenceSweep = setInterval(() => {
  const now = Date.now();
  for (const conn of connections.values()) {
    if (conn.presenceOnline && conn.userId && (now - conn.lastHeartbeat) > PRESENCE_HEARTBEAT_TIMEOUT_MS) {
      conn.presenceOnline = false;
      markPresenceOffline(conn.userId).catch(e => console.error('[arena] markPresenceOffline failed', e));
      broadcastPresence(conn.userId, false);
    }
  }
}, PRESENCE_SWEEP_MS);
wss.on('close', () => clearInterval(presenceSweep));

/* guild chat retention: messages older than 7 days are deleted hourly.
 * Also run once shortly after boot in case the server was down past the
 * top of an hour and a backlog built up. The read path in
 * listGuildChatMessages() defensively re-filters by age too, so nothing
 * expired is ever served even in the gap between sweeps. */
const GUILD_CHAT_CLEANUP_INTERVAL_MS = 60 * 60 * 1000;
setTimeout(() => cleanupExpiredGuildChatMessages().catch(e => console.error('[arena] guild chat cleanup failed', e)), Number(process.env.GUILD_CHAT_INITIAL_CLEANUP_DELAY_MS) || 10_000);
const guildChatCleanup = setInterval(() => {
  cleanupExpiredGuildChatMessages().catch(e => console.error('[arena] guild chat cleanup failed', e));
}, GUILD_CHAT_CLEANUP_INTERVAL_MS);
wss.on('close', () => clearInterval(guildChatCleanup));

/* marketplace sweep: settles any listing/auction whose expires_at has
 * passed, auctions with a bid go to the highest bidder, everything else
 * (no-bid auctions, unsold price listings) returns the card to the seller.
 * Runs frequently since listings can be as short as 1 day and players
 * shouldn't wait an hour to get an expired card back. */
setTimeout(() => settleExpiredListings().catch(e => console.error('[arena] marketplace sweep failed', e)), 5_000);
const marketSweep = setInterval(() => {
  settleExpiredListings().catch(e => console.error('[arena] marketplace sweep failed', e));
}, MARKET_SWEEP_MS);
wss.on('close', () => clearInterval(marketSweep));

/* purges listing-linked DM clutter ~1hr after that listing settles, see
 * cleanupExpiredListingDMs() for why. */
setTimeout(() => cleanupExpiredListingDMs().catch(e => console.error('[arena] DM cleanup failed', e)), 15_000);
const dmCleanupSweep = setInterval(() => {
  cleanupExpiredListingDMs().catch(e => console.error('[arena] DM cleanup failed', e));
}, DM_CLEANUP_SWEEP_MS);
wss.on('close', () => clearInterval(dmCleanupSweep));

/* keeps official Daily/Weekly tournament slots open for registration and
 * locks+shards any event (official or player-hosted) whose start_at has
 * arrived, see tournamentSweep(). Runs once immediately so the official
 * slots exist right at boot rather than waiting for the first tick. */
tournamentSweep().catch(e => console.error('[arena] initial tournament sweep failed', e));
const tournamentSweepTimer = setInterval(() => {
  tournamentSweep().catch(e => console.error('[arena] tournament sweep failed', e));
}, TOURNAMENT_SWEEP_MS);
wss.on('close', () => clearInterval(tournamentSweepTimer));

syncChatImageCatalog();
startGlobalChatQuestWatcher();

server.listen(PORT, () => {
  console.log(`[arena] listening on :${PORT} (supabase ${HAS_SUPABASE ? 'ON' : 'OFF, guest mode'})`);
});
