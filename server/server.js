'use strict';
/**
 * Arena of PESTS — realtime combat server.
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
  console.warn('[arena] names.json missing/invalid — falling back to a tiny built-in name list.');
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
  console.warn('[arena] SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY not set — running in GUEST-ONLY mode.');
  console.warn('[arena] Wins/losses/gold/gems/collection will NOT persist. See .env.example.');
}

/* ── IN-MEMORY REGISTRIES ────────────────────────────────────────── */
/** userId -> Connection */
const connections = new Map();
/** userId -> Match */
const activeMatchByUser = new Map();
/** matchId -> Match */
const matches = new Map();
/** FIFO queue of userIds waiting for an opponent */
const queue = [];
/** userId -> pending bot-fallback Timeout, armed while that user sits in `queue` */
const queueTimers = new Map();
/** targetUserId -> requesterUserId — at most one live incoming duel invite
 * tracked per target; a newer invite simply replaces an older unanswered one. */
const pendingDuels = new Map();
/** targetUserId -> requesterUserId — same shape as pendingDuels, but for
 * trade invites. A player can have at most one pending trade AND one
 * pending duel at a time, tracked independently. */
const pendingTrades = new Map();
/** tradeId -> TradeSession — live trade negotiations. */
const tradeSessions = new Map();
/** userId -> TradeSession — at most one active trade per user, mirroring
 * activeMatchByUser so "already trading"/"already in a match" checks read
 * the same way everywhere. */
const activeTradeByUser = new Map();
/** userId -> matchId — at most one live spectate session per viewer;
 * starting a new one silently replaces whatever they were watching before. */
const spectatingUserMatch = new Map();

/** Sends to literally every connected client — used only for the
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
const guestPresence = new Map(); // userId -> { lastHeartbeat, online } — only used when Supabase isn't configured
const guestFriendships = new Map(); // pairKey -> { status:'pending'|'accepted', requestedBy, createdAt } — guest-mode fallback

/** Guild registries — only used when Supabase isn't configured. Mirrors the
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

let nextGuestId = 1;

/* ── PROFILE CUSTOMIZATION (validated allow-lists) ────────────────── */
// The server is the only thing that ever writes these fields, and it only
// ever accepts values from these lists — an emoji/theme the client didn't
// offer never reaches Postgres, no matter what a modified client sends.
// Icon values are ids (not emoji) — the client maps each id to a custom SVG
// glyph it draws itself. Keep this list in sync with ICON_SVGS in docs/index.html.
const PROFILE_ICONS = ['star','crown','skull','flame','blade','shield','moon','ward','thorn','storm','spider','scorpion','beetle','serpent','laurel'];
const PROFILE_BANNERS = ['violet','crimson','emerald','gold','azure','obsidian','rose','storm'];
const BIO_MAX = 140;
const USERNAME_MAX = 24;
const FAVORITES_MAX = 3;

/* ── GUILDS (validated allow-lists, same posture as profile icons/banners) ─
 * `icon` is the emblem drawn in the middle (reuses the same hand-drawn SVG
 * glyph set as player profiles — never emoji). `frame` is a separate
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

/** Favorite cards must actually be owned — checked against the caller's own
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
        gold: 500, gems: 25, wins: 0, losses: 0, icon: 'star', banner: 'violet', bio: '', favoriteCards: [],
        collection: seedStarterIds(), deck: [] });
    }
    return guestProfiles.get(userId);
  }
  let { data: profile, error } = await supabase.from('profiles').select('*').eq('id', userId).maybeSingle();
  if (error) throw error;
  if (!profile) {
    const insert = { id: userId, username: fallbackName || `Pestmaster${nextGuestId++}`, gold: 500, gems: 25, wins: 0, losses: 0 };
    const { data: created, error: insErr } = await supabase.from('profiles').insert(insert).select('*').single();
    if (insErr) throw insErr;
    profile = created;
    // seed starter collection for brand-new accounts
    const starter = seedStarterIds();
    await supabase.from('player_cards').upsert(
      starter.map(card_id => ({ owner_id: userId, card_id, quantity: 1 })),
      { onConflict: 'owner_id,card_id' }
    );
  }
  const { data: cardRows } = await supabase.from('player_cards').select('card_id,quantity').eq('owner_id', userId);
  const { data: deckRow } = await supabase.from('player_decks').select('card_ids').eq('owner_id', userId).maybeSingle();
  return {
    id: profile.id, username: profile.username, gold: profile.gold, gems: profile.gems,
    wins: profile.wins, losses: profile.losses,
    icon: profile.icon || 'star', banner: profile.banner || 'violet', bio: profile.bio || '',
    favoriteCards: profile.favorite_cards || [],
    collection: (cardRows || []).flatMap(r => Array(r.quantity).fill(r.card_id)),
    deck: (deckRow && deckRow.card_ids) || [],
  };
}

/** Validates and persists a profile customization update (name/icon/banner/
 * bio/favorite cards). Silently drops anything that fails validation rather
 * than erroring the whole request, then returns a fresh full profile
 * snapshot so the client can re-render from one source of truth. */
async function updateProfile(userId, msg, ownedSet) {
  const fields = sanitizeProfileFields(msg);
  const favoriteCards = sanitizeFavorites(msg.favoriteCards, ownedSet);

  if (!HAS_SUPABASE) {
    const p = guestProfiles.get(userId);
    if (p) {
      Object.assign(p, fields);
      if (favoriteCards !== undefined) p.favoriteCards = favoriteCards;
    }
    return fetchProfile(userId);
  }

  const dbFields = { ...fields };
  if (favoriteCards !== undefined) dbFields.favorite_cards = favoriteCards;
  if (Object.keys(dbFields).length) {
    const { error } = await supabase.from('profiles').update(dbFields).eq('id', userId);
    if (error) throw error;
  }
  return fetchProfile(userId);
}
/* ── FRIENDS + PRESENCE LAYER (Supabase-backed, guest fallback) ───────
 * Every mutation still flows through a WebSocket message like everything
 * else in this file — this just decides where the resulting row lives.
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

/** Lightweight {username,icon} lookup for many ids at once — a friends
 * list has no business pulling everyone's full collection/deck the way
 * fetchProfile does. */
async function fetchProfileSummaries(userIds) {
  const ids = [...new Set(userIds)];
  const out = new Map();
  if (!ids.length) return out;
  if (!HAS_SUPABASE) {
    for (const id of ids) {
      const p = guestProfiles.get(id);
      out.set(id, { username: p ? p.username : 'Unknown', icon: (p && p.icon) || 'star' });
    }
    return out;
  }
  const { data, error } = await supabase.from('profiles').select('id,username,icon').in('id', ids);
  if (error) throw error;
  for (const row of data || []) out.set(row.id, { username: row.username, icon: row.icon || 'star' });
  for (const id of ids) if (!out.has(id)) out.set(id, { username: 'Unknown', icon: 'star' });
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

/** Deletes the relationship regardless of status — covers unfriending an
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

/** Batched online check — a user counts as online only if their presence
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

/** Pushes a presence flip to every online friend's live connection —
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
 * as friends — the client never talks to these tables directly. */

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
 * leader-first then alphabetically — same "who's actually here" shape the
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

/** Validated field extraction shared by guild_create — throws with a `.code`
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
 * (insufficient funds, duplicate name, already in a guild, bad fields) —
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

/** Everyone a guild has outstanding invites out to right now — shown only
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
 * cleanupExpiredGuildChatMessages sweep near server startup below) — the
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

/** Inserts one message and returns it fully enriched (username/icon) —
 * exactly the shape the client needs to render it immediately, whether
 * from guild_chat_history or a live guild_chat_message broadcast. */
async function sendGuildChatMessage(guildId, userId, text) {
  const message = String(text || '').trim().slice(0, GUILD_CHAT_MESSAGE_MAX);
  if (!message) { const e = new Error('guild_chat_empty'); e.code = 'guild_chat_empty'; throw e; }
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
 * guild — used after any join/leave/kick/disband/leadership-change so
 * every open client's roster stays in sync without polling. */
async function broadcastGuildState(guildId) {
  try {
    const members = await listGuildMembers(guildId);
    await Promise.all(members.map(m => sendGuildState(m.userId)));
  } catch (e) { console.error('[arena] broadcastGuildState failed', e); }
}

function seedStarterIds() {
  // All-equipment-plus-normal-creatures starter set contains no Boss/Overlord
  // (or special Pests-tier) cards at all, so it's legal by construction under
  // Engine.deckClassificationOk — a new player can save their whole starter
  // collection as their first deck.
  const equipment = Engine.CardDB.filter(c => c.cardType === 'weapon' || c.cardType === 'defense').map(c => c.id);
  const normals = Engine.CardDB.filter(c => !c.cardType && c.classification === 'normal').map(c => c.id);
  return [...equipment, ...normals].slice(0, Engine.DECK_SIZE);
}

async function saveDeck(userId, cardIds, ownedSet) {
  const ids = Array.isArray(cardIds) ? cardIds : [];
  const owned = ids.filter(id => ownedSet.has(id));
  if (!Engine.isDeckLegal(owned)) {
    const e = new Error('deck_illegal');
    e.code = owned.length !== Engine.DECK_SIZE ? 'deck_wrong_size' : 'deck_composition_invalid';
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
  const result = Engine.openPack(packId); // throws on bad packId — validated server-side, client can't fake odds
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
    // bump quantities: fetch existing then upsert (small N, simplicity over cleverness)
    const counts = {};
    result.cards.forEach(c => { counts[c.id] = (counts[c.id] || 0) + 1; });
    for (const [card_id, addQty] of Object.entries(counts)) {
      const { data: existing } = await supabase.from('player_cards').select('quantity').eq('owner_id', userId).eq('card_id', card_id).maybeSingle();
      const quantity = (existing?.quantity || 0) + addQty;
      await supabase.from('player_cards').upsert({ owner_id: userId, card_id, quantity }, { onConflict: 'owner_id,card_id' });
    }
  }
  return { cards: result.cards, newBalance, currency: result.currency };
}

/** Bot opponents get a `bot:<uuid>` userId — never a real profile row, so
 * nothing here should try to read/write one as if it belonged to a player. */
const isBotId = id => typeof id === 'string' && id.startsWith('bot:');

async function applyMatchReward(winnerId, loserId) {
  if (!HAS_SUPABASE) {
    const w = guestProfiles.get(winnerId), l = guestProfiles.get(loserId);
    if (w) { w.wins++; w.gold += WIN_GOLD_REWARD; }
    if (l) { l.losses++; }
    return { gold: WIN_GOLD_REWARD, gems: 0 };
  }
  if (!isBotId(winnerId)) {
    const { data: winner } = await supabase.from('profiles').select('gold,wins').eq('id', winnerId).maybeSingle();
    if (winner) {
      await supabase.from('profiles').update({ gold: winner.gold + WIN_GOLD_REWARD, wins: winner.wins + 1 }).eq('id', winnerId);
    }
  }
  if (!isBotId(loserId)) {
    const { data: loser } = await supabase.from('profiles').select('losses').eq('id', loserId).maybeSingle();
    if (loser) await supabase.from('profiles').update({ losses: loser.losses + 1 }).eq('id', loserId);
  }
  // don't log fake matches against a bot into permanent match history
  if (!isBotId(winnerId) && !isBotId(loserId)) {
    await supabase.from('match_history').insert({ player_a: winnerId, player_b: loserId, winner: winnerId, reward_gold: WIN_GOLD_REWARD, reward_gems: 0 });
  }
  return { gold: WIN_GOLD_REWARD, gems: 0 };
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
  constructor(userA, userB, deckA, deckB) {
    this.id = crypto.randomUUID();
    this.users = [userA, userB]; // side 0, side 1
    this.sides = [Engine.freshSide(deckA), Engine.freshSide(deckB)];
    this.actedThisTurn = [new Set(), new Set()];
    this.phase = 'SETUP';
    this.turn = 0; // side index whose turn it is (meaningless during SETUP)
    this.readyForBattle = [false, false];
    this.timer = null;
    this.disconnectTimers = [null, null];
    /** userIds currently spectating this match — see addSpectator/removeSpectator. */
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

  /** Relays a chat line to both participants in this match only — never
   * broadcast anywhere else. Silently drops empty/oversized text instead
   * of erroring, since a stray keystroke shouldn't need a round trip. */
  handleChat(userId, text) {
    const clean = String(text || '').trim().slice(0, 240);
    if (!clean) return;
    const side = this.sideOf(userId);
    if (side === -1) return;
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

  /** Public, hidden-hand-free view of both sides — spectators never see
   * either player's hand, only counts, matching how the opponent's hand is
   * already hidden from a normal player. */
  spectatorView() {
    const strip = s => ({
      hp: s.hp, maxHp: s.maxHp, activeCard: s.activeCard, activeCard2: s.activeCard2,
      weaponCard: s.weaponCard, defenseCard: s.defenseCard, deckCount: s.deck.length, handCount: s.hand.length,
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
        { userId: this.users[0], username: this.usernames?.[0] || 'Player', icon: this.icons?.[0] || 'star' },
        { userId: this.users[1], username: this.usernames?.[1] || 'Player', icon: this.icons?.[1] || 'star' },
      ],
      state: this.spectatorView(), events: events || [],
    };
    for (const uid of this.spectators) connections.get(uid)?.send(payload);
  }

  /** Notify every current spectator the match is over, and forget them —
   * called right before the match itself is torn down. */
  clearSpectators(reason) {
    for (const uid of this.spectators) {
      connections.get(uid)?.send({ type:'spectate_ended', matchId:this.id, reason: reason || 'finished' });
      if (spectatingUserMatch.get(uid) === this.id) spectatingUserMatch.delete(uid);
    }
    this.spectators.clear();
  }

  /** Never leak the opponent's hand contents — only its count. */
  perspective(side) {
    const opp = this.otherSide(side);
    const strip = s => ({
      hp: s.hp, maxHp: s.maxHp, activeCard: s.activeCard, activeCard2: s.activeCard2,
      weaponCard: s.weaponCard, defenseCard: s.defenseCard, deckCount: s.deck.length,
    });
    return {
      you: { ...strip(this.sides[side]), hand: this.sides[side].hand },
      opponent: { ...strip(this.sides[opp]), handCount: this.sides[opp].hand.length },
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
    const side = this.turn;
    const entity = this.sides[side];
    if (!isFirstTurnOfMatch && entity.deck.length > 0 && entity.hand.length < 6) entity.hand.push(entity.deck.pop());
    const ctx = { events: [], skipTurn: false };
    if (entity.activeCard || entity.activeCard2) {
      Engine.processEffects(entity, 'onTurnStart', ctx, side);
      Engine.checkCardDeath(entity, ctx.events, side);
    }
    this.actedThisTurn = [new Set(), new Set()];
    const over = Engine.isMatchOver(this);
    if (over !== null) { this.broadcastState(ctx.events); this.finish(over); return; }
    if (ctx.skipTurn) {
      this.broadcastState(ctx.events.concat([{ t:'turn_skip', side }]));
      setTimeout(() => this.endTurn(side, true), 1200);
      return;
    }
    this.broadcastState(ctx.events);
    this.armTurnTimer();
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
      Engine.applyDeployAbility(this.sides, side, card, events);
      events.push({ t:'deploy', side, slotType:'slot1', card, swapped:false });
    } else if (!entity.activeCard2) {
      entity.activeCard2 = card; entity.hand.splice(idx, 1);
      Engine.applyDeployAbility(this.sides, side, card, events);
      events.push({ t:'deploy', side, slotType:'slot2', card, swapped:false });
    } else {
      // swap into slot1 — triggers rocks trap from the opposing active card, exactly like the client
      events.push({ t:'deploy', side, slotType:'slot1', card, swapped:true });
      Engine.triggerRocks(this.sides[this.otherSide(side)], card, events, side, 'slot1');
      const old = entity.activeCard;
      entity.activeCard = card; entity.hand.splice(idx, 1); entity.hand.push(old);
      Engine.checkCardDeath(entity, events, side);
    }
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
    this.broadcastState(result.events);

    const over = Engine.isMatchOver(this);
    if (over !== null) { this.finish(over); return; }

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
    this.conn(side)?.send({ type:'match_found', matchId: this.id, youAre: side, opponentName: this.usernames?.[this.otherSide(side)] || 'Opponent', opponentIcon: this.icons?.[this.otherSide(side)] || 'star', resumed: true });
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
    try { reward = await applyMatchReward(winnerId, loserId); }
    catch (e) { console.error('[arena] reward write failed', e); }

    for (let side = 0; side < 2; side++) {
      const c = this.conn(side);
      if (!c) continue;
      const won = this.users[side] === winnerId;
      let profile = null;
      try { profile = await fetchProfile(this.users[side]); } catch (e) { /* best effort */ }
      c.send({ type:'match_over', result: won ? 'win' : 'loss', reward: won ? reward : { gold:0, gems:0 }, profile });
    }
  }
}

/* ── MATCHMAKING ──────────────────────────────────────────────────── */
function clearQueueTimer(userId) {
  const t = queueTimers.get(userId);
  if (t) { clearTimeout(t); queueTimers.delete(userId); }
}

/** Arm (or re-arm) the randomized 13–17s window after which, if this user is
 * still waiting, they're matched against a bot instead of a real opponent. */
function armBotFallback(userId) {
  clearQueueTimer(userId);
  const delay = BOT_FALLBACK_MIN_MS + Math.floor(Math.random() * (BOT_FALLBACK_MAX_MS - BOT_FALLBACK_MIN_MS));
  queueTimers.set(userId, setTimeout(() => startBotMatch(userId), delay));
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
 * during SETUP, then plays/attacks/ends turn during MAIN — all through the
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
 * no real opponent found. Builds a normal two-sided Match — the human's
 * client only ever sees a `match_found` with a human-sounding opponent name
 * and never learns the other "player" is server-controlled. */
async function startBotMatch(userId) {
  queueTimers.delete(userId);
  const i = queue.indexOf(userId);
  if (i === -1) return; // already matched with a real opponent, or left the queue
  queue.splice(i, 1);

  const conn = connections.get(userId);
  if (!conn || conn.ws.readyState !== conn.ws.OPEN) return;

  try {
    const profile = await fetchProfile(userId, conn.username);
    const humanDeck = Engine.buildDeckFromIds(Engine.isDeckLegal(profile.deck) ? profile.deck : null);
    const botDeck = Engine.buildDeckFromIds(null); // random deck, same as any fresh/guest opponent would get
    const botUserId = `bot:${crypto.randomUUID()}`;
    const botName = pickBotName();
    const humanSide = Math.random() < 0.5 ? 0 : 1;

    const uA = humanSide === 0 ? userId : botUserId;
    const uB = humanSide === 0 ? botUserId : userId;
    const dA = humanSide === 0 ? humanDeck : botDeck;
    const dB = humanSide === 0 ? botDeck : humanDeck;

    const match = new Match(uA, uB, dA, dB);
    match.usernames = humanSide === 0 ? [profile.username, botName] : [botName, profile.username];
    match.icons = humanSide === 0 ? [profile.icon || 'star', 'skull'] : ['skull', profile.icon || 'star'];
    match.botSide = humanSide === 0 ? 1 : 0;
    match.botUserId = botUserId;

    conn.send({ type: 'match_found', matchId: match.id, youAre: humanSide, opponentName: botName, opponentIcon: 'skull' });
    match.broadcastState([]);
    attachBotAI(match);
  } catch (e) {
    console.error('[arena] bot match failed', e);
    conn.send({ type: 'error', reason: 'matchmaking_failed' });
  }
}

/** Starts a direct match between two friends who both agreed to a duel —
 * same match-creation shape as tryMatch/startBotMatch, just without the
 * queue or bot-fallback machinery around it. */
async function startDuelMatch(uA, uB) {
  const connA = connections.get(uA), connB = connections.get(uB);
  try {
    const [profileA, profileB] = await Promise.all([fetchProfile(uA), fetchProfile(uB)]);
    const deckA = Engine.buildDeckFromIds(Engine.isDeckLegal(profileA.deck) ? profileA.deck : null);
    const deckB = Engine.buildDeckFromIds(Engine.isDeckLegal(profileB.deck) ? profileB.deck : null);
    const match = new Match(uA, uB, deckA, deckB);
    match.usernames = [profileA.username, profileB.username];
    match.icons = [profileA.icon || 'star', profileB.icon || 'star'];
    connA?.send({ type: 'match_found', matchId: match.id, youAre: 0, opponentName: profileB.username, opponentIcon: profileB.icon || 'star' });
    connB?.send({ type: 'match_found', matchId: match.id, youAre: 1, opponentName: profileA.username, opponentIcon: profileA.icon || 'star' });
    match.broadcastState([]);
  } catch (e) {
    console.error('[arena] duel match failed', e);
    connA?.send({ type: 'error', reason: 'duel_match_failed' });
    connB?.send({ type: 'error', reason: 'duel_match_failed' });
  }
}

/* ── TRADING ──────────────────────────────────────────────────────
 * A trade is a live negotiation between two connected players: each side
 * builds an "offer" (some cards + gold + gems taken from their own
 * collection/wallet), both sides must explicitly mark themselves ready,
 * and then both sides must explicitly *confirm* — matching the client's
 * "are you sure?" prompt — before anything is actually moved. Every offer
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

/** Final, authoritative check right before cards/currency actually move —
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
/** Any offer change invalidates both sides' ready/confirm state — same
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

/** Starts a live trade session between two already-agreed players — same
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

/** +delta gives copies to userId, -delta removes them — used for both
 * sides of a trade swap. Guest mode mutates the in-memory flat array;
 * Supabase mode bumps/deletes the player_cards row. */
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
  const { data: existing } = await supabase.from('player_cards').select('quantity').eq('owner_id', userId).eq('card_id', cardId).maybeSingle();
  const newQty = (existing?.quantity || 0) + delta;
  if (newQty <= 0) await supabase.from('player_cards').delete().eq('owner_id', userId).eq('card_id', cardId);
  else await supabase.from('player_cards').upsert({ owner_id: userId, card_id: cardId, quantity: newQty }, { onConflict: 'owner_id,card_id' });
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

/** The actual swap — only ever called once both sides have confirmed.
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
    } catch (e) { /* history logging is best-effort — never blocks the trade itself */ }
  }
}

async function tryMatch() {
  while (queue.length >= 2) {
    const uA = queue.shift(), uB = queue.shift();
    clearQueueTimer(uA); clearQueueTimer(uB);
    const connA = connections.get(uA), connB = connections.get(uB);
    if (!connA || connA.ws.readyState !== connA.ws.OPEN) { if (connB) { queue.unshift(uB); armBotFallback(uB); } continue; }
    if (!connB || connB.ws.readyState !== connB.ws.OPEN) { queue.unshift(uA); armBotFallback(uA); continue; }
    try {
      const [profileA, profileB] = await Promise.all([fetchProfile(uA), fetchProfile(uB)]);
      const deckA = Engine.buildDeckFromIds(Engine.isDeckLegal(profileA.deck) ? profileA.deck : null);
      const deckB = Engine.buildDeckFromIds(Engine.isDeckLegal(profileB.deck) ? profileB.deck : null);
      const match = new Match(uA, uB, deckA, deckB);
      match.usernames = [profileA.username, profileB.username];
      match.icons = [profileA.icon || 'star', profileB.icon || 'star'];
      connA.send({ type:'match_found', matchId: match.id, youAre: 0, opponentName: profileB.username, opponentIcon: profileB.icon || 'star' });
      connB.send({ type:'match_found', matchId: match.id, youAre: 1, opponentName: profileA.username, opponentIcon: profileA.icon || 'star' });
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
  if (req.url === '/health') { res.writeHead(200, {'content-type':'application/json'}); res.end(JSON.stringify({ ok:true, matches: matches.size, queue: queue.length })); return; }
  if (req.url === '/cards.hash') {
    // Tiny endpoint for the "have I already got this?" check — a client with
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
    // The single canonical card library — the client fetches this instead of keeping
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
        // Every client must be running the exact same card library we are — this is the
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
          // guest path — stable id per socket session, not persisted server-restart
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
        conn.send({ type:'auth_ok', userId, profile, profileOptions: { icons: PROFILE_ICONS, banners: PROFILE_BANNERS, bioMax: BIO_MAX, usernameMax: USERNAME_MAX, favoritesMax: FAVORITES_MAX }, guildOptions: { icons: GUILD_ICONS, frames: GUILD_FRAMES, nameMin: GUILD_NAME_MIN, nameMax: GUILD_NAME_MAX, maxMembers: GUILD_MAX_MEMBERS, createCostGems: GUILD_CREATE_COST_GEMS, joinFeeMaxGold: GUILD_JOIN_FEE_MAX_GOLD, joinFeeMaxGems: GUILD_JOIN_FEE_MAX_GEMS, chatMessageMax: GUILD_CHAT_MESSAGE_MAX, chatRetentionDays: GUILD_CHAT_RETENTION_MS / (24*60*60*1000) }, inMatchUserIds: [...activeMatchByUser.keys()] });
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
        // pairing with a real opponent or falling back to a bot — same gate, same code path.
        if (conn.cardLibraryHash !== Engine.CARD_LIBRARY_HASH) {
          return conn.send({ type:'error', reason:'card_library_mismatch', expectedHash: Engine.CARD_LIBRARY_HASH });
        }
        if (!queue.includes(userId)) queue.push(userId);
        armBotFallback(userId);
        conn.send({ type:'queue_status', inQueue:true });
        tryMatch();
        break;
      }
      case 'queue_leave': {
        const i = queue.indexOf(userId); if (i !== -1) queue.splice(i, 1);
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
        // friend) — strips wallet balances and the full collection/deck,
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
        } catch (e) { conn.send({ type:'error', reason: e.code || 'save_deck_failed' }); }
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

      /* ── SOCIAL: friends + presence (data lives in Supabase; every
       * mutation is still an ordinary WS request/response like everything
       * above) ── */
      case 'heartbeat': {
        // The client only ever sends this after it has explicitly "gotten
        // on" — auth alone never implies presence, by design.
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
            conn.send({ type:'friends_list', ...(await buildFriendsList(userId)) });
            if (otherConn) otherConn.send({ type:'friends_list', ...(await buildFriendsList(otherId)) });
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
       * above — every mutation re-validates from scratch server-side
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
          // notify the leader (and only the leader — no officer role yet) if online
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
              // afford the fee anymore) — tell them plainly instead of silently
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

      /* ── SOCIAL: duels (1v1 challenges) — plain WS request/response,
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
          const i = queue.indexOf(id); if (i !== -1) queue.splice(i, 1);
          clearQueueTimer(id);
        });
        await startDuelMatch(fromId, userId);
        break;
      }

      /* ── SOCIAL: trading — a live negotiation, not a one-shot request
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
            { userId: match.users[0], username: match.usernames?.[0] || 'Player', icon: match.icons?.[0] || 'star' },
            { userId: match.users[1], username: match.usernames?.[1] || 'Player', icon: match.icons?.[1] || 'star' },
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
      default:
        conn.send({ type:'error', reason:'unknown_message_type' });
    }
  });

  ws.on('close', () => {
    if (conn.userId && connections.get(conn.userId) === conn) {
      connections.delete(conn.userId);
      const i = queue.indexOf(conn.userId); if (i !== -1) queue.splice(i, 1);
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

server.listen(PORT, () => {
  console.log(`[arena] listening on :${PORT} (supabase ${HAS_SUPABASE ? 'ON' : 'OFF — guest mode'})`);
});
