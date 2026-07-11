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

let nextGuestId = 1;

/* ── PROFILE CUSTOMIZATION (validated allow-lists) ────────────────── */
// The server is the only thing that ever writes these fields, and it only
// ever accepts values from these lists — an emoji/theme the client didn't
// offer never reaches Postgres, no matter what a modified client sends.
const PROFILE_ICONS = ['✦','🐛','🕷️','🦂','🐜','🦗','🪲','🐝','👑','💀','🔥','⚔️','🛡️','🌙','☠️','🧿'];
const PROFILE_BANNERS = ['violet','crimson','emerald','gold','azure','obsidian','rose','storm'];
const BIO_MAX = 140;
const USERNAME_MAX = 24;
const FAVORITES_MAX = 3;

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
        gold: 500, gems: 25, wins: 0, losses: 0, icon: '✦', banner: 'violet', bio: '', favoriteCards: [],
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
    icon: profile.icon || '✦', banner: profile.banner || 'violet', bio: profile.bio || '',
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
    this.cardLibraryHash = null;
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
    matches.set(this.id, this);
    this.users.forEach(u => activeMatchByUser.set(u, this));
  }

  otherSide(side) { return side === 0 ? 1 : 0; }
  sideOf(userId) { return this.users[0] === userId ? 0 : this.users[1] === userId ? 1 : -1; }

  conn(side) { return connections.get(this.users[side]) || null; }

  broadcastState(events) {
    for (let side = 0; side < 2; side++) {
      const c = this.conn(side);
      if (c) c.send({ type: 'state', matchId: this.id, phase: this.phase, turn: this.turn, you: side, state: this.perspective(side), events: events || [] });
    }
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
    this.conn(side)?.send({ type:'match_found', matchId: this.id, youAre: side, opponentName: this.usernames?.[this.otherSide(side)] || 'Opponent', resumed: true });
    this.broadcastState([]);
  }

  async finish(winnerSide) {
    if (this.finished) return; this.finished = true;
    this.clearTimer();
    this.disconnectTimers.forEach(t => t && clearTimeout(t));
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
    match.botSide = humanSide === 0 ? 1 : 0;
    match.botUserId = botUserId;

    conn.send({ type: 'match_found', matchId: match.id, youAre: humanSide, opponentName: botName });
    match.broadcastState([]);
    attachBotAI(match);
  } catch (e) {
    console.error('[arena] bot match failed', e);
    conn.send({ type: 'error', reason: 'matchmaking_failed' });
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
      connA.send({ type:'match_found', matchId: match.id, youAre: 0, opponentName: profileB.username });
      connB.send({ type:'match_found', matchId: match.id, youAre: 1, opponentName: profileA.username });
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
        conn.send({ type:'auth_ok', userId, profile, profileOptions: { icons: PROFILE_ICONS, banners: PROFILE_BANNERS, bioMax: BIO_MAX, usernameMax: USERNAME_MAX, favoritesMax: FAVORITES_MAX } });
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
      case 'get_profile': {
        try { conn.send({ type:'profile', profile: await fetchProfile(userId, conn.username) }); }
        catch (e) { conn.send({ type:'error', reason:'profile_fetch_failed' }); }
        break;
      }
      case 'update_profile': {
        try {
          const profile = await fetchProfile(userId, conn.username);
          const owned = new Set(profile.collection);
          const updated = await updateProfile(userId, msg, owned);
          if (updated.username) conn.username = updated.username;
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

server.listen(PORT, () => {
  console.log(`[arena] listening on :${PORT} (supabase ${HAS_SUPABASE ? 'ON' : 'OFF — guest mode'})`);
});
