'use strict';
/**
 * Arena of PESTS — authoritative game engine.
 *
 * This is a DOM-free, 1:1 port of the combat/gacha rules that used to live
 * (and run, trustingly, on the attacker's own machine) inside index.html.
 * Nothing in here touches a socket or a database — it's pure data in,
 * data + events out — so it can be unit tested and so server.js stays thin.
 *
 * Every function that used to reach into `document.getElementById(...)` to
 * play an animation now instead pushes a small serializable "event" onto an
 * `events` array that the caller returns to both clients. The client replays
 * those events through its existing Anim.* functions, so all the juice
 * (lunge, shake, floating numbers, status vfx, deaths) keeps working exactly
 * as before — it's just triggered by the server instead of trusted to it.
 *
 * ── COMBAT RULES v2 (creature-destruction win condition) ────────────────
 * There is no more player HP win condition. A side loses the instant it has
 * zero creatures left anywhere — not deployed, not in hand, not in deck.
 * `sides[i].hp/maxHp` still exist on the side object purely for UI/back-
 * compat (some support abilities still top it up cosmetically) but nothing
 * in the engine ever checks it to decide a match's outcome anymore.
 *
 * Death is permanent — a dead creature never returns to the deck — with one
 * exception: a card whose top effect is a `revive` ability (instead of an
 * `attack`) can be activated as that card's turn action to bring back any
 * one creature from its own side's graveyard, player's choice, at a
 * fraction of its max HP (see `executeRevive`). Like an attack, this uses up
 * that slot's action for the turn — no free/automatic revives of any kind.
 */

const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

/* ── CARD DATABASE ──────────────────────────────────────────────────
 * Loaded from cards.json — the single canonical copy of every card's
 * stats. The client fetches this exact file (see server.js's /cards.json
 * route) instead of keeping its own hardcoded copy, and the server
 * checks a hash of it during auth — so a modified/forked client can't
 * sneak a buffed "god card" past matchmaking: either it's playing with
 * the real numbers, or its hash won't match and it's refused. */
const CARD_LIBRARY_PATH = path.join(__dirname, 'cards.json');
const CARD_LIBRARY_RAW = fs.readFileSync(CARD_LIBRARY_PATH, 'utf8');
const CARD_LIBRARY_HASH = crypto.createHash('sha256').update(CARD_LIBRARY_RAW).digest('hex');
const CardDB = JSON.parse(CARD_LIBRARY_RAW);
const CardById = Object.fromEntries(CardDB.map(c => [c.id, c]));

/** ── CARD SETS ─────────────────────────────────────────────────────
 * A "set" is any group of CardDB entries sharing the same `set` string.
 * Building every single member of a set into your deck (weapon/defense
 * members included) grants +SET_STAT_BONUS max HP and +SET_STAT_BONUS
 * damage to that set's non-equipment members for the whole match —
 * equipment members unlock the set but never get the bonus themselves.
 * Derived once from CardDB at load, same pattern as CardById. */
const SET_STAT_BONUS = 150;
const CARD_SET_MEMBERS = {}; // setName -> [cardId, ...]
for (const c of CardDB) { if (c.set) (CARD_SET_MEMBERS[c.set] = CARD_SET_MEMBERS[c.set] || []).push(c.id); }

/** Mutates `cards` (live card instances just built for a deck) in place,
 * adding the set bonus to any card whose base definition belongs to a set
 * that's fully present among `ids` (the deck's card ids). Safe to call on
 * any deck — decks with no complete set are untouched. */
function applySetBonuses(cards, ids) {
  const idSet = new Set(ids);
  for (const [setName, memberIds] of Object.entries(CARD_SET_MEMBERS)) {
    if (!memberIds.every(id => idSet.has(id))) continue;
    for (const card of cards) {
      const base = CardById[card.baseId];
      if (!base || base.set !== setName) continue;
      if (card.cardType === 'weapon' || card.cardType === 'defense') continue; // required for the set, no bonus
      card.hp += SET_STAT_BONUS; card.maxHp += SET_STAT_BONUS; card.currentHp += SET_STAT_BONUS;
      if (card.bottomAttack) card.bottomAttack.damage = (card.bottomAttack.damage || 0) + SET_STAT_BONUS;
      if (card.topEffect && card.topEffect.type === 'attack') card.topEffect.value = (card.topEffect.value || 0) + SET_STAT_BONUS;
    }
  }
  return cards;
}

const RARITY_ORDER = ['common','uncommon','rare','epic','legendary','mythic'];
const rarityRank = r => RARITY_ORDER.indexOf(r);

/* ── PACK DEFINITIONS (verbatim from client) ──────────────────────── */
const PACK_DEFS = [
  {id:'basic',    currency:'gold', size:3, cost:80,
   weights:{common:74,uncommon:23,rare:3,epic:0,legendary:0,mythic:0}, guarantees:[], filter:null},
  {id:'standard', currency:'gold', size:5, cost:200,
   weights:{common:56,uncommon:27,rare:13,epic:3,legendary:0.8,mythic:0.2}, guarantees:[], filter:null},
  {id:'mob',      currency:'gold', size:4, cost:180,
   weights:{common:56,uncommon:27,rare:13,epic:3,legendary:0.8,mythic:0.2}, guarantees:[], filter:c=>c.types?.includes('mob')},
  {id:'dragon',   currency:'gold', size:4, cost:180,
   weights:{common:56,uncommon:27,rare:13,epic:3,legendary:0.8,mythic:0.2}, guarantees:[], filter:c=>c.types?.includes('dragon')},
  {id:'wizard',   currency:'gold', size:4, cost:180,
   weights:{common:56,uncommon:27,rare:13,epic:3,legendary:0.8,mythic:0.2}, guarantees:[], filter:c=>c.types?.includes('wizard')},
  {id:'armory',   currency:'gold', size:5, cost:220,
   weights:{common:56,uncommon:27,rare:13,epic:3,legendary:0.8,mythic:0.2}, guarantees:[], filter:c=>c.cardType==='weapon'||c.cardType==='defense'},
  {id:'boss',     currency:'gems', size:7, cost:150,
   weights:{common:28,uncommon:32,rare:26,epic:10,legendary:3,mythic:1}, guarantees:['rare'], filter:null},
  {id:'overlord', currency:'gems', size:7, cost:250,
   weights:{common:0,uncommon:14,rare:42,epic:30,legendary:10,mythic:4}, guarantees:['epic'], filter:null},
];
const PackById = Object.fromEntries(PACK_DEFS.map(p => [p.id, p]));

function rollRarityFromWeights(weights) {
  const total = Object.values(weights).reduce((s, w) => s + w, 0);
  if (total === 0) return 'common';
  let roll = Math.random() * total;
  for (const [key, w] of Object.entries(weights)) { roll -= w; if (roll <= 0) return key; }
  return 'common';
}

function pickCardOfRarity(rarity, exclude = [], typeFilter = null) {
  const base = typeFilter ? CardDB.filter(typeFilter) : CardDB;
  let pool = base.filter(c => c.rarity === rarity && !exclude.includes(c.id));
  if (!pool.length) {
    const startIdx = rarityRank(rarity);
    for (let i = startIdx - 1; i >= 0; i--) {
      pool = base.filter(c => c.rarity === RARITY_ORDER[i] && !exclude.includes(c.id));
      if (pool.length) break;
    }
  }
  if (!pool.length) pool = base.filter(c => !exclude.includes(c.id));
  if (!pool.length) pool = base.length ? base : CardDB;
  return pool[Math.floor(Math.random() * pool.length)];
}

function generatePackCards(packDef) {
  const { size, weights, guarantees, filter } = packDef;
  const cards = [], usedIds = [];
  guarantees.forEach(minRarity => {
    const eligible = RARITY_ORDER.filter(r => rarityRank(r) >= rarityRank(minRarity));
    const sub = {}; eligible.forEach(r => { if (weights[r]) sub[r] = weights[r]; });
    const r = rollRarityFromWeights(Object.keys(sub).length ? sub : { [minRarity]: 1 });
    const card = pickCardOfRarity(r, usedIds, filter);
    cards.push(card); usedIds.push(card.id);
  });
  for (let i = cards.length; i < size; i++) {
    const r = rollRarityFromWeights(weights);
    const card = pickCardOfRarity(r, usedIds, filter);
    cards.push(card); usedIds.push(card.id);
  }
  for (let i = cards.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [cards[i], cards[j]] = [cards[j], cards[i]];
  }
  return cards;
}

/** Opens a pack authoritatively. Returns {cards, cost, currency} or throws. */
function openPack(packId) {
  const def = PackById[packId];
  if (!def) throw new Error('unknown_pack');
  return { cards: generatePackCards(def), cost: def.cost, currency: def.currency };
}

/* ── EFFECT REGISTRY (verbatim rules, DOM calls stripped to events) ──
 * Each logic(card, ctx, effectInstance, match) mutates card.currentHp /
 * ctx.skipTurn / ctx.cancelAttack and may push a {type:'vfx', ...} event
 * onto ctx.events.
 *
 * `effectInstance.ampSide`/`ampKind` — when present — is how weapon/
 * defense amplification works: a weapon with `ampEffects: { burn: 25 }`
 * stamps which side's gear to keep re-checking onto every burn stack it
 * inflicts (see applyEffectToCard), so that stack ticks for 10+25=35 *as
 * long as* that side still has an amplifying weapon equipped — the
 * instant it breaks or gets swapped out, the very next tick reverts to
 * the effect's normal 10. Nothing about the bonus is baked in
 * permanently; see currentAmpValue/effectiveDmg/chanceFor below. A
 * defense's `dampEffects` is the mirror image, evaluated on the fly from
 * the *affected* card's own side rather than stamped onto the effect at
 * all: while that defense is equipped, it subtracts from any effect
 * currently ticking on/afflicting its owner's cards. */
const Effects = {
  bleed:        { trigger:'onTurnStart', dmg:10,  logic(c,x,ed,match){dot(c,x, effectiveDmg(match,x.side,ed,'bleed',10), 'bleed');} },
  poison:       { trigger:'onTurnStart', dmg:25,  logic(c,x,ed,match){dot(c,x, effectiveDmg(match,x.side,ed,'poison',25), 'poison');} },
  strongPoison: { trigger:'onTurnStart', dmg:50,  logic(c,x,ed,match){dot(c,x, effectiveDmg(match,x.side,ed,'strongPoison',50), 'strongPoison');} },
  mythicPoison: { trigger:'onTurnStart', dmg:75,  logic(c,x,ed,match){dot(c,x, effectiveDmg(match,x.side,ed,'mythicPoison',75), 'mythicPoison');} },
  curse:        { trigger:'onTurnStart', logic(){} },
  confusion:    { trigger:'onAttack',    logic(c,x){ if (Math.random()<.5) x.cancelAttack = true; } }, // its amp/damp-aware miss roll lives inline in performHit, alongside shock/soak — see missChanceFor
  sleep:        { trigger:'onTurnStart', logic(c,x,ed,match){ rollSkip(c,x,'sleep',ed,match); } },
  paralyze:     { trigger:'onTurnStart', logic(c,x,ed,match){ rollSkip(c,x,'paralyze',ed,match); } },
  burn:         { trigger:'onTurnStart', dmg:10,  logic(c,x,ed,match){dot(c,x, effectiveDmg(match,x.side,ed,'burn',10), 'burn');} },
  shock:        { trigger:'onTurnStart', dmg:25,  logic(c,x,ed,match){dot(c,x, effectiveDmg(match,x.side,ed,'shock',25), 'shock');} },
  soak:         { trigger:'onTurnStart', logic(){} },
  cryo:         { trigger:'onTurnStart', logic(c,x,ed,match){ dot(c,x, effectiveDmg(match,x.side,ed,'cryo',10), 'cryo'); rollSkip(c,x,'cryo',ed,match); } },
  rocks:        { trigger:'onSwap',      logic(){} }, // no per-turn logic — see applyRocksOnSwap; this fires on swap, not on turn start
};
function dot(card, ctx, dmg, type) {
  card.currentHp -= dmg;
  ctx.events.push({ t:'dot', side: ctx.side, slot: ctx.slot, card:card.instanceId, dmg, effect:type });
}
function rollSkip(card, ctx, type, ed, match) {
  const chance = chanceFor(match, ctx.side, ed, type, .5);
  if (Math.random() < chance) {
    ctx.skipTurn = true;
    ctx.events.push({ t:'status', side: ctx.side, slot: ctx.slot, card:card.instanceId, effect:type, hit:true });
  } else {
    ctx.events.push({ t:'status', side: ctx.side, slot: ctx.slot, card:card.instanceId, effect:type, hit:false });
  }
}
function hasEffect(card, type) { return !!card && card.activeEffects.some(e => e.type === type); }

/* ── WEAPON/DEFENSE EFFECT AMPLIFICATION & DAMPENING (live, additive) ──
 * Two independent, symmetric bonuses stack additively onto an effect's
 * baseline every time it's checked — never baked into the stack at the
 * moment it was inflicted, so either one turns on/off immediately as gear
 * gets equipped, broken, or swapped:
 *
 *  • AMPLIFICATION (ampEffects, on a weapon OR a defense's addEffects):
 *    the INFLICTING side's currently-equipped gear adds a bonus to any
 *    effect it causes — stamped as `ampSide`/`ampKind` onto the effect
 *    instance at infliction time (applyEffectToCard), then re-checked
 *    live via currentAmpValue() against whatever that side has equipped
 *    *right now*, which may or may not still be the same item.
 *
 *  • DAMPENING (dampEffects, defense only): the AFFECTED side's own
 *    currently-equipped defense subtracts from any effect currently
 *    afflicting their cards, regardless of who inflicted it or whether it
 *    was itself amplified. Needs no stamping at all — every tick already
 *    knows which side owns the affected card (ctx.side), so it's just a
 *    live lookup of that side's own gear via currentDampValue().
 *
 * Both are additive and stack with each other: a +25 amp from the
 * attacker's weapon and a -10 damp from the defender's own defense nets
 * out to +15 over baseline, and the total damage/chance never goes below
 * zero. Either one disappears the instant its gear isn't active anymore. */
function currentAmpValue(match, ed, type) {
  if (!match || !ed || ed.ampSide == null || !ed.ampKind) return undefined;
  const gearSide = match.sides[ed.ampSide];
  const gear = ed.ampKind === 'weapon' ? (gearSide && gearSide.weaponCard) : (gearSide && gearSide.defenseCard);
  return gear && gear.ampEffects && gear.ampEffects[type] != null ? gear.ampEffects[type] : undefined;
}
function currentDampValue(match, side, type) {
  if (!match || side == null) return undefined;
  const gearSide = match.sides[side];
  const gear = gearSide && gearSide.defenseCard;
  return gear && gear.dampEffects && gear.dampEffects[type] != null ? gear.dampEffects[type] : undefined;
}
/** Damage-type effects (bleed, poison, burn, shock, cryo, ...): amp adds
 * to the effect's baseline tick damage, damp subtracts from it — e.g.
 * baseline 10, ampEffects:{burn:25} while equipped -> 35, and if the
 * affected side's own defense also carries dampEffects:{burn:5} while
 * equipped -> 30. Never drops below 0. `side` is whichever side owns the
 * card the effect is ticking on (i.e. the potential dampener). */
function effectiveDmg(match, side, ed, type, baseDmg) {
  const amp = currentAmpValue(match, ed, type);
  const damp = currentDampValue(match, side, type);
  let dmg = baseDmg;
  if (amp != null) dmg += amp;
  if (damp != null) dmg -= damp;
  return Math.max(0, dmg);
}
/** Chance-type effects (sleep, paralyze, confusion, shock/soak's miss
 * roll, ...): ampEffects/dampEffects values are 0–100 percentage POINTS
 * added to/subtracted from the effect's baseline chance — e.g. baseline
 * 50%, ampEffects:{paralyze:25} while equipped -> 75%, and if the
 * affected side's defense carries dampEffects:{paralyze:20} while
 * equipped -> 55%. Clamped to the 0–100% range either way. */
function chanceFor(match, side, ed, type, baseChance) {
  const amp = currentAmpValue(match, ed, type);
  const damp = currentDampValue(match, side, type);
  let pct = baseChance * 100;
  if (amp != null) pct += amp;
  if (damp != null) pct -= damp;
  return Math.max(0, Math.min(100, pct)) / 100;
}
/** Same idea as chanceFor, but for the inline onAttack-time miss checks
 * (confusion/shock/soak causing the AFFECTED card to whiff its own
 * attack) in performHit, which look the effect instance up by type
 * instead of already holding a reference to it. `side` here is whichever
 * side owns `card` (the one possibly missing its attack) — the same side
 * whose defense would be doing any dampening. */
function missChanceFor(card, type, match, side, baseChance) {
  const ed = card.activeEffects.find(e => e.type === type);
  return chanceFor(match, side, ed, type, baseChance);
}

/* ── SYNERGY PASSIVE ──────────────────────────────────────────────────
 * topEffect.synergy = {
 *   // legacy 2-card form — the only form allowed to use shareAttack:
 *   partnerId: 'other_card_base_id',
 *   bonusHp: 20, bonusDamage: 10, shareAttack: true,
 *
 *   // multi-card form — stat buffs ONLY, no attack sharing. Lists any
 *   // number of partner base ids; bonusHp/bonusDamage are PER PARTNER
 *   // actually present in the deck, so a 3-card ring where every card
 *   // lists the other two scales up to 2x the listed bonus when the
 *   // whole trio is together, and still gives a partial bonus if only
 *   // one of the two partners made the deck.
 *   partnerIds: ['ally_a', 'ally_b', 'ally_c'],
 *   bonusHp: 10, bonusDamage: 5,
 * }
 * Applied once, at deck-build time, over every card instance on a side (deck+hand
 * combined) — so it reflects deck *composition*, not what's currently drawn/deployed. */
function applySynergies(cardInstances) {
  cardInstances.forEach(card => {
    if (!card || card.cardType) return; // skip weapon/defense equipment
    const syn = card.topEffect && card.topEffect.type === 'passive' && card.topEffect.synergy;
    if (!syn) return;

    // Legacy single-partner form — the only one that may share an attack.
    if (syn.partnerId) {
      const partner = cardInstances.find(c => c && !c.cardType && c.baseId === syn.partnerId && c !== card);
      if (partner) {
        if (syn.bonusHp) { card.maxHp += syn.bonusHp; card.currentHp += syn.bonusHp; }
        if (syn.bonusDamage) { card.synergyDamageBonus = (card.synergyDamageBonus || 0) + syn.bonusDamage; }
        if (syn.shareAttack) {
          const ba = partner.bottomAttack;
          card.topEffect = {
            type: 'attack', name: `${ba.name} (shared)`, value: ba.damage, element: ba.element,
            effects: ba.effects || [], heal: ba.heal, healTarget: ba.healTarget, multiAttack: ba.multiAttack,
            description: `Shared from ${partner.name}: ${ba.name}.`,
          };
        }
        card.synergyPartnerInstanceId = partner.instanceId; // informational, for UI display
      }
    }

    // Multi-card group form — stat buffs only, scales with # of partners present.
    if (Array.isArray(syn.partnerIds) && syn.partnerIds.length) {
      const matched = [];
      syn.partnerIds.forEach(pid => {
        const p = cardInstances.find(c => c && !c.cardType && c.baseId === pid && c !== card && !matched.includes(c));
        if (p) matched.push(p);
      });
      if (matched.length) {
        if (syn.bonusHp) {
          const add = syn.bonusHp * matched.length;
          card.maxHp += add; card.currentHp += add;
        }
        if (syn.bonusDamage) {
          card.synergyDamageBonus = (card.synergyDamageBonus || 0) + syn.bonusDamage * matched.length;
        }
        card.synergyGroupPartnerInstanceIds = matched.map(p => p.instanceId);
      }
    }
  });
}

/** True if `target`'s own passive grants immunity to `effectType`. A passive
 * can list one or more immunities alongside its other fields (flat/percent
 * damage reduction, synergy, etc.) — e.g. `{type:'passive', passiveReduction:
 * {flat:10}, immuneEffects:['burn']}` reduces incoming damage by 10 AND
 * shrugs off burn entirely. Immunity is checked at the moment an effect
 * would be applied, so it blocks new stacks and duration extensions alike;
 * it does nothing to a stack the card was already carrying before the
 * passive existed (there's no such case in practice — a card's passive
 * never changes mid-match). */
function isImmuneTo(target, effectType) {
  const te = target && target.topEffect;
  return !!(te && te.type === 'passive' && Array.isArray(te.immuneEffects) && te.immuneEffects.includes(effectType));
}

/** Applies a status effect to `target`. `ampSource` — when given, as
 * `{side, kind:'weapon'|'defense'}` — is the equipped gear (if any) that
 * was active on the inflicting side at the moment of this attack; it gets
 * stamped onto the effect instance so future ticks/rolls can dynamically
 * re-check whether that gear (or another one like it) is still active —
 * see currentAmpValue. If the target already has a stack of this type,
 * duration extends and the amp source is refreshed to whatever's active
 * right now (re-applying the same effect while amplifying gear is
 * equipped should start amplifying it, same as a fresh stack would).
 * Returns true if the effect was applied, false if the target's passive
 * immunity blocked it entirely (nothing is mutated in that case). */
function applyEffectToCard(target, effectDef, ampSource) {
  const eDef = Effects[effectDef.type]; if (!eDef) return false;
  if (isImmuneTo(target, effectDef.type)) return false;
  const ex = target.activeEffects.find(e => e.type === effectDef.type);
  if (ex) {
    if (effectDef.duration < 9999) ex.duration = Math.min(9999, ex.duration + effectDef.duration);
    if (ampSource) { ex.ampSide = ampSource.side; ex.ampKind = ampSource.kind; }
  } else {
    target.activeEffects.push({
      type: effectDef.type, duration: effectDef.duration,
      ampSide: ampSource ? ampSource.side : undefined,
      ampKind: ampSource ? ampSource.kind : undefined,
    });
  }
  return true;
}

/** `side` (0|1) is whichever side owns `entity` — stamped onto every event so
 * a client on either side of the match can map it back to its own DOM
 * (its own board is always "player-*", the opponent's is always "enemy-*").
 * `match` is threaded through to each effect's logic() so DOT ticks and
 * skip-turn rolls can dynamically re-check weapon/defense amplification
 * (see currentAmpValue) instead of using a value frozen at infliction. */
function processEffects(match, entity, trigger, ctx, side) {
  [['activeCard','slot1'], ['activeCard2','slot2']].forEach(([key, slotKey]) => {
    const card = entity[key];
    if (!card) return;
    for (let i = card.activeEffects.length - 1; i >= 0; i--) {
      const ed = card.activeEffects[i]; const eDef = Effects[ed.type];
      if (!eDef || eDef.trigger !== trigger) continue;
      ctx.side = side; ctx.slot = slotKey;
      eDef.logic(card, ctx, ed, match);
      if (trigger === 'onTurnStart' && ed.duration < 9999) {
        ed.duration--; if (ed.duration <= 0) card.activeEffects.splice(i, 1);
      }
    }
  });
}

/** Kills whatever creature is sitting in `match.sides[side][slotKey]`:
 * removes it from the active slot and pushes it onto that side's graveyard,
 * permanently, unless and until that side spends a `revive` ability's turn
 * action to bring it back (see `executeRevive`). There is no automatic or
 * banked revive of any kind — every revival is a deliberate player choice
 * that costs a card's action, same as an attack would. */
function killCard(match, side, slotKey, events) {
  const entity = match.sides[side];
  const card = cardInSlot(entity, slotKey);
  if (!card) return;
  events.push({ t:'death', side, slot: slotKey, card:card.instanceId, name:card.name });
  if (entity.activeCard === card) entity.activeCard = null;
  else if (entity.activeCard2 === card) entity.activeCard2 = null;
  entity.graveyard.push(card);
}

/** Activates a `revive` top-effect ability as `slotKey`'s action for the
 * turn: the acting card must be alive, unacted-this-turn, and have
 * `topEffect.type === 'revive'`; `deadInstanceId` must name a creature
 * currently in this side's graveyard (the caller's choice — earliest,
 * latest, whichever they want). The revived creature returns to hand at
 * `topEffect.healPercent` of its max HP (50% if unspecified) with all
 * lingering statuses cleared, and this consumes the acting card's turn
 * exactly like an attack would. */
function executeRevive(match, side, slotKey, deadInstanceId) {
  const entity = match.sides[side];
  const events = [];
  if (match.actedThisTurn[side].has(slotKey)) return { ok:false, reason:'already_acted', events };
  const actingCard = cardInSlot(entity, slotKey);
  if (!actingCard) return { ok:false, reason:'no_card_in_slot', events };
  if (!actingCard.topEffect || actingCard.topEffect.type !== 'revive') return { ok:false, reason:'no_revive_ability', events };

  const idx = entity.graveyard.findIndex(c => c.instanceId === deadInstanceId);
  if (idx === -1) return { ok:false, reason:'invalid_target', events };
  const [card] = entity.graveyard.splice(idx, 1);

  const healPercent = actingCard.topEffect.healPercent != null ? actingCard.topEffect.healPercent : 0.5;
  card.currentHp = Math.max(1, Math.round(card.maxHp * healPercent));
  card.activeEffects = [];
  entity.hand.push(card);
  events.push({ t:'revive', side, card: card.instanceId, name: card.name, hp: card.currentHp, maxHp: card.maxHp, via: actingCard.instanceId });

  match.actedThisTurn[side].add(slotKey);
  return { ok:true, events };
}

/** Checks both of `side`'s active slots for a creature at <=0 HP (e.g. after
 * onTurnStart DOT ticks, or a rocks hit on swap) and kills it. */
function checkCardDeath(match, side, events) {
  const entity = match.sides[side];
  ['slot1','slot2'].forEach(slotKey => {
    const c = cardInSlot(entity, slotKey);
    if (c && c.currentHp <= 0) killCard(match, side, slotKey, events);
  });
}

/** Effect durations keep ticking down even on a benched (in-hand) card —
 * swapping a card out doesn't pause its clock. Only an ACTIVE card runs an
 * effect's actual per-turn logic (damage ticks, skip-turn rolls, etc. — see
 * processEffects); a benched card just counts duration down toward zero and
 * has expired effects removed, same as it would while deployed. This is
 * what lets e.g. a 'rocks' stack on a card sitting in your hand still run
 * out naturally instead of freezing the moment you bench it. */
function decayHandEffects(entity, events, side) {
  entity.hand.forEach(card => {
    if (!card || card.cardType) return; // equipment doesn't carry status effects
    for (let i = card.activeEffects.length - 1; i >= 0; i--) {
      const ed = card.activeEffects[i];
      if (ed.duration >= 9999) continue; // permanent (e.g. an innate passive-granted effect)
      ed.duration--;
      if (ed.duration <= 0) {
        card.activeEffects.splice(i, 1);
        events.push({ t:'effect_expire', side, card: card.instanceId, effect: ed.type });
      }
    }
  });
}

/* ── CARD FACTORY ──────────────────────────────────────────────────── */
function createCard(baseId) {
  const base = CardById[baseId]; if (!base) return null;
  if (base.cardType === 'weapon' || base.cardType === 'defense') {
    return {
      instanceId: crypto.randomUUID(), baseId, name: base.name, cardType: base.cardType,
      flatBonus: base.flatBonus, maxDurability: base.maxDurability,
      currentDurability: base.maxDurability, image: base.image,
      // Optional customization — see performHit/currentAmpValue/currentDampValue:
      ampEffects: base.ampEffects ? { ...base.ampEffects } : null,
      dampEffects: base.dampEffects ? { ...base.dampEffects } : null,
      addEffects: base.addEffects ? base.addEffects.map(e => ({ ...e })) : null,
    };
  }
  const card = {
    instanceId: crypto.randomUUID(), baseId, name: base.name, hp: base.hp, types: [...base.types],
    classification: base.classification, image: base.image,
    topEffect: JSON.parse(JSON.stringify(base.topEffect)),
    bottomAttack: JSON.parse(JSON.stringify(base.bottomAttack)),
    maxHp: base.hp, currentHp: base.hp, activeEffects: [],
  };
  if (base.topEffect.type === 'passive' && base.topEffect.effects.length > 0) {
    base.topEffect.effects.forEach(e => card.activeEffects.push({ type: e.type, duration: e.duration }));
  }
  return card;
}

/** on-deploy ability hook: existing "apply effects to enemy active card" behavior,
 * plus an optional heal ('self' | 'ally' | 'side') for support-style cards. */
function applyDeployAbility(sides, side, card, events) {
  if (card.topEffect?.type !== 'ability') return;
  const otherSide = side === 0 ? 1 : 0;
  if (card.topEffect.effects && card.topEffect.effects.length) {
    const opp = sides[otherSide];
    const target = opp.activeCard || opp.activeCard2;
    if (target) {
      card.topEffect.effects.forEach(eff => {
        const applied = applyEffectToCard(target, eff);
        if (!applied) events.push({ t:'immune', side: otherSide, card: target.instanceId, effect: eff.type });
      });
      events.push({ t:'ability', card: card.instanceId, target: target.instanceId });
    }
  }
  if (card.topEffect.heal) {
    const own = sides[side];
    const h = card.topEffect.heal;
    const amount = h.amount || 0;
    if (h.target === 'side') {
      own.hp = Math.min(own.maxHp, own.hp + amount);
      events.push({ t:'heal', side, slot:null, card:card.instanceId, targetCard:null, amount, target:'side' });
    } else if (h.target === 'ally') {
      const ally = own.activeCard === card ? own.activeCard2 : own.activeCard;
      if (ally) {
        const before = ally.currentHp;
        ally.currentHp = Math.min(ally.maxHp, ally.currentHp + amount);
        events.push({ t:'heal', side, card:card.instanceId, targetCard:ally.instanceId, amount: ally.currentHp - before, target:'ally' });
      }
    } else {
      const before = card.currentHp;
      card.currentHp = Math.min(card.maxHp, card.currentHp + amount);
      events.push({ t:'heal', side, card:card.instanceId, targetCard:card.instanceId, amount: card.currentHp - before, target:'self' });
    }
  }
}

const DECK_SIZE = 16;
/** Creatures (wizard/mob/dragon cards — anything without a cardType) are
 * capped at 12 per deck; the remaining slots (down to DECK_SIZE) must be
 * weapon/defense equipment. */
const MAX_CREATURES = 12;

/** A deck-legality check reused by generateDeck's fallback path and by
 * isDeckLegal below, so the random "your deck was invalid" deck the server
 * hands out never itself breaks the rule it's enforcing. */
function deckClassificationOk(defs) {
  const creatureDefs = defs.filter(d => !d.cardType);
  if (creatureDefs.length < 1) return false;
  if (creatureDefs.length > MAX_CREATURES) return false;
  const classes = creatureDefs.map(d => d.classification).filter(Boolean);
  const bossOrOverlordCount = classes.filter(c => c === 'boss' || c === 'overlord').length;
  if (bossOrOverlordCount > 1) return false;
  if (classes.includes('overlord') && classes.some(c => c === 'pests' || c === 'boss')) return false;
  return true;
}

function generateDeck(n) {
  const normals = CardDB.filter(c => !c.cardType && c.classification === 'pests');
  const bosses = CardDB.filter(c => !c.cardType && c.classification === 'boss');
  const overlords = CardDB.filter(c => !c.cardType && c.classification === 'overlord');
  const equipment = CardDB.filter(c => c.cardType === 'weapon' || c.cardType === 'defense');
  const pick = pool => pool[Math.floor(Math.random() * pool.length)];

  const defs = [];
  let creatureCount = 0;
  const addCreature = def => { defs.push(def); creatureCount++; };

  // Rare chance of an Overlord deck — if so, everything else must be Normal/equipment.
  if (overlords.length && Math.random() < 0.08) {
    addCreature(pick(overlords));
    while (defs.length < n) {
      const canAddCreature = creatureCount < MAX_CREATURES && normals.length;
      if (equipment.length && (!canAddCreature || Math.random() < 0.6)) defs.push(pick(equipment));
      else if (canAddCreature) addCreature(pick(normals));
      else if (equipment.length) defs.push(pick(equipment));
      else break;
    }
    return defs.slice(0, n).map(d => createCard(d.id)).filter(Boolean);
  }
  // Otherwise, at most one Boss, rest Normal/equipment — creatures capped at MAX_CREATURES.
  if (bosses.length && Math.random() < 0.35) addCreature(pick(bosses));
  while (defs.length < n) {
    const canAddCreature = creatureCount < MAX_CREATURES && normals.length;
    const useEquip = equipment.length > 0 && (!canAddCreature || Math.random() < 0.3);
    if (useEquip) defs.push(pick(equipment));
    else if (canAddCreature) addCreature(pick(normals));
    else if (equipment.length) defs.push(pick(equipment));
    else break;
  }
  return defs.slice(0, n).map(d => createCard(d.id)).filter(Boolean);
}

/** Builds a validated deck of live card instances from a list of owned card ids. */
/** A deck is legal if it's exactly DECK_SIZE cards, every id exists in the
 * canonical library, no more than MAX_CREATURES of them are creatures, at
 * most one BOSS-or-OVERLORD creature is present, and — if that one card is
 * an OVERLORD — no PESTS or BOSS creatures ride along with it. Checked
 * fresh every time a match is built, not just once when the deck was saved,
 * so a stale/tampered deck never quietly slips through. */
function isDeckLegal(ids) {
  if (!Array.isArray(ids) || ids.length !== DECK_SIZE) return false;
  if (!ids.every(id => !!CardById[id])) return false;
  return deckClassificationOk(ids.map(id => CardById[id]));
}

function buildDeckFromIds(ids) {
  const cards = isDeckLegal(ids) ? ids.map(id => createCard(id)) : generateDeck(DECK_SIZE);
  // Derive the id list from the actual cards built rather than trusting the
  // input `ids` — covers the generateDeck(DECK_SIZE) fallback path too, so a
  // lucky random bot deck gets its set bonus exactly like a real one would.
  return applySetBonuses(cards, cards.map(c => c.baseId));
}

/* ── PLAYER SIDE FACTORY ──────────────────────────────────────────── */
function freshSide(deck) {
  const d = [...deck];
  applySynergies(d); // deck+hand together — synergy is about composition, not what's drawn yet
  return {
    hp: 100, maxHp: 100, // cosmetic only — see module doc; never decides the match anymore
    activeCard: null, activeCard2: null, weaponCard: null, defenseCard: null,
    deck: d, hand: d.splice(0, 4),
    graveyard: [], // permanently-dead creatures, until/unless a revive ability brings one back
  };
}

/* ── COMBAT ───────────────────────────────────────────────────────── */
// slotKey: 'slot1' | 'slot2'
function cardInSlot(entity, slotKey) { return slotKey === 'slot1' ? entity.activeCard : entity.activeCard2; }
function slotOfCard(entity, card) { return entity.activeCard === card ? 'slot1' : 'slot2'; }

/** Total creatures a side has left anywhere — deployed, in hand, or still in
 * deck. This, and only this, decides the match now: hit zero and you lose. */
function aliveCreatureCount(side) {
  const deckC = side.deck.filter(c => !c.cardType).length;
  const handC = side.hand.filter(c => !c.cardType).length;
  const activeC = (side.activeCard ? 1 : 0) + (side.activeCard2 ? 1 : 0);
  return deckC + handC + activeC;
}

/**
 * Resolves which attack definition `chosenAttackIndex` refers to.
 * 0 = top attack (only valid if topEffect.type === 'attack' — which is either how
 *     the card was authored, or what its shareAttack synergy turned it into)
 * 1 = bottom attack (always available)
 * Returns null if the index isn't usable right now.
 */
function attackDefFor(atkEntity, card, atkIndex) {
  if (atkIndex !== 0 && atkIndex !== 1) return null;
  const src = atkIndex === 0 ? card.topEffect : card.bottomAttack;
  if (atkIndex === 0 && src.type !== 'attack') return null;
  return { name: src.name, damage: src.value != null ? src.value : src.damage, effects: src.effects || [], element: src.element, heal: src.heal, healTarget: src.healTarget, multiAttack: src.multiAttack };
}

/** A heal attack/attack-ability heals instead of dealing damage. `healTarget` is
 * 'self' (default), 'ally' (the other slot on the same side), or 'side' (player hp pool). */
function performHeal(atkEntity, atkSlotKey, ac, atkDef, events, side) {
  const amount = atkDef.damage || 0;
  if (atkDef.healTarget === 'side') {
    atkEntity.hp = Math.min(atkEntity.maxHp, atkEntity.hp + amount);
    events.push({ t:'heal', side, slot:null, card:ac.instanceId, targetCard:null, amount, target:'side', name:atkDef.name });
    return { stop:false };
  }
  let targetCard = ac;
  if (atkDef.healTarget === 'ally') {
    targetCard = atkSlotKey === 'slot1' ? atkEntity.activeCard2 : atkEntity.activeCard;
  }
  if (!targetCard) { events.push({ t:'heal_fizzle', side, slot:atkSlotKey, card:ac.instanceId }); return { stop:false }; }
  const before = targetCard.currentHp;
  targetCard.currentHp = Math.min(targetCard.maxHp, targetCard.currentHp + amount);
  events.push({ t:'heal', side, slot:atkSlotKey, card:ac.instanceId, targetCard:targetCard.instanceId, amount: targetCard.currentHp - before, target: atkDef.healTarget || 'self', name:atkDef.name });
  return { stop:false };
}

/** Resolves a single swing of an attack (damage or heal), including weapon/defense
 * durability, elemental passive reduction, curse recoil, and revive-bank kill checks.
 * Returns { stop:true } when the attacker or (non-revived) target died — signalling
 * a multi-attack sequence should not continue.
 *
 * Weapon/defense customization:
 *  - weapon.ampEffects / defense.ampEffects: { effectType: bonus } — while this
 *    piece of equipment is the one landing/absorbing the hit, any matching status
 *    effect it applies ticks for its normal baseline PLUS this bonus (e.g. a weapon
 *    with ampEffects:{burn:25} makes its burn deal 10+25=35/turn instead of 10), for
 *    as long as that equipment stays active — see currentAmpValue/effectiveDmg.
 *  - defense.dampEffects: { effectType: reduction } — the mirror image: while this
 *    defense is equipped, it subtracts from any effect currently afflicting its
 *    owner's cards, regardless of who inflicted it or whether it was itself
 *    amplified — see currentDampValue. Amp and damp stack additively and the
 *    combined result never drops below the effect's normal floor (0 dmg / 0% chance).
 *  - weapon.addEffects / defense.addEffects: [{type, duration}, ...] — extra status
 *    effects applied on every hit this equipment participates in, independent of
 *    whatever the attack itself already applies. A weapon's addEffects land on the
 *    target being hit; a defense's addEffects land back on the attacker (thorns). */
function performHit(match, side, atkSlotKey, targetSlotKey, atkDef, ac, atkEntity, defEntity, defSide, events) {
  if (hasEffect(ac, 'confusion') && Math.random() < missChanceFor(ac,'confusion',match,side,.5)) { events.push({t:'miss',side,slot:atkSlotKey,card:ac.instanceId,cause:'confusion'}); return { stop:false }; }
  if (hasEffect(ac, 'shock') && Math.random() < missChanceFor(ac,'shock',match,side,.5))         { events.push({t:'miss',side,slot:atkSlotKey,card:ac.instanceId,cause:'shock'});     return { stop:false }; }
  if (hasEffect(ac, 'soak') && Math.random() < missChanceFor(ac,'soak',match,side,.5))           { events.push({t:'miss',side,slot:atkSlotKey,card:ac.instanceId,cause:'soak'});      return { stop:false }; }

  if (atkDef.heal) return performHeal(atkEntity, atkSlotKey, ac, atkDef, events, side);

  const targetCard = targetSlotKey ? cardInSlot(defEntity, targetSlotKey) : null;

  let wBonus = 0, weaponRef = null;
  if (atkEntity.weaponCard) {
    weaponRef = atkEntity.weaponCard;
    wBonus = weaponRef.flatBonus; weaponRef.currentDurability--;
    events.push({ t:'weapon_use', side, bonus:wBonus, breaks: weaponRef.currentDurability<=0 });
    if (weaponRef.currentDurability <= 0) atkEntity.weaponCard = null;
  }
  let dReduce = 0, defenseRef = null;
  if (defEntity.defenseCard) {
    defenseRef = defEntity.defenseCard;
    dReduce = defenseRef.flatBonus; defenseRef.currentDurability--;
    events.push({ t:'defense_use', side:defSide, bonus:dReduce, breaks: defenseRef.currentDurability<=0 });
    if (defenseRef.currentDurability <= 0) defEntity.defenseCard = null;
  }

  let dmg = atkDef.damage + wBonus + (ac.synergyDamageBonus || 0);
  if (hasEffect(ac, 'burn')) { dmg = Math.floor(dmg * .5); events.push({t:'burn_penalty', side, slot:atkSlotKey}); }

  if (!targetCard) {
    // No player HP anymore — a "direct hit" into an empty slot is a no-op,
    // kept only so the client's existing miss/whiff animation still fires.
    events.push({ t:'hit', atkSide:side, atkSlot:atkSlotKey, atkCard:ac.instanceId, defSide, defSlot:null, tgtCard:null, direct:true, dmg:0, name:atkDef.name, element:atkDef.element });
    return { stop:false };
  }

  const tgtSlotKey = slotOfCard(defEntity, targetCard);

  if (hasEffect(targetCard, 'soak')) { dmg = Math.floor(dmg * .5); events.push({t:'soak_reduce', side:defSide, slot:tgtSlotKey}); }
  const pr = targetCard.topEffect && targetCard.topEffect.type === 'passive' ? targetCard.topEffect.passiveReduction : null;
  if (pr) {
    const bypassed = pr.exceptElements && pr.exceptElements.includes(atkDef.element);
    if (!bypassed) {
      if (pr.percent) dmg = Math.floor(dmg * (1 - pr.percent));
      if (pr.flat) dmg = Math.max(0, dmg - pr.flat);
    }
  }
  dmg = Math.max(0, dmg - dReduce);
  targetCard.currentHp -= dmg;
  events.push({ t:'hit', atkSide:side, atkSlot:atkSlotKey, atkCard:ac.instanceId, defSide, defSlot:tgtSlotKey, tgtCard:targetCard.instanceId, direct:false, dmg, name:atkDef.name, element:atkDef.element });

  const weaponAmp = weaponRef ? { side, kind:'weapon' } : null;
  (atkDef.effects || []).forEach(eff => {
    const applied = applyEffectToCard(targetCard, eff, weaponAmp);
    if (!applied) events.push({ t:'immune', side: defSide, card: targetCard.instanceId, effect: eff.type });
  });
  if (weaponRef && weaponRef.addEffects && weaponRef.addEffects.length) {
    weaponRef.addEffects.forEach(eff => {
      const applied = applyEffectToCard(targetCard, eff, weaponAmp);
      if (!applied) events.push({ t:'immune', side: defSide, card: targetCard.instanceId, effect: eff.type });
    });
    events.push({ t:'weapon_effect', side, slot:atkSlotKey, weapon:weaponRef.baseId, target:targetCard.instanceId });
  }
  if (defenseRef && defenseRef.addEffects && defenseRef.addEffects.length) {
    const defenseAmp = { side: defSide, kind:'defense' };
    defenseRef.addEffects.forEach(eff => {
      const applied = applyEffectToCard(ac, eff, defenseAmp);
      if (!applied) events.push({ t:'immune', side, card: ac.instanceId, effect: eff.type });
    });
    events.push({ t:'defense_effect', side:defSide, slot:tgtSlotKey, defense:defenseRef.baseId, target:ac.instanceId });
  }

  if (hasEffect(targetCard, 'curse')) {
    const r = Math.floor(dmg * .25); ac.currentHp -= r;
    events.push({ t:'curse_recoil', side, slot:atkSlotKey, card:ac.instanceId, dmg:r });
    if (ac.currentHp <= 0) {
      killCard(match, side, atkSlotKey, events);
      return { stop:true };
    }
  }
  if (targetCard.currentHp <= 0) {
    killCard(match, defSide, tgtSlotKey, events);
    return { stop:true };
  }
  return { stop:false };
}

/**
 * Executes one attack activation. `chosenAttackIndex` is 0 (top) or 1 (bottom) —
 * always an explicit player/opponent choice, never randomized, so this function is
 * used for both human turns and (with a server-side random index) AI/bot turns.
 *
 * If the chosen attack itself has a multiAttack config, this may resolve more than
 * one swing — it's part of the attack, not the card, so a card can have one attack
 * that always hits once and another that hits multiple times:
 *   multiAttack: {
 *     guaranteed: 2,        // always swings this many times, no rolling
 *     chance: 0.3,          // OR: chance to get another swing after each one lands
 *     maxExtra: 1,          // cap on how many bonus swings `chance` can grant (default 1)
 *   }
 * Mutates `match` in place and returns the list of events produced.
 */
function executeAttack(match, side, atkSlotKey, targetSlotKey, chosenAttackIndex) {
  const events = [];
  const atkEntity = match.sides[side];
  const defSide = side === 0 ? 1 : 0;
  const defEntity = match.sides[defSide];
  const ac = cardInSlot(atkEntity, atkSlotKey);
  if (!ac) return { ok:false, reason:'no_card_in_slot', events };
  if (match.actedThisTurn[side].has(atkSlotKey)) return { ok:false, reason:'already_acted', events };

  const targetCard = targetSlotKey ? cardInSlot(defEntity, targetSlotKey) : null;
  if (targetSlotKey && !targetCard) return { ok:false, reason:'no_target', events };

  const atkDef = attackDefFor(atkEntity, ac, chosenAttackIndex);
  if (!atkDef) return { ok:false, reason:'invalid_attack', events };

  const multi = atkDef.multiAttack;
  let swings = (multi && multi.guaranteed) ? multi.guaranteed : 1;
  const maxExtra = (multi && !multi.guaranteed && multi.chance) ? (multi.maxExtra != null ? multi.maxExtra : 1) : 0;
  let extrasUsed = 0;

  let i = 0;
  while (i < swings) {
    const result = performHit(match, side, atkSlotKey, targetSlotKey, atkDef, ac, atkEntity, defEntity, defSide, events);
    i++;
    if (result.stop) break;
    if (maxExtra > 0 && extrasUsed < maxExtra && i === swings) {
      if (Math.random() < multi.chance) {
        swings++; extrasUsed++;
        events.push({ t:'multi_attack', side, slot:atkSlotKey, card:ac.instanceId, swing:swings });
      }
    }
  }
  return finishAttack(match, side, atkSlotKey, events);
}
function finishAttack(match, side, atkSlotKey, events) {
  match.actedThisTurn[side].add(atkSlotKey);
  return { ok:true, events };
}

/** 'rocks' deals 50 damage to a card carrying the effect every time it swaps
 * into OR out of an active slot — not a trap on the opposing card like it
 * used to be, but a hazard on the card itself. The effect isn't consumed by
 * triggering: it keeps ticking down via the normal per-turn decay (see
 * processEffects/decayHandEffects) and can fire again on every subsequent
 * swap for as long as it's still active, in or out of hand.
 * `slotKey` is whichever active slot the card is (about to be, or just was)
 * sitting in. Returns false if the card died from this hit (caller should
 * treat the slot as cleared already — killCard has handled that), true
 * otherwise (including when there was no 'rocks' effect to trigger at all). */
function applyRocksOnSwap(match, side, slotKey, events) {
  const entity = match.sides[side];
  const card = cardInSlot(entity, slotKey);
  if (!card || card.cardType || !hasEffect(card, 'rocks')) return true;
  card.currentHp -= 50;
  events.push({ t:'rocks', side, slot: slotKey, card: card.instanceId, name: card.name, dmg: 50 });
  if (card.currentHp <= 0) { killCard(match, side, slotKey, events); return false; }
  return true;
}

/** Win condition: total creature destruction, no player HP involved. A side
 * loses the instant it has zero creatures left anywhere (deployed + hand +
 * deck). Returns 0 or 1 (the winning side), 'draw' for a simultaneous
 * double-wipe (e.g. mutual curse-recoil kills on each side's last creature),
 * or null if the match continues. */
function isMatchOver(match) {
  const c0 = aliveCreatureCount(match.sides[0]);
  const c1 = aliveCreatureCount(match.sides[1]);
  if (c0 <= 0 && c1 <= 0) return 'draw';
  if (c0 <= 0) return 1;
  if (c1 <= 0) return 0;
  return null;
}

/* ── RANK SYSTEM ──────────────────────────────────────────────────────
 * Ten tiers, five sub-ranks each (V worst → I best), 5 rank points per
 * sub-rank — 25 points to climb a whole tier, 250 to run the entire ladder.
 * A win is +2 rank points, a loss is -1 (floored at 0 — see applyMatchReward
 * in server.js, where rankPoints is actually persisted). This table is the
 * single source of truth for turning a raw point total into a tier/sub-rank
 * label; the client mirrors just the display table, never the math. */
const RANK_TIERS = ['Copper','Bronze','Iron','Gold','Platinum','Diamond','Legend','Mythic','Godly','Absolute'];
const RANK_SUBS = ['V','IV','III','II','I']; // index 0 = worst of the tier, index 4 = best
const RANK_POINTS_PER_SUB = 5;
const RANK_SUBS_PER_TIER = RANK_SUBS.length;
const RANK_POINTS_PER_TIER = RANK_POINTS_PER_SUB * RANK_SUBS_PER_TIER; // 25
const RANK_MAX_POINTS = RANK_TIERS.length * RANK_POINTS_PER_TIER - 1;  // 249 — top of Absolute I

/** points -> {tier, sub, label, points}. Points are clamped into the valid
 * ladder range only for the purposes of this lookup — the raw stored value
 * is never itself clamped/mutated, so no precision is lost once someone's
 * sitting at the very top. */
function getRank(points) {
  const p = Math.max(0, Number(points) || 0);
  const clamped = Math.min(p, RANK_MAX_POINTS);
  const step = Math.floor(clamped / RANK_POINTS_PER_SUB);
  const tierIndex = Math.min(RANK_TIERS.length - 1, Math.floor(step / RANK_SUBS_PER_TIER));
  const subIndex = step - tierIndex * RANK_SUBS_PER_TIER;
  const tier = RANK_TIERS[tierIndex], sub = RANK_SUBS[subIndex];
  return { tier, sub, label: `${tier} ${sub}`, points: p };
}

module.exports = {
  CardDB, CardById, CARD_LIBRARY_HASH, CARD_LIBRARY_RAW, PACK_DEFS, PackById, RARITY_ORDER, rarityRank,
  Effects, hasEffect, applyEffectToCard, isImmuneTo, processEffects, checkCardDeath, decayHandEffects,
  createCard, generateDeck, buildDeckFromIds, isDeckLegal, freshSide,
  executeAttack, applyRocksOnSwap, isMatchOver, applyDeployAbility,
  killCard, executeRevive, applySynergies, attackDefFor, aliveCreatureCount,
  applySetBonuses, CARD_SET_MEMBERS, SET_STAT_BONUS,
  openPack, rollRarityFromWeights, pickCardOfRarity, generatePackCards,
  DECK_SIZE, MAX_CREATURES, deckClassificationOk,
  RANK_TIERS, RANK_SUBS, RANK_POINTS_PER_SUB, RANK_POINTS_PER_TIER, RANK_MAX_POINTS, getRank,
};
