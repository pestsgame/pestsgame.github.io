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
 * Death is permanent — a dead creature never returns to the deck — with two
 * exceptions, both driven entirely by `topEffect` data:
 *
 *  1. An `ability`-type top effect carrying a `revive` block (instead of an
 *     `attack`) can be activated as that card's turn action to bring back
 *     any one creature from its own side's graveyard, player's choice, at a
 *     fraction of its max HP (see `executeRevive`). Like an attack, this
 *     uses up that slot's action for the turn — a deliberate player choice,
 *     never automatic.
 *  2. A `passive`-type top effect carrying a `revive` block instead grants
 *     the CARD IT'S ON (and only that card) a self-revive on its own death —
 *     no turn action, no player choice, no graveyard trip at all; it just
 *     comes right back in its own slot (see `tryPassiveRevive`, called from
 *     `killCard`). This is the only kind of "free"/automatic revive in the
 *     game, and it's strictly self-only — it can never bring back anything
 *     else. `revive.guaranteed` banks N always-succeeds revives (tracked per
 *     live card instance), `revive.chance` is a 0–1 roll attempted after any
 *     guaranteed revives are exhausted (or from the very first death if
 *     there's no `guaranteed` at all), and `revive.healPercent` controls how
 *     much HP each revive restores (defaults to 1.0 — full HP — since this
 *     is a much narrower effect than the active ability's).
 *  3. The REVIVE QUEUE (see `processReviveQueue`) is a third, independent
 *     path back from the graveyard — automatic like the passive self-revive,
 *     but classification-driven and NOT self-only: killing an enemy PESTS,
 *     BOSS, or OVERLORD credits your side with revive charges, which are
 *     then spent oldest-to-newest against your own graveyard (which doubles
 *     as this queue) to bring dead creatures back to hand. Entirely separate
 *     currency and trigger from the two revive mechanisms above — a card
 *     can be brought back by whichever of the three gets there first.
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

const RARITY_ORDER = ['common','uncommon','rare','epic','legendary','mythic','fabled'];
const rarityRank = r => RARITY_ORDER.indexOf(r);

/* ── PACK DEFINITIONS (verbatim from client) ──────────────────────── */
const PACK_DEFS = [
  {id:'basic',    currency:'gold', size:3, cost:80,
   weights:{common:50,uncommon:35,rare:15,epic:0,legendary:0,mythic:0,fabled:0}, guarantees:[], filter:null},
  {id:'standard', currency:'gold', size:5, cost:200,
   weights:{common:30,uncommon:32,rare:25,epic:10,legendary:3,mythic:0,fabled:0}, guarantees:[], filter:null},
  {id:'mob',      currency:'gold', size:4, cost:180,
   weights:{common:22,uncommon:30,rare:27,epic:15,legendary:5,mythic:1,fabled:0}, guarantees:[], filter:c=>c.types?.includes('mob')},
  {id:'dragon',   currency:'gold', size:4, cost:180,
   weights:{common:22,uncommon:30,rare:27,epic:15,legendary:5,mythic:1,fabled:0}, guarantees:[], filter:c=>c.types?.includes('dragon')},
  {id:'wizard',   currency:'gold', size:4, cost:180,
   weights:{common:22,uncommon:30,rare:27,epic:15,legendary:5,mythic:1,fabled:0}, guarantees:[], filter:c=>c.types?.includes('wizard')},
  // Armory can only give weapons/defenses, and is capped at Legendary — no Mythic weapons/defenses exist, so mythic stays at 0.
  {id:'armory',   currency:'gold', size:5, cost:220,
   weights:{common:22,uncommon:30,rare:27,epic:15,legendary:6,mythic:0,fabled:0}, guarantees:[], filter:c=>c.cardType==='weapon'||c.cardType==='defense'},
  {id:'boss',     currency:'gems', size:7, cost:150,
   weights:{common:0,uncommon:15,rare:32,epic:30,legendary:17,mythic:4,fabled:2}, guarantees:['rare'], filter:null},
  {id:'overlord', currency:'gems', size:7, cost:250,
   weights:{common:0,uncommon:0,rare:30,epic:30,legendary:23,mythic:10,fabled:7}, guarantees:['epic'], filter:null},
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
  confusion:    { trigger:'onAttack',    logic(c,x){ if (Math.random()<.125) x.cancelAttack = true; } }, // its amp/damp-aware miss roll lives inline in performHit, alongside shock/soak — see missChanceFor
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
  const chance = chanceFor(match, ctx.side, ed, type, .125);
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
/** Some status effects tick damage AND roll a percentage-based chance in the
 * same effect (cryo: dmg + skip-turn chance; shock: dmg + miss-attack
 * chance). For these, a gear's `ampEffects`/`dampEffects` entry must say
 * which dimension it's boosting/reducing — `'damage'` or `'chance'` — via
 * the object form `{ value: N, target: 'damage'|'chance' }`, since a bare
 * number would be ambiguous. Effects with only one dimension (bleed,
 * poison, burn, ... are damage-only; sleep, paralyze, confusion are
 * chance-only) never need `target` — a bare number just applies to that
 * effect's one dimension, and an object form's `target` is ignored. */
const MULTI_DIMENSION_EFFECTS = new Set(['cryo', 'shock']);
/** `soak` is the odd one out: it rolls TWO different percentages (the
 * chance its own carrier whiffs their attack, and the % it reduces
 * incoming damage by) but, unlike cryo/shock, they aren't a damage+chance
 * pair you can target independently — they're both percentages. So a
 * single ampEffects/dampEffects.soak value (bare number OR object form —
 * `target` is ignored) is applied to BOTH of soak's percentages at once. */
const DUAL_PERCENT_EFFECTS = new Set(['soak']);
/** Pulls the raw ampEffects/dampEffects entry for `type` (bare number,
 * `{value,target}` object, or undefined if that gear doesn't touch it) and
 * resolves it down to a plain number for the dimension being asked about
 * (`'damage'` or `'chance'`) — or `undefined` if this entry doesn't apply
 * to that dimension at all (e.g. a shock amp targeting 'chance' has no
 * effect on shock's damage tick). */
function resolveAmpDamp(entry, type, dimension) {
  if (entry == null) return undefined;
  const isObj = typeof entry === 'object';
  const value = isObj ? entry.value : entry;
  if (value == null) return undefined;
  if (DUAL_PERCENT_EFFECTS.has(type)) return value; // applies to both of soak's percentages regardless of target
  if (!MULTI_DIMENSION_EFFECTS.has(type)) return value; // single-dimension effect: no target needed
  // Multi-dimension (cryo/shock): a bare number defaults to 'damage' (matching
  // ampEffects' original damage-only behavior); an object must match `dimension`.
  const target = isObj ? entry.target : 'damage';
  return target === dimension ? value : undefined;
}
function currentAmpValue(match, ed, type, dimension) {
  if (!match || !ed || ed.ampSide == null || !ed.ampKind) return undefined;
  const gearSide = match.sides[ed.ampSide];
  const gear = ed.ampKind === 'weapon' ? (gearSide && gearSide.weaponCard) : (gearSide && gearSide.defenseCard);
  return resolveAmpDamp(gear && gear.ampEffects && gear.ampEffects[type], type, dimension);
}
function currentDampValue(match, side, type, dimension) {
  if (!match || side == null) return undefined;
  const gearSide = match.sides[side];
  const gear = gearSide && gearSide.defenseCard;
  return resolveAmpDamp(gear && gear.dampEffects && gear.dampEffects[type], type, dimension);
}
/** Damage-type effects (bleed, poison, burn, shock, cryo, ...): amp adds
 * to the effect's baseline tick damage, damp subtracts from it — e.g.
 * baseline 10, ampEffects:{burn:25} while equipped -> 35, and if the
 * affected side's own defense also carries dampEffects:{burn:5} while
 * equipped -> 30. For cryo/shock, only an amp/damp entry targeting
 * `'damage'` (or a bare number) applies here — one targeting `'chance'`
 * is skipped. Never drops below 0. `side` is whichever side owns the
 * card the effect is ticking on (i.e. the potential dampener). */
function effectiveDmg(match, side, ed, type, baseDmg) {
  const amp = currentAmpValue(match, ed, type, 'damage');
  const damp = currentDampValue(match, side, type, 'damage');
  let dmg = baseDmg;
  if (amp != null) dmg += amp;
  if (damp != null) dmg -= damp;
  return Math.max(0, dmg);
}
/** Chance-type effects (sleep, paralyze, confusion, shock/soak's miss
 * roll, cryo's skip-turn roll, ...): ampEffects/dampEffects values are
 * 0–100 percentage POINTS added to/subtracted from the effect's baseline
 * chance — e.g. baseline 12.5%, ampEffects:{paralyze:25} while equipped
 * -> 37.5%, and if the affected side's defense carries
 * dampEffects:{paralyze:10} while equipped -> 27.5%. For cryo/shock, only
 * an amp/damp entry targeting `'chance'` applies here — a bare number (or
 * one targeting `'damage'`) is skipped, since it's boosting the DOT tick
 * instead. Clamped to the 0–100% range either way. */
function chanceFor(match, side, ed, type, baseChance) {
  const amp = currentAmpValue(match, ed, type, 'chance');
  const damp = currentDampValue(match, side, type, 'chance');
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
 * permanently, unless and until that side spends an `ability`-type revive's
 * turn action to bring it back (see `executeRevive`) — or unless the dying
 * card itself carries a self-revive `passive` (see `tryPassiveRevive`),
 * checked first, right here, before any death actually lands. Aside from
 * that self-revive passive, there is no automatic or banked revive of any
 * kind — every other revival is a deliberate player choice that costs a
 * card's action, same as an attack would.
 * Returns `true` if the card actually died (graveyard, slot cleared), or
 * `false` if a self-revive passive saved it — callers that were about to
 * treat this as a kill (stopping a multi-attack sequence, clearing a slot,
 * etc.) should check this before assuming the card is gone. */
function killCard(match, side, slotKey, events) {
  const entity = match.sides[side];
  const card = cardInSlot(entity, slotKey);
  if (!card) return true;
  if (tryPassiveRevive(card, events, side, slotKey)) return false;
  events.push({ t:'death', side, slot: slotKey, card:card.instanceId, name:card.name });
  if (entity.activeCard === card) entity.activeCard = null;
  else if (entity.activeCard2 === card) entity.activeCard2 = null;
  entity.graveyard.push(card); // joins the back of the Revive Queue — see processReviveQueue

  // ── Revive Queue (classification-based, automatic) ──────────────────
  // Whichever side didn't just lose this creature is credited with its
  // classification's revive charges (0 for a Normal/unclassified kill —
  // see reviveClassFor) — this covers every death path uniformly
  // (attack, curse recoil, DOT, rocks), crediting the "other side" of
  // whoever died in every case. Both sides' queues are then re-checked:
  // the dying side's because it just gained a new entry that a
  // previously-unspendable held charge might now afford, the credited
  // side's because it may have just gained enough to clear something.
  const otherSide = side === 0 ? 1 : 0;
  const base = CardById[card.baseId];
  grantReviveCharges(match, otherSide, base && base.classification, events);
  processReviveQueue(match, otherSide, events);
  processReviveQueue(match, side, events);
  return true;
}

/** ── REVIVE QUEUE ─────────────────────────────────────────────────────
 * A side's `graveyard` array doubles as its Revive Queue: pushes at death
 * time, so index 0 is always the oldest death and the array is already in
 * the right order to scan front-to-back — no separate structure needed.
 *
 * Classification -> { cost, charge } (cards.json's `"classification"` field
 * — `"normal"` is a real, explicit value here, not just "no classification"):
 *   normal:   cost 1 to revive, grants 0 charges when killed.
 *   pests:    cost 1 to revive, grants 1 charge when killed.
 *   boss:     cost 2 to revive, grants 2 charges when killed.
 *   overlord: cost 2 to revive, grants 2 charges when killed.
 * Killing an ENEMY creature credits YOUR side with its `charge` value;
 * reviving one of YOUR OWN dead creatures spends its `cost` from your
 * side's `reviveCharges` bank. Anything missing/unrecognized falls back to
 * the `normal` row, so a stray/legacy unclassified card doesn't crash. */
const REVIVE_CLASS_TABLE = {
  normal:   { cost: 1, charge: 0 },
  pests:    { cost: 1, charge: 1 },
  boss:     { cost: 2, charge: 2 },
  overlord: { cost: 2, charge: 2 },
};
function reviveClassFor(classification) {
  return REVIVE_CLASS_TABLE[classification] || REVIVE_CLASS_TABLE.normal;
}
/** Credits `side` with the revive charges earned for killing an enemy
 * creature of `classification` — a no-op (0 charges) for a Normal kill. */
function grantReviveCharges(match, side, classification, events) {
  const { charge } = reviveClassFor(classification);
  if (charge <= 0) return;
  match.sides[side].reviveCharges += charge;
  events.push({ t:'revive_charge_gain', side, amount: charge, classification, total: match.sides[side].reviveCharges });
}
/** Placeholder heal fraction applied to a creature the Revive Queue brings
 * back — freely tunable, nothing else depends on this number. */
const REVIVE_QUEUE_HEAL_PERCENT = 1;
/** Walks `side`'s graveyard/Revive Queue oldest -> newest, reviving (back
 * to hand) every entry `side.reviveCharges` can fully afford and skipping
 * — never partially paying toward — any it can't, per the "no partial
 * progress on a BOSS/OVERLORD" rule. A successful revive removes that
 * entry and restarts the scan from the front, since spending charges (and
 * the queue shrinking) can change what else is now reachable; charges
 * that can't fully afford anything currently in the queue are simply left
 * sitting on `side.reviveCharges` untouched — there's nothing extra to do
 * to "hold" them, since holding is just what happens when nothing gets
 * spent. That's also why this only needs to run after (a) a kill grants
 * new charges, and (b) a new death joins the queue — both are the only
 * moments that can change what's affordable. */
function processReviveQueue(match, side, events) {
  const entity = match.sides[side];
  let progressed = true;
  while (progressed) {
    progressed = false;
    for (let i = 0; i < entity.graveyard.length; i++) {
      const card = entity.graveyard[i];
      const base = CardById[card.baseId];
      const { cost } = reviveClassFor(base && base.classification);
      if (entity.reviveCharges < cost) continue; // can't fully afford it — skip in place, no partial spend
      entity.graveyard.splice(i, 1);
      entity.reviveCharges -= cost;
      card.currentHp = Math.max(1, Math.round(card.maxHp * REVIVE_QUEUE_HEAL_PERCENT));
      card.activeEffects = [];
      entity.hand.push(card);
      events.push({ t:'queue_revive', side, card: card.instanceId, name: card.name, hp: card.currentHp, maxHp: card.maxHp, cost, chargesLeft: entity.reviveCharges });
      progressed = true;
      break; // queue + charges both changed — rescan from the front
    }
  }
}

/** Checks `card` for a self-revive `passive` (`topEffect.type === 'passive'`
 * with a `topEffect.revive` block) and, if it grants a revive right now,
 * applies it in place: heals the SAME card back up (never leaves its slot,
 * never touches the graveyard) and clears its lingering status effects,
 * same cleanup a normal revive gets. This is self-only by design — it never
 * revives anything else, and other creatures can't trigger it for it.
 *
 * `revive.guaranteed` (if present) is a bank of always-succeeds revives,
 * tracked per LIVE CARD INSTANCE in `card.reviveState.guaranteedLeft`
 * (seeded once at `createCard` time, so it's per-match and doesn't leak
 * between games). The guaranteed bank is always drained before any
 * `revive.chance` roll is attempted — so "N guaranteed, then a chance after
 * that" spends the guaranteed pool down first, and only starts rolling once
 * it's empty. A card with only `chance` (no `guaranteed`) rolls every single
 * death, forever, with no free revives at all.
 *
 * Returns true if the card was revived (caller must NOT proceed with the
 * kill), false if no self-revive passive applies here, or this particular
 * death rolled unlucky. */
function tryPassiveRevive(card, events, side, slotKey) {
  const te = card.topEffect;
  if (!te || te.type !== 'passive' || !te.revive) return false;
  // Boss/Overlord cards never come back via a card's own revive ability
  // (active or passive) — losing one is meant to be permanent within a
  // match, unlike the classification-driven Revive Queue (see
  // processReviveQueue), which is a deliberately different mechanic that
  // DOES let charges bring a Boss/Overlord back.
  const selfDef = CardById[card.baseId];
  if (selfDef && (selfDef.classification === 'boss' || selfDef.classification === 'overlord')) return false;
  const rv = te.revive;
  let revived = false;
  if (card.reviveState && card.reviveState.guaranteedLeft > 0) {
    card.reviveState.guaranteedLeft--;
    revived = true;
  } else if (rv.chance) {
    revived = Math.random() < rv.chance;
  }
  if (!revived) return false;
  const healPercent = rv.healPercent != null ? rv.healPercent : 1;
  card.currentHp = Math.max(1, Math.round(card.maxHp * healPercent));
  card.activeEffects = [];
  events.push({ t:'passive_revive', side, slot: slotKey, card: card.instanceId, name: card.name, hp: card.currentHp, maxHp: card.maxHp });
  return true;
}

/** Activates an `ability` top-effect's `revive` block as `slotKey`'s action
 * for the turn: the acting card must be alive, unacted-this-turn, and have
 * `topEffect.type === 'ability'` with a `topEffect.revive` block present
 * (this is what distinguishes an active revive ability from an ordinary
 * on-deploy `ability` like a curse or a heal); `deadInstanceId` must name a
 * creature currently in this side's graveyard (the caller's choice —
 * earliest, latest, whichever they want) — EXCEPT a Boss or Overlord,
 * which a revive ability can never target (see tryPassiveRevive for the
 * same rule on the passive/self-revive side; the classification-driven
 * Revive Queue in processReviveQueue is deliberately exempt from this —
 * that's its whole purpose). The revived creature returns to hand at
 * `topEffect.revive.healPercent` of its max HP (50% if unspecified) with
 * all lingering statuses cleared, and this consumes the acting card's turn
 * exactly like an attack would.
 *
 * One-turn cooldown: the SAME acting card instance can't use its revive
 * ability again on its side's very next turn (tracked via
 * `match.turnCounter`, which increments once per runTurnStart call for
 * either side — since turns strictly alternate, "this side's next turn" is
 * always turnCounter+2 from whichever turnCounter it was just used on).
 * Other revive-capable cards on the same side are unaffected; this is a
 * per-card lock, not a side-wide one.
 *
 * This is unrelated to (and can't trigger) a `passive`-type self-revive —
 * see `tryPassiveRevive` — which is automatic, self-only, and never costs a
 * turn action. */
function executeRevive(match, side, slotKey, deadInstanceId) {
  const entity = match.sides[side];
  const events = [];
  if (match.actedThisTurn[side].has(slotKey)) return { ok:false, reason:'already_acted', events };
  const actingCard = cardInSlot(entity, slotKey);
  if (!actingCard) return { ok:false, reason:'no_card_in_slot', events };
  if (!actingCard.topEffect || actingCard.topEffect.type !== 'ability' || !actingCard.topEffect.revive) return { ok:false, reason:'no_revive_ability', events };
  if (actingCard.reviveLockUntilTurnCounter === match.turnCounter) return { ok:false, reason:'revive_on_cooldown', events };

  const idx = entity.graveyard.findIndex(c => c.instanceId === deadInstanceId);
  if (idx === -1) return { ok:false, reason:'invalid_target', events };
  const targetDef = CardById[entity.graveyard[idx].baseId];
  if (targetDef && (targetDef.classification === 'boss' || targetDef.classification === 'overlord')) {
    return { ok:false, reason:'cannot_revive_boss_or_overlord', events };
  }
  const [card] = entity.graveyard.splice(idx, 1);

  const healPercent = actingCard.topEffect.revive.healPercent != null ? actingCard.topEffect.revive.healPercent : 0.5;
  card.currentHp = Math.max(1, Math.round(card.maxHp * healPercent));
  card.activeEffects = [];
  entity.hand.push(card);
  events.push({ t:'revive', side, card: card.instanceId, name: card.name, hp: card.currentHp, maxHp: card.maxHp, via: actingCard.instanceId });

  actingCard.reviveLockUntilTurnCounter = match.turnCounter + 2;
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
  if (base.topEffect.type === 'passive' && base.topEffect.revive) {
    // Per-live-instance revive bank — freshly seeded every time a deck is
    // built, so it's per-match and never leaks a spent revive between games.
    card.reviveState = { guaranteedLeft: base.topEffect.revive.guaranteed || 0 };
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
        if (applied) events.push({ t:'effect_applied', side, target: target.instanceId, effect: eff.type });
        else events.push({ t:'immune', side: otherSide, card: target.instanceId, effect: eff.type });
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

const DECK_SIZE = 12;
/** Creatures (wizard/mob/dragon cards — anything without a cardType) are
 * capped at 12 per deck (i.e. a deck can be all-creature, with equipment
 * entirely optional) — the remaining slots (down to DECK_SIZE) must be
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
  // "Normal" and "PESTS" are both unrestricted filler classifications (see
  // deckClassificationOk — only boss/overlord counts are capped), so both
  // pool together here. cards.json is currently all placeholder data using
  // only 'pests'; 'normal' is included too so real cards using that
  // classification (see REVIVE_CLASS_TABLE) slot in without further changes.
  const normals = CardDB.filter(c => !c.cardType && (c.classification === 'pests' || c.classification === 'normal'));
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

/** Plain Fisher-Yates shuffle, in place. Used so the starting hand (and
 * subsequent draws) are a genuinely random sample of the deck, not
 * whatever fixed order it happened to be built/stored in. */
function shuffleDeck(arr) {
  for (let i = arr.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [arr[i], arr[j]] = [arr[j], arr[i]];
  }
  return arr;
}

/* ── PLAYER SIDE FACTORY ──────────────────────────────────────────── */
function freshSide(deck) {
  const d = [...deck];
  applySynergies(d); // deck+hand together — synergy is about composition, not what's drawn yet, so this
                      // runs before the shuffle below (order-independent — see applySynergies' own .find() lookups)
  shuffleDeck(d);
  return {
    hp: 100, maxHp: 100, // cosmetic only — see module doc; never decides the match anymore
    activeCard: null, activeCard2: null, weaponCard: null, defenseCard: null,
    deck: d, hand: d.splice(0, 5), // 5 random cards from the shuffled deck
    graveyard: [], // doubles as the Revive Queue — oldest death first (see processReviveQueue)
    reviveCharges: 0, // spent oldest-to-newest against the graveyard/Revive Queue
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
  if (hasEffect(ac, 'confusion') && Math.random() < missChanceFor(ac,'confusion',match,side,.125)) { events.push({t:'miss',side,slot:atkSlotKey,card:ac.instanceId,cause:'confusion'}); return { stop:false }; }
  if (hasEffect(ac, 'shock') && Math.random() < missChanceFor(ac,'shock',match,side,.125))         { events.push({t:'miss',side,slot:atkSlotKey,card:ac.instanceId,cause:'shock'});     return { stop:false }; }
  if (hasEffect(ac, 'soak') && Math.random() < missChanceFor(ac,'soak',match,side,.125))           { events.push({t:'miss',side,slot:atkSlotKey,card:ac.instanceId,cause:'soak'});      return { stop:false }; }

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
  if (hasEffect(ac, 'burn')) { dmg = Math.floor(dmg * .75); events.push({t:'burn_penalty', side, slot:atkSlotKey}); }

  if (!targetCard) {
    // No player HP anymore — a "direct hit" into an empty slot is a no-op,
    // kept only so the client's existing miss/whiff animation still fires.
    events.push({ t:'hit', atkSide:side, atkSlot:atkSlotKey, atkCard:ac.instanceId, defSide, defSlot:null, tgtCard:null, direct:true, dmg:0, name:atkDef.name, element:atkDef.element });
    return { stop:false };
  }

  const tgtSlotKey = slotOfCard(defEntity, targetCard);

  if (hasEffect(targetCard, 'soak')) {
    const soakEd = targetCard.activeEffects.find(e => e.type === 'soak');
    const reduction = chanceFor(match, defSide, soakEd, 'soak', .25); // same amp/damp value as soak's miss-attack roll, per DUAL_PERCENT_EFFECTS
    dmg = Math.floor(dmg * (1 - reduction));
    events.push({t:'soak_reduce', side:defSide, slot:tgtSlotKey});
  }
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
    if (applied) events.push({ t:'effect_applied', side, target: targetCard.instanceId, effect: eff.type });
    else events.push({ t:'immune', side: defSide, card: targetCard.instanceId, effect: eff.type });
  });
  if (weaponRef && weaponRef.addEffects && weaponRef.addEffects.length) {
    weaponRef.addEffects.forEach(eff => {
      const applied = applyEffectToCard(targetCard, eff, weaponAmp);
      if (applied) events.push({ t:'effect_applied', side, target: targetCard.instanceId, effect: eff.type });
      else events.push({ t:'immune', side: defSide, card: targetCard.instanceId, effect: eff.type });
    });
    events.push({ t:'weapon_effect', side, slot:atkSlotKey, weapon:weaponRef.baseId, target:targetCard.instanceId });
  }
  if (defenseRef && defenseRef.addEffects && defenseRef.addEffects.length) {
    const defenseAmp = { side: defSide, kind:'defense' };
    defenseRef.addEffects.forEach(eff => {
      const applied = applyEffectToCard(ac, eff, defenseAmp);
      if (applied) events.push({ t:'effect_applied', side: defSide, target: ac.instanceId, effect: eff.type });
      else events.push({ t:'immune', side, card: ac.instanceId, effect: eff.type });
    });
    events.push({ t:'defense_effect', side:defSide, slot:tgtSlotKey, defense:defenseRef.baseId, target:ac.instanceId });
  }

  if (hasEffect(targetCard, 'curse')) {
    const r = Math.floor(dmg * .25); ac.currentHp -= r;
    events.push({ t:'curse_recoil', side, slot:atkSlotKey, card:ac.instanceId, dmg:r });
    if (ac.currentHp <= 0) {
      const died = killCard(match, side, atkSlotKey, events);
      if (died) return { stop:true };
    }
  }
  if (targetCard.currentHp <= 0) {
    const died = killCard(match, defSide, tgtSlotKey, events);
    if (died) return { stop:true };
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
 * sitting in. Returns false if the card actually died from this hit (caller
 * should treat the slot as cleared already — killCard has handled that),
 * true otherwise — including when there was no 'rocks' effect to trigger at
 * all, AND when the card had lethal rocks damage but self-revived via a
 * `passive` revive block instead of dying (see `tryPassiveRevive`). */
function applyRocksOnSwap(match, side, slotKey, events) {
  const entity = match.sides[side];
  const card = cardInSlot(entity, slotKey);
  if (!card || card.cardType || !hasEffect(card, 'rocks')) return true;
  card.currentHp -= 50;
  events.push({ t:'rocks', side, slot: slotKey, card: card.instanceId, name: card.name, dmg: 50 });
  if (card.currentHp <= 0) { const died = killCard(match, side, slotKey, events); return !died; }
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
/** Rank-points threshold for "reach `tier` `sub`" (sub omitted/null means
 * just entering the tier, i.e. its worst sub, index 0). Used by the
 * rank-climb permanent quests below — e.g. rankThreshold('Bronze','I') is
 * the point total at which you've cleared all of Bronze and are one win
 * from promoting into Iron. */
function rankThreshold(tier, sub) {
  const tierIdx = Math.max(0, RANK_TIERS.indexOf(tier));
  const subIdx = sub ? Math.max(0, RANK_SUBS.indexOf(sub)) : 0;
  return tierIdx * RANK_POINTS_PER_TIER + subIdx * RANK_POINTS_PER_SUB;
}

/* ══════════════════════════════════════════════════════════════════════
 * QUEST SYSTEM — pure data + pure helpers only. All progress is tracked,
 * persisted, and verified server-side (see server.js's player_quest_progress
 * / player_quest_bars tables + recordQuestEvent/recordQuestThreshold/
 * addQuestBarPoints) — the client is only ever told the result, never
 * trusted to report it, same spirit as everything else in this file. Every
 * quest below pays real currency (permanent) or quest-bar points
 * (daily/weekly), so every last one of them is server-verified — nothing
 * here is decorative.
 *
 * Three scopes:
 *   daily      — resets every UTC day (see dailyPeriodKey). Completing one
 *                awards `points` onto that day's Daily Quest Bar.
 *   weekly     — resets every ISO week (see weeklyPeriodKey). Same idea,
 *                onto that week's Weekly Quest Bar.
 *   permanent  — never resets, completes once, ever. Pays its `reward`
 *                (currency now; `banner`/`icon` fields are placeholders for
 *                a future cosmetic-unlock system — currently a no-op if set)
 *                on completion, no bar involved. Reaching a daily/weekly
 *                quest's target doesn't pay anything by itself either —
 *                see claim_quest/claim_milestone in server.js, nothing
 *                pays out until the player explicitly claims it.
 *
 * Two flavors of progress, both server-driven, never client-reported:
 *   cumulative — recordQuestEvent(userId, track, amount) ADDS `amount` to
 *                every active quest listening on `track` (e.g. "play 3
 *                matches", "open 15 packs").
 *   threshold  — recordQuestThreshold(userId, track, currentValue) sets
 *                progress to max(existing progress, currentValue) instead
 *                of adding — for "reach/have/hold N" style quests (gold
 *                held, friend count, unique cards owned, rank points, the
 *                highest loss-streak against any single opponent) where
 *                what matters is the best value ever observed, not a tally
 *                of events.
 * Each def's `kind` (defaulting to 'cumulative' when omitted) tells
 * server.js which of the two to use for it. */
const QUEST_TRACK = {
  MATCH_PLAY: 'match_play',                   // any match completed, regardless of outcome
  MATCH_WIN: 'match_win',
  RANKED_MATCH_PLAY: 'ranked_match_play',
  RANKED_MATCH_WIN: 'ranked_match_win',
  STATUS_EFFECT_APPLY: 'status_effect_apply',
  MARKET_LISTING_CREATE: 'market_listing_create',
  MARKET_LISTINGS_ACTIVE: 'market_listings_active', // threshold: concurrent active listings right now
  MARKET_PURCHASE: 'market_purchase',        // buyer side of a completed transaction
  MARKET_SALE: 'market_sale',                // seller side of a completed transaction
  MARKET_TRANSACTION: 'market_transaction',  // fires for BOTH sides of any completed transaction
  MARKET_FLIP_PROFIT: 'market_flip_profit',  // event: sold something for more than its last-bought price
  MARKET_SELL_LOSS: 'market_sell_loss',      // event: sold something for less than its last-bought price
  GOLD_HELD: 'gold_held',                    // threshold: current gold balance
  PACK_BUY: 'pack_buy',
  PACK_VARIETY: 'pack_variety',              // threshold: count of distinct pack ids ever opened
  FRIEND_DUEL: 'friend_duel',
  CARDS_UNIQUE: 'cards_unique',              // threshold: distinct card ids owned
  OVERLORDS_OWNED: 'overlords_owned',        // threshold: count of Overlord-classified cards owned
  CHAT_MESSAGE: 'chat_message',
  FRIENDS_COUNT: 'friends_count',            // threshold
  NEMESIS_LOSS: 'nemesis_loss',              // threshold: worst per-opponent loss streak (see recordQuestThreshold)
  GUILD_JOIN: 'guild_join',
  PULL_RARITY_MYTHIC: 'pull_rarity_mythic',
  PULL_OVERLORD_CARD: 'pull_overlord_card',  // pulled a card classified 'overlord' from a pack
  RANK_POINTS: 'rank_points',                // threshold: current rank points
  PROFILE_CUSTOMIZE: 'profile_customize',
};

const QUEST_DEFS = [
  // ── DAILY (8) ──────────────────────────────────────────────────────
  { id:'daily_arena_regular', scope:'daily', track:QUEST_TRACK.MATCH_PLAY, target:3, points:20,
    title:'Arena Regular', desc:'Play 3 matches.' },
  { id:'daily_victory_lap', scope:'daily', track:QUEST_TRACK.MATCH_WIN, target:1, points:10,
    title:'Victory Lap', desc:'Win 1 match.' },
  { id:'daily_ranked_regular', scope:'daily', track:QUEST_TRACK.RANKED_MATCH_PLAY, target:1, points:10,
    title:'Ranked Regular', desc:'Play 1 ranked match.' },
  { id:'daily_status_report', scope:'daily', track:QUEST_TRACK.STATUS_EFFECT_APPLY, target:5, points:10,
    title:'Status Report', desc:'Apply 5 status effects.' },
  { id:'daily_merchant', scope:'daily', track:QUEST_TRACK.MARKET_LISTING_CREATE, target:1, points:10,
    title:'Merchant', desc:'List 1 item on the marketplace.' },
  { id:'daily_pack_opener', scope:'daily', track:QUEST_TRACK.PACK_BUY, target:1, points:20,
    title:'Pack Opener', desc:'Open 1 pack.' },
  { id:'daily_shopping_spree', scope:'daily', track:QUEST_TRACK.MARKET_PURCHASE, target:2, points:20,
    title:'Shopping Spree', desc:'Purchase 2 marketplace items.' },
  { id:'daily_rich_guy', scope:'daily', track:QUEST_TRACK.GOLD_HELD, target:200, points:10, kind:'threshold',
    title:'Rich Guy', desc:'Get 200 coins.' },

  // ── WEEKLY (9) ─────────────────────────────────────────────────────
  { id:'weekly_arena_conquerer', scope:'weekly', track:QUEST_TRACK.MATCH_PLAY, target:15, points:10,
    title:'Arena Conquerer', desc:'Play 15 matches.' },
  { id:'weekly_victorious', scope:'weekly', track:QUEST_TRACK.MATCH_WIN, target:5, points:10,
    title:'Victorious', desc:'Win 5 matches.' },
  { id:'weekly_rank_up', scope:'weekly', track:QUEST_TRACK.RANKED_MATCH_WIN, target:4, points:20,
    title:'Rank Up', desc:'Win 4 ranked matches.' },
  { id:'weekly_marketplace_mogul', scope:'weekly', track:QUEST_TRACK.MARKET_TRANSACTION, target:8, points:20,
    title:'Marketplace Mogul', desc:'Complete 8 marketplace transactions.' },
  { id:'weekly_open_for_business', scope:'weekly', track:QUEST_TRACK.MARKET_LISTINGS_ACTIVE, target:4, points:10, kind:'threshold',
    title:'Open for Business', desc:'Have 4 marketplace listings active at the same time.' },
  { id:'weekly_pack_addict', scope:'weekly', track:QUEST_TRACK.PACK_BUY, target:15, points:20,
    title:'Pack Addict', desc:'Open 15 packs.' },
  { id:'weekly_pack_variety', scope:'weekly', track:QUEST_TRACK.PACK_VARIETY, target:5, points:10, kind:'threshold',
    title:'Pack Variety', desc:'Open at least one pack of 5 different types.' },
  { id:'weekly_old_friends', scope:'weekly', track:QUEST_TRACK.FRIEND_DUEL, target:1, points:10,
    title:'Old Friends', desc:'Duel a friend one time.' },
  { id:'weekly_golden_guy', scope:'weekly', track:QUEST_TRACK.GOLD_HELD, target:1000, points:20, kind:'threshold',
    title:'Golden Guy', desc:'Get 1000 coins.' },

  // ── PERMANENT (28) — reward amounts are placeholder/tunable; several
  // carry a reward.icon or reward.banner too (unlocking a quest-locked
  // profile cosmetic — see PROFILE_ICONS_LOCKED/PROFILE_BANNERS_LOCKED in
  // server.js and ICON_SVGS/banner-* CSS in docs/index.html), granted
  // alongside the currency the instant the quest completes, same as spec:
  // "permanent quests give immediate currency... and profile
  // banners/icons". ─────────────────────────────────────────────────
  { id:'perm_first_card', scope:'permanent', track:QUEST_TRACK.CARDS_UNIQUE, target:1, kind:'threshold',
    title:'First Card', desc:'Obtain your first card.', reward:{gold:50,gems:0,icon:'first_card'} },
  { id:'perm_first_pack', scope:'permanent', track:QUEST_TRACK.PACK_BUY, target:1,
    title:'First Pack', desc:'Open your first pack.', reward:{gold:50,gems:0,banner:'first_pack'} },
  { id:'perm_collector', scope:'permanent', track:QUEST_TRACK.CARDS_UNIQUE, target:50, kind:'threshold',
    title:'Collector', desc:'Obtain 50 unique cards.', reward:{gold:400,gems:5,icon:'collector'} },
  { id:'perm_archivist', scope:'permanent', track:QUEST_TRACK.CARDS_UNIQUE, target:100, kind:'threshold',
    title:'Archivist', desc:'Obtain 100 unique cards.', reward:{gold:900,gems:15,banner:'archivist'} },
  { id:'perm_overlord_master', scope:'permanent', track:QUEST_TRACK.OVERLORDS_OWNED, target:2, kind:'threshold',
    title:'Overlord Master', desc:'Obtain two overlords.', reward:{gold:600,gems:10,icon:'overlord_master'} },
  { id:'perm_first_sale', scope:'permanent', track:QUEST_TRACK.MARKET_SALE, target:1,
    title:'First Sale', desc:'Sell your first item.', reward:{gold:75,gems:0,banner:'first_sale'} },
  { id:'perm_first_purchase', scope:'permanent', track:QUEST_TRACK.MARKET_PURCHASE, target:1,
    title:'First Purchase', desc:'Buy your first item.', reward:{gold:75,gems:0} },
  { id:'perm_merchant', scope:'permanent', track:QUEST_TRACK.MARKET_TRANSACTION, target:10,
    title:'Merchant', desc:'Complete 10 marketplace transactions.', reward:{gold:350,gems:5,icon:'merchant'} },
  { id:'perm_flipper', scope:'permanent', track:QUEST_TRACK.MARKET_FLIP_PROFIT, target:1,
    title:'Flipper', desc:'Buy an item and sell it later for more than what you originally paid.', reward:{gold:200,gems:0,icon:'flipper'} },
  { id:'perm_market_crash', scope:'permanent', track:QUEST_TRACK.MARKET_SELL_LOSS, target:1,
    title:'Market Crash', desc:'Sell an item for less than you originally paid.', reward:{gold:100,gems:0,banner:'market_crash'} },
  { id:'perm_hello_world', scope:'permanent', track:QUEST_TRACK.CHAT_MESSAGE, target:1,
    title:'Hello, World', desc:'Send your first Global Chat message.', reward:{gold:50,gems:0,banner:'hello_world'} },
  { id:'perm_popular', scope:'permanent', track:QUEST_TRACK.FRIENDS_COUNT, target:10, kind:'threshold',
    title:'Popular', desc:'Have 10 friends.', reward:{gold:250,gems:5} },
  { id:'perm_nemesis', scope:'permanent', track:QUEST_TRACK.NEMESIS_LOSS, target:5, kind:'threshold',
    title:'Nemesis', desc:'Lose to the same player 5 times.', reward:{gold:150,gems:0,icon:'nemesis'} },
  { id:'perm_guildgoer', scope:'permanent', track:QUEST_TRACK.GUILD_JOIN, target:1,
    title:'Guildgoer', desc:'Join a Guild.', reward:{gold:75,gems:0,banner:'guildgoer'} },
  { id:'perm_pack_rat', scope:'permanent', track:QUEST_TRACK.PACK_BUY, target:10,
    title:'Pack Rat', desc:'Open 10 packs.', reward:{gold:150,gems:0} },
  { id:'perm_pack_addict', scope:'permanent', track:QUEST_TRACK.PACK_BUY, target:100,
    title:'Pack Addict', desc:'Open 100 packs.', reward:{gold:800,gems:10} },
  { id:'perm_pack_fiend', scope:'permanent', track:QUEST_TRACK.PACK_BUY, target:1000,
    title:'Pack Fiend', desc:'Open 1,000 packs.', reward:{gold:5000,gems:100,icon:'pack_fiend'} },
  { id:'perm_lucky_bastard', scope:'permanent', track:QUEST_TRACK.PULL_RARITY_MYTHIC, target:1,
    title:'Lucky Bastard', desc:'Pull a Mythic.', reward:{gold:300,gems:10,icon:'lucky_bastard'} },
  { id:'perm_fabled_pull', scope:'permanent', track:QUEST_TRACK.PULL_OVERLORD_CARD, target:1,
    title:'Fabled Pull', desc:'Pull an OVERLORD.', reward:{gold:500,gems:20,banner:'fabled_pull'} },
  { id:'perm_bronze_climber', scope:'permanent', track:QUEST_TRACK.RANK_POINTS, target:rankThreshold('Bronze','I'), kind:'threshold',
    title:'Bronze Climber', desc:'Reach Bronze I.', reward:{gold:100,gems:0} },
  { id:'perm_iron_climber', scope:'permanent', track:QUEST_TRACK.RANK_POINTS, target:rankThreshold('Iron','I'), kind:'threshold',
    title:'Iron Climber', desc:'Reach Iron I.', reward:{gold:150,gems:0} },
  { id:'perm_gold_climber', scope:'permanent', track:QUEST_TRACK.RANK_POINTS, target:rankThreshold('Gold','I'), kind:'threshold',
    title:'Gold Climber', desc:'Reach Gold I.', reward:{gold:250,gems:5} },
  { id:'perm_platinum_climber', scope:'permanent', track:QUEST_TRACK.RANK_POINTS, target:rankThreshold('Platinum','I'), kind:'threshold',
    title:'Platinum Climber', desc:'Reach Platinum I.', reward:{gold:400,gems:10} },
  { id:'perm_diamond_climber', scope:'permanent', track:QUEST_TRACK.RANK_POINTS, target:rankThreshold('Diamond','I'), kind:'threshold',
    title:'Diamond Climber', desc:'Reach Diamond I.', reward:{gold:600,gems:15} },
  { id:'perm_legendary_status', scope:'permanent', track:QUEST_TRACK.RANK_POINTS, target:rankThreshold('Legend'), kind:'threshold',
    title:'Legendary Status', desc:'Reach Legend.', reward:{gold:900,gems:25} },
  { id:'perm_mythic_status', scope:'permanent', track:QUEST_TRACK.RANK_POINTS, target:rankThreshold('Mythic'), kind:'threshold',
    title:'Mythic Status', desc:'Reach Mythic.', reward:{gold:1300,gems:40} },
  { id:'perm_godly', scope:'permanent', track:QUEST_TRACK.RANK_POINTS, target:rankThreshold('Godly'), kind:'threshold',
    title:'Godly', desc:'Reach Godly.', reward:{gold:1800,gems:60} },
  { id:'perm_absolute', scope:'permanent', track:QUEST_TRACK.RANK_POINTS, target:rankThreshold('Absolute'), kind:'threshold',
    title:'Absolute', desc:'Reach Absolute.', reward:{gold:2500,gems:100,banner:'absolute'} },

  // Scaffolded, not in the current list — same system, shows what else can
  // slot in later without any further engine changes:
  // { id:'perm_customize_profile', scope:'permanent', track:QUEST_TRACK.PROFILE_CUSTOMIZE, target:1,
  //   title:'Make It Yours', desc:'Customize your profile.', reward:{ gold:100, gems:0 } },
];
const QuestById = Object.fromEntries(QUEST_DEFS.map(q => [q.id, q]));
const questDefsForTrack = track => QUEST_DEFS.filter(q => q.track === track);
const questDefsForScope = scope => QUEST_DEFS.filter(q => q.scope === scope);

/** The bar fills from 0–100 via each completed daily/weekly quest's
 * `points`, paying out a (placeholder, tunable) currency reward every time
 * it crosses one of these milestones — weekly pays more than daily since
 * its quests are harder. */
const QUEST_BAR_MILESTONES = [20, 40, 60, 80, 100];
const QUEST_BAR_MILESTONE_REWARDS = {
  daily:  { 20:{gold:25}, 40:{gold:25}, 60:{gold:35}, 80:{gold:35}, 100:{gold:75,  gems:2} },
  weekly: { 20:{gold:60}, 40:{gold:60}, 60:{gold:80}, 80:{gold:80}, 100:{gold:200, gems:8} },
};

/** UTC calendar day, e.g. '2026-08-13' — the daily reset boundary. A new
 * key each day means a brand-new progress/bar row is simply created fresh;
 * nothing needs to explicitly "reset" the old one. */
function dailyPeriodKey(now = new Date()) {
  return now.toISOString().slice(0, 10);
}
/** ISO-8601 week key, e.g. '2026-W33' — the weekly reset boundary (Monday
 * start, first week of a year is the one containing that year's first
 * Thursday). Same "new key = fresh row" reasoning as dailyPeriodKey. */
function weeklyPeriodKey(now = new Date()) {
  const d = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()));
  const dayNum = (d.getUTCDay() + 6) % 7; // Mon=0 .. Sun=6
  d.setUTCDate(d.getUTCDate() - dayNum + 3); // shift to this ISO week's Thursday
  const firstThursday = new Date(Date.UTC(d.getUTCFullYear(), 0, 4));
  const week = 1 + Math.round(((d - firstThursday) / 86400000 - 3 + ((firstThursday.getUTCDay() + 6) % 7)) / 7);
  return `${d.getUTCFullYear()}-W${String(week).padStart(2, '0')}`;
}
/** Permanent quests never reset, so they always live under this one
 * constant "period" rather than a rolling date/week key. */
const PERMANENT_PERIOD_KEY = 'permanent';
function periodKeyForScope(scope, now = new Date()) {
  if (scope === 'daily') return dailyPeriodKey(now);
  if (scope === 'weekly') return weeklyPeriodKey(now);
  return PERMANENT_PERIOD_KEY;
}

/** Given a bar's already-claimed milestone list and its new point total,
 * returns the milestones newly crossed (each only once, ever, per bar
 * period — diffing against `claimedMilestones` rather than an old/new
 * point delta makes this safe to call however/whenever, not just exactly
 * once per point-add) and the combined reward to pay out for them. Does
 * NOT mutate its inputs — the caller persists the returned claimed list.
 * Not currently wired into server.js's live path — reaching a milestone's
 * threshold no longer auto-pays it out, see claimBarMilestone there for
 * the explicit-claim flow that replaced calling this on every point-add.
 * Left here as a reusable pure helper (e.g. for a future "N milestones
 * ready to claim" display) since the math is still exactly right. */
function resolveBarMilestoneCrossings(scope, points, claimedMilestones) {
  const claimed = new Set(claimedMilestones || []);
  const rewardTable = QUEST_BAR_MILESTONE_REWARDS[scope] || {};
  const newlyCrossed = [];
  let reward = { gold: 0, gems: 0 };
  for (const m of QUEST_BAR_MILESTONES) {
    if (points >= m && !claimed.has(m)) {
      newlyCrossed.push(m);
      claimed.add(m);
      const r = rewardTable[m] || {};
      reward = { gold: reward.gold + (r.gold || 0), gems: reward.gems + (r.gems || 0) };
    }
  }
  return { newlyCrossed, reward, claimedMilestones: [...claimed].sort((a, b) => a - b) };
}

module.exports = {
  CardDB, CardById, CARD_LIBRARY_HASH, CARD_LIBRARY_RAW, PACK_DEFS, PackById, RARITY_ORDER, rarityRank,
  Effects, hasEffect, applyEffectToCard, isImmuneTo, processEffects, checkCardDeath, decayHandEffects,
  createCard, generateDeck, buildDeckFromIds, isDeckLegal, freshSide,
  executeAttack, applyRocksOnSwap, isMatchOver, applyDeployAbility,
  killCard, executeRevive, applySynergies, attackDefFor, aliveCreatureCount,
  reviveClassFor, grantReviveCharges, processReviveQueue, REVIVE_CLASS_TABLE,
  applySetBonuses, CARD_SET_MEMBERS, SET_STAT_BONUS,
  openPack, rollRarityFromWeights, pickCardOfRarity, generatePackCards,
  QUEST_TRACK, QUEST_DEFS, QuestById, questDefsForTrack, questDefsForScope,
  QUEST_BAR_MILESTONES, QUEST_BAR_MILESTONE_REWARDS, PERMANENT_PERIOD_KEY,
  dailyPeriodKey, weeklyPeriodKey, periodKeyForScope, resolveBarMilestoneCrossings,
  DECK_SIZE, MAX_CREATURES, deckClassificationOk,
  RANK_TIERS, RANK_SUBS, RANK_POINTS_PER_SUB, RANK_POINTS_PER_TIER, RANK_MAX_POINTS, getRank, rankThreshold,
};
