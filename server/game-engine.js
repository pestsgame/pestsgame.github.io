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

/* ── EFFECT REGISTRY (verbatim rules, DOM calls stripped to events) ── */
// each logic(card, ctx) mutates card.currentHp / ctx.skipTurn / ctx.cancelAttack
// and may push a {type:'vfx', ...} event onto ctx.events for the client to animate.
const Effects = {
  bleed:        { trigger:'onTurnStart', dmg:10,  logic(c,x){dot(c,x,10,'bleed');} },
  poison:       { trigger:'onTurnStart', dmg:25,  logic(c,x){dot(c,x,25,'poison');} },
  strongPoison: { trigger:'onTurnStart', dmg:50,  logic(c,x){dot(c,x,50,'strongPoison');} },
  mythicPoison: { trigger:'onTurnStart', dmg:75,  logic(c,x){dot(c,x,75,'mythicPoison');} },
  curse:        { trigger:'onTurnStart', logic(){} },
  confusion:    { trigger:'onAttack',    logic(c,x){ if (Math.random()<.5) x.cancelAttack = true; } },
  sleep:        { trigger:'onTurnStart', logic(c,x){ rollSkip(c,x,'sleep'); } },
  paralyze:     { trigger:'onTurnStart', logic(c,x){ rollSkip(c,x,'paralyze'); } },
  burn:         { trigger:'onTurnStart', dmg:10,  logic(c,x){dot(c,x,10,'burn');} },
  shock:        { trigger:'onTurnStart', dmg:25,  logic(c,x){dot(c,x,25,'shock');} },
  soak:         { trigger:'onTurnStart', logic(){} },
  cryo:         { trigger:'onTurnStart', logic(c,x){ dot(c,x,10,'cryo',false); rollSkip(c,x,'cryo',true); } },
  rocks:        { trigger:'onSwap',      logic(){} },
};
function dot(card, ctx, dmg, type) {
  card.currentHp -= dmg;
  ctx.events.push({ t:'dot', side: ctx.side, slot: ctx.slot, card:card.instanceId, dmg, effect:type });
}
function rollSkip(card, ctx, type, alreadyHit) {
  if (Math.random() < .5) {
    ctx.skipTurn = true;
    ctx.events.push({ t:'status', side: ctx.side, slot: ctx.slot, card:card.instanceId, effect:type, hit:true });
  } else {
    ctx.events.push({ t:'status', side: ctx.side, slot: ctx.slot, card:card.instanceId, effect:type, hit:false });
  }
}
function hasEffect(card, type) { return !!card && card.activeEffects.some(e => e.type === type); }

/* ── REVIVE PASSIVE ──────────────────────────────────────────────────
 * topEffect.revive = {
 *   guaranteed: 2,      // this many revives always succeed, no roll, consumed first
 *   chance: 0.3,        // once `guaranteed` is used up (or if it's omitted), each death
 *                       // instead rolls this % chance to revive, indefinitely
 *   healPercent: 0.5,   // fraction of maxHp restored on a successful revive (default 0.5)
 * }
 * A card can have guaranteed-only, chance-only, or both (guaranteed revives first,
 * falling back to the % chance after they run out) — covers all three modes asked for. */
function tryRevive(card, events, side, slotKey) {
  const revive = card.topEffect && card.topEffect.type === 'passive' && card.topEffect.revive;
  if (!revive) return false;
  let revived = false;
  if (card.reviveGuaranteedLeft > 0) {
    card.reviveGuaranteedLeft--;
    revived = true;
  } else if (revive.chance && Math.random() < revive.chance) {
    revived = true;
  }
  if (!revived) return false;
  const pct = revive.healPercent != null ? revive.healPercent : 0.5;
  card.currentHp = Math.max(1, Math.round(card.maxHp * pct));
  card.activeEffects = []; // shed lingering DOTs/statuses on revive
  events.push({ t:'revive', side, slot:slotKey, card:card.instanceId, name:card.name, hp:card.currentHp, maxHp:card.maxHp });
  return true;
}

/* ── SYNERGY PASSIVE ──────────────────────────────────────────────────
 * topEffect.synergy = {
 *   partnerId: 'other_card_base_id',
 *   bonusHp: 20,        // optional — added to max/current HP if partner is in the same deck
 *   bonusDamage: 10,    // optional — added to every attack this card lands
 *   shareAttack: true,  // optional — if the partner is in the same deck, this card's
 *                       // ENTIRE topEffect is replaced with an attack copy of the
 *                       // partner's bottomAttack. Without the partner, whatever
 *                       // topEffect this card was declared with (usually a plain
 *                       // non-attack passive/ability) is what it's stuck with — so
 *                       // that top slot is only useful when the pair is together.
 * }
 * Applied once, at deck-build time, over every card instance on a side (deck+hand
 * combined) — so it reflects deck *composition*, not what's currently drawn/deployed.
 * Give the block to just one card for a one-directional share, or to both (each
 * pointing at the other) so they mutually swap top slots for each other's bottom
 * attack. */
function applySynergies(cardInstances) {
  cardInstances.forEach(card => {
    if (!card || card.cardType) return; // skip weapon/defense equipment
    const syn = card.topEffect && card.topEffect.type === 'passive' && card.topEffect.synergy;
    if (!syn || !syn.partnerId) return;
    const partner = cardInstances.find(c => c && !c.cardType && c.baseId === syn.partnerId && c !== card);
    if (!partner) return;
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
  });
}

function applyEffectToCard(target, effectDef) {
  const eDef = Effects[effectDef.type]; if (!eDef) return;
  const ex = target.activeEffects.find(e => e.type === effectDef.type);
  if (ex) { if (effectDef.duration < 9999) ex.duration = Math.min(9999, ex.duration + effectDef.duration); }
  else target.activeEffects.push({ type: effectDef.type, duration: effectDef.duration });
}

/** `side` (0|1) is whichever side owns `entity` — stamped onto every event so
 * a client on either side of the match can map it back to its own DOM
 * (its own board is always "player-*", the opponent's is always "enemy-*"). */
function processEffects(entity, trigger, ctx, side) {
  [['activeCard','slot1'], ['activeCard2','slot2']].forEach(([key, slotKey]) => {
    const card = entity[key];
    if (!card) return;
    for (let i = card.activeEffects.length - 1; i >= 0; i--) {
      const ed = card.activeEffects[i]; const eDef = Effects[ed.type];
      if (!eDef || eDef.trigger !== trigger) continue;
      ctx.side = side; ctx.slot = slotKey;
      eDef.logic(card, ctx);
      if (trigger === 'onTurnStart' && ed.duration < 9999) {
        ed.duration--; if (ed.duration <= 0) card.activeEffects.splice(i, 1);
      }
    }
  });
}

function checkCardDeath(entity, events, side) {
  [['activeCard','slot1'], ['activeCard2','slot2']].forEach(([key, slotKey]) => {
    const c = entity[key];
    if (c && c.currentHp <= 0) {
      if (tryRevive(c, events, side, slotKey)) return;
      events.push({ t:'death', side, slot: slotKey, card:c.instanceId, name:c.name });
      entity[key] = null;
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
    card.reviveGuaranteedLeft = base.topEffect.revive.guaranteed || 0;
  } else {
    card.reviveGuaranteedLeft = 0;
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
      card.topEffect.effects.forEach(eff => applyEffectToCard(target, eff));
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

function generateDeck(n) {
  const creatures = CardDB.filter(c => !c.cardType);
  const equipment = CardDB.filter(c => c.cardType === 'weapon' || c.cardType === 'defense');
  const d = [];
  for (let i = 0; i < n; i++) {
    const useEquip = equipment.length > 0 && Math.random() < 0.3;
    const pool = useEquip ? equipment : creatures;
    const p = pool[Math.floor(Math.random() * pool.length)];
    const c = createCard(p.id); if (c) d.push(c);
  }
  return d;
}

/** Builds a validated deck of live card instances from a list of owned card ids. */
/** A deck is legal if it's 4–10 cards long and every single id in it exists
 * in the canonical library — checked fresh every time a match is built, not
 * just once when the deck was saved, so a stale/tampered deck never quietly
 * slips through with some cards silently dropped. */
function isDeckLegal(ids) {
  return Array.isArray(ids) && ids.length >= 4 && ids.length <= 10 && ids.every(id => !!CardById[id]);
}

function buildDeckFromIds(ids) {
  if (!isDeckLegal(ids)) return generateDeck(10);
  return ids.map(id => createCard(id));
}

/* ── PLAYER SIDE FACTORY ──────────────────────────────────────────── */
function freshSide(deck) {
  const d = [...deck];
  applySynergies(d); // deck+hand together — synergy is about composition, not what's drawn yet
  return {
    hp: 100, maxHp: 100,
    activeCard: null, activeCard2: null, weaponCard: null, defenseCard: null,
    deck: d, hand: d.splice(0, 4),
  };
}

/* ── COMBAT ───────────────────────────────────────────────────────── */
// slotKey: 'slot1' | 'slot2'
function cardInSlot(entity, slotKey) { return slotKey === 'slot1' ? entity.activeCard : entity.activeCard2; }
function slotOfCard(entity, card) { return entity.activeCard === card ? 'slot1' : 'slot2'; }

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
 * durability, elemental passive reduction, curse recoil, and revive-on-death checks.
 * Returns { stop:true } when the attacker or (non-revived) target died — signalling
 * a multi-attack sequence should not continue. */
function performHit(match, side, atkSlotKey, targetSlotKey, atkDef, ac, atkEntity, defEntity, defSide, events) {
  if (hasEffect(ac, 'confusion') && Math.random() < .5) { events.push({t:'miss',side,slot:atkSlotKey,card:ac.instanceId,cause:'confusion'}); return { stop:false }; }
  if (hasEffect(ac, 'shock') && Math.random() < .5)     { events.push({t:'miss',side,slot:atkSlotKey,card:ac.instanceId,cause:'shock'});     return { stop:false }; }
  if (hasEffect(ac, 'soak') && Math.random() < .5)      { events.push({t:'miss',side,slot:atkSlotKey,card:ac.instanceId,cause:'soak'});      return { stop:false }; }

  if (atkDef.heal) return performHeal(atkEntity, atkSlotKey, ac, atkDef, events, side);

  const targetCard = targetSlotKey ? cardInSlot(defEntity, targetSlotKey) : null;

  let wBonus = 0;
  if (atkEntity.weaponCard) {
    wBonus = atkEntity.weaponCard.flatBonus; atkEntity.weaponCard.currentDurability--;
    events.push({ t:'weapon_use', side, bonus:wBonus, breaks: atkEntity.weaponCard.currentDurability<=0 });
    if (atkEntity.weaponCard.currentDurability <= 0) atkEntity.weaponCard = null;
  }
  let dReduce = 0;
  if (defEntity.defenseCard) {
    dReduce = defEntity.defenseCard.flatBonus; defEntity.defenseCard.currentDurability--;
    events.push({ t:'defense_use', side:defSide, bonus:dReduce, breaks: defEntity.defenseCard.currentDurability<=0 });
    if (defEntity.defenseCard.currentDurability <= 0) defEntity.defenseCard = null;
  }

  let dmg = atkDef.damage + wBonus + (ac.synergyDamageBonus || 0);
  if (hasEffect(ac, 'burn')) { dmg = Math.floor(dmg * .5); events.push({t:'burn_penalty', side, slot:atkSlotKey}); }

  const tgtSlotKey = targetCard ? slotOfCard(defEntity, targetCard) : null;

  if (!targetCard) {
    dmg = Math.max(0, dmg - dReduce);
    defEntity.hp -= dmg;
    events.push({ t:'hit', atkSide:side, atkSlot:atkSlotKey, atkCard:ac.instanceId, defSide, defSlot:null, tgtCard:null, direct:true, dmg, name:atkDef.name, element:atkDef.element });
    return { stop:false };
  }

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
  (atkDef.effects || []).forEach(eff => applyEffectToCard(targetCard, eff));

  if (hasEffect(targetCard, 'curse')) {
    const r = Math.floor(dmg * .25); ac.currentHp -= r;
    events.push({ t:'curse_recoil', side, slot:atkSlotKey, card:ac.instanceId, dmg:r });
    if (ac.currentHp <= 0) {
      if (!tryRevive(ac, events, side, atkSlotKey)) {
        events.push({ t:'death', side, slot:atkSlotKey, card:ac.instanceId, name:ac.name });
        if (atkEntity.activeCard === ac) atkEntity.activeCard = null; else atkEntity.activeCard2 = null;
        return { stop:true };
      }
    }
  }
  if (targetCard.currentHp <= 0) {
    if (tryRevive(targetCard, events, defSide, tgtSlotKey)) return { stop:false };
    const ex = Math.abs(targetCard.currentHp);
    events.push({ t:'death', side:defSide, slot:tgtSlotKey, card:targetCard.instanceId, name:targetCard.name });
    if (defEntity.activeCard === targetCard) defEntity.activeCard = null; else defEntity.activeCard2 = null;
    if (ex > 0) { defEntity.hp -= ex; events.push({ t:'excess', side:defSide, dmg:ex }); }
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

/** rocks trap — triggered when a card is deployed/swapped into the active slot facing a 'rocks' holder.
 * `side`/`slot` describe the *incoming* card (the side that just deployed). */
function triggerRocks(defendingSideEntity, incomingCard, events, side, slot) {
  const holder = defendingSideEntity.activeCard;
  if (!holder) return;
  const re = holder.activeEffects.find(e => e.type === 'rocks'); if (!re) return;
  incomingCard.currentHp -= 50;
  events.push({ t:'rocks', side, slot, card:incomingCard.instanceId, dmg:50 });
}

function isMatchOver(match) {
  if (match.sides[0].hp <= 0) return 1; // side 1 (index) wins
  if (match.sides[1].hp <= 0) return 0;
  return null;
}

module.exports = {
  CardDB, CardById, CARD_LIBRARY_HASH, CARD_LIBRARY_RAW, PACK_DEFS, PackById, RARITY_ORDER, rarityRank,
  Effects, hasEffect, applyEffectToCard, processEffects, checkCardDeath,
  createCard, generateDeck, buildDeckFromIds, isDeckLegal, freshSide,
  executeAttack, triggerRocks, isMatchOver, applyDeployAbility,
  tryRevive, applySynergies, attackDefFor,
  openPack, rollRarityFromWeights, pickCardOfRarity, generatePackCards,
};
