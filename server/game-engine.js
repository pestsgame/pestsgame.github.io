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

/* ── CARD DATABASE (verbatim from client) ─────────────────────────── */
const CardDB = [
  {id:"venom_spider",rarity:"common",name:"Skrix the Venom Weaver",hp:80,types:["mob"],classification:"pests",image:"spider.png",
   topEffect:{type:"passive",name:"Toxic Hide",value:0,effects:[],
     passiveReduction:{flat:10,exceptElements:['lightning']},
     description:"−10 flat dmg. Immune to Poison. Lightning bypasses."},
   bottomAttack:{name:"Venom Bite",damage:20,element:"poison",effects:[{type:"poison",duration:3}]}},
  {id:"plague_rat",rarity:"common",name:"Gruk the Plague Spreader",hp:60,types:["mob"],classification:"pests",image:"rat.png",
   topEffect:{type:"attack",name:"Gnaw",value:15,element:"shadow",effects:[{type:"bleed",duration:2}]},
   bottomAttack:{name:"Plague Bite",damage:10,element:"poison",effects:[{type:"strongPoison",duration:2}]}},
  {id:"ember_witch",rarity:"uncommon",name:"Solvara the Ember Witch",hp:70,types:["wizard"],classification:"pests",image:"witch.png",
   topEffect:{type:"ability",name:"Hex Aura",value:0,element:"arcane",effects:[{type:"curse",duration:3}],description:"On deploy: enemy gains Curse."},
   bottomAttack:{name:"Fireball",damage:30,element:"fire",effects:[{type:"burn",duration:2}]}},
  {id:"frost_crawler",rarity:"uncommon",name:"Glacius the Frost Crawler",hp:100,types:["mob"],classification:"pests",image:"crawler.png",
   topEffect:{type:"passive",name:"Icy Shell",value:0,element:null,effects:[{type:"soak",duration:9999}],
     passiveReduction:{percent:0.35,exceptElements:['fire']},
     description:"−35% incoming dmg. Fire bypasses. Always Soaked."},
   bottomAttack:{name:"Frost Bite",damage:25,element:"ice",effects:[{type:"cryo",duration:2}]}},
  {id:"shock_beetle",rarity:"common",name:"Zoltaxx the Storm Beetle",hp:65,types:["mob"],classification:"pests",image:"beetle.png",
   topEffect:{type:"passive",name:"Static Field",value:0,element:"lightning",effects:[],
     passiveReduction:{percent:0.20,exceptElements:['water','ice']},
     description:"−20% incoming dmg. Water & Ice bypass."},
   bottomAttack:{name:"Zap Sting",damage:22,element:"lightning",effects:[{type:"shock",duration:2},{type:"confusion",duration:1}]}},
  {id:"shadow_rat",rarity:"common",name:"Nyxor the Shadow Gnawer",hp:55,types:["mob"],classification:"pests",image:"shadow_rat.png",
   topEffect:{type:"attack",name:"Ambush",value:25,element:"shadow",effects:[{type:"paralyze",duration:1}]},
   bottomAttack:{name:"Shadow Bite",damage:18,element:"shadow",effects:[{type:"sleep",duration:1}]}},
  {id:"cursed_golem",rarity:"rare",name:"Thrakk the Cursed Colossus",hp:130,types:["mob"],classification:"pests",image:"golem.png",
   topEffect:{type:"passive",name:"Stone Curse",value:0,element:null,effects:[{type:"curse",duration:9999}],
     passiveReduction:{flat:18,exceptElements:['water']},
     description:"−18 flat dmg. 25% recoil on attackers. Water bypasses."},
   bottomAttack:{name:"Boulder Slam",damage:35,element:"earth",effects:[{type:"rocks",duration:2}]}},
  {id:"swarm_queen",rarity:"rare",name:"Vexa the Swarm Empress",hp:120,types:["mob","wizard"],classification:"boss",image:"queen.png",
   topEffect:{type:"attack",name:"Summon Swarm",value:30,element:"nature",effects:[{type:"bleed",duration:3},{type:"poison",duration:2}]},
   bottomAttack:{name:"Queen's Wrath",damage:35,element:"poison",effects:[{type:"mythicPoison",duration:1}]}},
  {id:"plague_dragon",rarity:"epic",name:"Morthaax the Plague Wyrm",hp:200,types:["dragon"],classification:"boss",image:"dragon.png",
   topEffect:{type:"passive",name:"Plague Hide",value:0,element:"poison",effects:[],
     passiveReduction:{percent:0.25,exceptElements:['wind','nature']},
     description:"−25% incoming dmg. Wind & Nature bypass."},
   bottomAttack:{name:"Dragon Crush",damage:55,element:"earth",effects:[{type:"rocks",duration:2},{type:"strongPoison",duration:3},{type:"burn",duration:2}]}},
  {id:"the_overlord",rarity:"mythic",name:"Vaelkor the Infinite Overlord",hp:300,types:["mob","wizard","dragon"],classification:"overlord",image:"overlord.png",
   topEffect:{type:"passive",name:"Void Mantle",value:0,element:"arcane",effects:[],
     passiveReduction:{percent:0.40,flat:15,exceptElements:['arcane']},
     description:"−40% then −15 flat incoming dmg. Arcane bypasses."},
   bottomAttack:{name:"World's End",damage:90,element:"fire",effects:[{type:"bleed",duration:3},{type:"burn",duration:2},{type:"shock",duration:2},{type:"rocks",duration:2},{type:"mythicPoison",duration:2},{type:"curse",duration:3},{type:"cryo",duration:2}]}},
  {id:"iron_blade",    rarity:"common",  cardType:"weapon", name:"Iron Blade",     flatBonus:15, maxDurability:3, image:"sword.png"},
  {id:"venom_dagger",  rarity:"common",  cardType:"weapon", name:"Venom Dagger",   flatBonus:10, maxDurability:5, image:"dagger.png"},
  {id:"war_axe",       rarity:"uncommon",cardType:"weapon", name:"War Axe",        flatBonus:28, maxDurability:2, image:"axe.png"},
  {id:"cursed_blade",  rarity:"rare",    cardType:"weapon", name:"Cursed Blade",   flatBonus:20, maxDurability:3, image:"cblade.png"},
  {id:"inferno_shard", rarity:"epic",    cardType:"weapon", name:"Inferno Shard",  flatBonus:35, maxDurability:1, image:"shard.png"},
  {id:"wooden_shield", rarity:"common",  cardType:"defense", name:"Wooden Shield",  flatBonus:10, maxDurability:3, image:"shield.png"},
  {id:"iron_armor",    rarity:"uncommon",cardType:"defense", name:"Iron Armor",     flatBonus:20, maxDurability:2, image:"armor.png"},
  {id:"barrier_rune",  rarity:"common",  cardType:"defense", name:"Barrier Rune",   flatBonus:15, maxDurability:4, image:"rune.png"},
  {id:"dragon_scale",  rarity:"epic",    cardType:"defense", name:"Dragon Scale",   flatBonus:30, maxDurability:2, image:"dscale.png"},
  {id:"mirror_ward",   rarity:"legendary",cardType:"defense", name:"Mirror Ward",    flatBonus:12, maxDurability:5, image:"ward.png"}
];
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
  return card;
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
function buildDeckFromIds(ids) {
  if (!Array.isArray(ids) || ids.length < 4) return generateDeck(10);
  const cards = ids.map(id => createCard(id)).filter(Boolean);
  return cards.length >= 4 ? cards : generateDeck(10);
}

/* ── PLAYER SIDE FACTORY ──────────────────────────────────────────── */
function freshSide(deck) {
  const d = [...deck];
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
 * Executes one attack. `chosenAttackIndex` is 0 (top) or 1 (bottom) — always
 * an explicit player/opponent choice, never randomized, so this function is
 * used for both human turns and (with a server-side random index) AI/bot turns.
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

  if (hasEffect(ac, 'confusion') && Math.random() < .5) { events.push({t:'miss',side,slot:atkSlotKey,card:ac.instanceId,cause:'confusion'}); return finishAttack(match, side, atkSlotKey, events); }
  if (hasEffect(ac, 'shock') && Math.random() < .5)     { events.push({t:'miss',side,slot:atkSlotKey,card:ac.instanceId,cause:'shock'});     return finishAttack(match, side, atkSlotKey, events); }
  if (hasEffect(ac, 'soak') && Math.random() < .5)      { events.push({t:'miss',side,slot:atkSlotKey,card:ac.instanceId,cause:'soak'});      return finishAttack(match, side, atkSlotKey, events); }

  let atkDef;
  if (chosenAttackIndex === 0) {
    if (ac.topEffect.type !== 'attack') return { ok:false, reason:'top_not_attack', events };
    atkDef = { name: ac.topEffect.name, damage: ac.topEffect.value, effects: ac.topEffect.effects, element: ac.topEffect.element };
  } else {
    atkDef = ac.bottomAttack;
  }

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

  let dmg = atkDef.damage + wBonus;
  if (hasEffect(ac, 'burn')) { dmg = Math.floor(dmg * .5); events.push({t:'burn_penalty', side, slot:atkSlotKey}); }

  const tgtSlotKey = targetCard ? slotOfCard(defEntity, targetCard) : null;

  if (!targetCard) {
    dmg = Math.max(0, dmg - dReduce);
    defEntity.hp -= dmg;
    events.push({ t:'hit', atkSide:side, atkSlot:atkSlotKey, atkCard:ac.instanceId, defSide, defSlot:null, tgtCard:null, direct:true, dmg, name:atkDef.name, element:atkDef.element });
  } else {
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
    atkDef.effects.forEach(eff => applyEffectToCard(targetCard, eff));

    if (hasEffect(targetCard, 'curse')) {
      const r = Math.floor(dmg * .25); ac.currentHp -= r;
      events.push({ t:'curse_recoil', side, slot:atkSlotKey, card:ac.instanceId, dmg:r });
      if (ac.currentHp <= 0) {
        events.push({ t:'death', side, slot:atkSlotKey, card:ac.instanceId, name:ac.name });
        if (atkEntity.activeCard === ac) atkEntity.activeCard = null; else atkEntity.activeCard2 = null;
        return finishAttack(match, side, atkSlotKey, events);
      }
    }
    if (targetCard.currentHp <= 0) {
      const ex = Math.abs(targetCard.currentHp);
      events.push({ t:'death', side:defSide, slot:tgtSlotKey, card:targetCard.instanceId, name:targetCard.name });
      if (defEntity.activeCard === targetCard) defEntity.activeCard = null; else defEntity.activeCard2 = null;
      if (ex > 0) { defEntity.hp -= ex; events.push({ t:'excess', side:defSide, dmg:ex }); }
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
  CardDB, CardById, PACK_DEFS, PackById, RARITY_ORDER, rarityRank,
  Effects, hasEffect, applyEffectToCard, processEffects, checkCardDeath,
  createCard, generateDeck, buildDeckFromIds, freshSide,
  executeAttack, triggerRocks, isMatchOver,
  openPack, rollRarityFromWeights, pickCardOfRarity, generatePackCards,
};
