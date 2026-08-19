/**
 * Adoption stats from db-sync for Ada, NIGHT, STRIKE-ADA, and SNEK2-ADA,
 * for the last 30 / 60 / 90 days.
 *
 *   DATABASE_URL=postgres://... node queries.js
 *   # or PGHOST, PGUSER, PGPASSWORD, PGDATABASE, PGPORT
 *   # REGISTRY_URL defaults to the public mainnet registry
 */

const { Client } = require('pg');

// https://public.liqwid.finance/v6/registry.json
const MARKETS = {
  Ada: {
    qToken: 'a04ce7a52545e5e33c2867e148898d9e667a69602285f6a1298f9d68',
    loanPolicies: [
      'ee944b56bab503197bdfb929509a177c3ef9e5083ca7e65ffa1469c8', // BorrowToken
      '63d7e1501efb7dfda4915597658552e1c3e35e6f34cdc74817fad1dc', // NewLoanPolicy
    ],
    liquidation: '3c9ee71d8f14b386df802f4121d82746df5508e29198744bea7552ef',
  },
  NIGHT: {
    qToken: 'c45fa8aefc662c003a32be67f6a4652d8ce56bd9e54d7696efd40c86',
    loanPolicies: [
      'ff7eccf5e7db571ac2721c232636699eec80ecb4d04123a63fccbd69',
      '50ed8de918b0d944215e69688536cbe2d7484c744c4e11b9d8b9fc07',
    ],
    liquidation: 'f12a27b2a980070ffe1bdd8d1ae7572e4574febf72e4959b85e7e18f',
  },
  'STRIKE-ADA': {
    qToken: '7cc5b5e85b03b9dc18ee93162a13a911a5bedad39053506d669465e8',
    loanPolicies: [
      'ece99399fc864ff5bfad424fc1cefe3c5a3b68036d2e36c41363f5a0',
      '2a8b2a5afe4bf67e21385b70fb27af71f652022a246a95ba0b1c8252',
    ],
    liquidation: '155450afea7ce67b4c92bf2287dd7f9e06711cf1dd9767a2d6620079',
  },
  'SNEK2-ADA': {
    qToken: '7a35cf17f7d4fc14e9b5ba99cf9be338d0e05a9df3841de767728ae5',
    loanPolicies: [
      '9a40e800205a941c5986798df629f6687be0f995b4c20949aa36721a',
      'bc7f36746506b9827c4aef3f1ca6b460a3ac7689ac952e3eeaa55945',
    ],
    liquidation: 'df9b970885f5940c729ab68e3e686d8f6388d1841fe62298cf332b2f',
  },
};

const WINDOWS = [30, 60, 90];
const REGISTRY_URL =
  process.env.REGISTRY_URL ?? 'https://public.liqwid.finance/v6/registry.json';

function dbConfig() {
  if (process.env.DATABASE_URL) {
    return { connectionString: process.env.DATABASE_URL };
  }

  const { PGHOST, PGUSER, PGPASSWORD, PGDATABASE, PGPORT } = process.env;
  if (!PGHOST || !PGUSER || !PGPASSWORD || !PGDATABASE) {
    throw new Error('set DATABASE_URL, or PGHOST / PGUSER / PGPASSWORD / PGDATABASE');
  }

  return {
    host: PGHOST,
    user: PGUSER,
    password: PGPASSWORD,
    database: PGDATABASE,
    port: PGPORT ? Number(PGPORT) : 5432,
  };
}

const client = new Client(dbConfig());

function policyIs(col, hash) {
  return `encode(${col}, 'hex') = '${hash}'`;
}

function policyIn(col, hashes) {
  return `encode(${col}, 'hex') IN (${hashes.map((h) => `'${h}'`).join(', ')})`;
}

async function loanPoliciesByMarket() {
  const res = await fetch(REGISTRY_URL);
  if (!res.ok) throw new Error(`failed to fetch registry: ${res.status}`);
  const { scriptInfos } = await res.json();

  const byMarket = {};
  for (const si of scriptInfos) {
    if (si.network?.tag !== 'MainnetId') continue;
    if (si.componentName !== 'Borrow' && si.componentName !== 'NewBorrow') continue;
    if (!si.market) continue;
    (byMarket[si.market] ??= []).push(si.scriptHash);
  }
  return byMarket;
}

function otherMarketLoanPolicies(byMarket) {
  const reported = new Set(Object.keys(MARKETS));
  return Object.entries(byMarket)
    .filter(([name]) => !reported.has(name))
    .flatMap(([, hashes]) => hashes);
}

function since(days) {
  return `block.time >= NOW() - INTERVAL '${Number(days)} days'`;
}

// Wrap the join so one tx with several mint rows still counts once.
function countTxs(fromWhere) {
  return `
    SELECT COUNT(*) AS tx_count, COALESCE(SUM(fee), 0) AS fee_sum
    FROM (
      SELECT DISTINCT tx.id, tx.fee
      ${fromWhere}
    ) t
  `;
}

function loanMint(hashes, cmp) {
  return `EXISTS (
    SELECT 1
    FROM ma_tx_mint m
    JOIN multi_asset ms ON m.ident = ms.id
    WHERE m.tx_id = tx.id
      AND ${policyIn('ms.policy', hashes)}
      AND m.quantity ${cmp} 0
  )`;
}

function noLiqMint(hash) {
  return `NOT EXISTS (
    SELECT 1
    FROM ma_tx_mint m
    JOIN multi_asset ms ON m.ident = ms.id
    WHERE m.tx_id = tx.id
      AND ${policyIs('ms.policy', hash)}
      AND m.quantity > 0
  )`;
}

function qTokenQuery(policy, cmp, days) {
  return countTxs(`
    FROM tx
    JOIN block ON block.id = tx.block_id
    JOIN ma_tx_mint ON ma_tx_mint.tx_id = tx.id
    JOIN multi_asset ON multi_asset.id = ma_tx_mint.ident
    WHERE ${policyIs('multi_asset.policy', policy)}
      AND ma_tx_mint.quantity ${cmp} 0
      AND ${since(days)}
  `);
}

function loanQuery(market, kind, days) {
  const { loanPolicies, liquidation } = market;
  const loanJoin = `
    FROM tx
    JOIN block ON block.id = tx.block_id
    JOIN ma_tx_mint mint ON mint.tx_id = tx.id
    JOIN multi_asset ma ON ma.id = mint.ident
  `;

  if (kind === 'creation') {
    return countTxs(`
      ${loanJoin}
      WHERE ${policyIn('ma.policy', loanPolicies)}
        AND mint.quantity > 0
        AND NOT ${loanMint(loanPolicies, '<')}
        AND ${noLiqMint(liquidation)}
        AND ${since(days)}
    `);
  }

  if (kind === 'modification') {
    return countTxs(`
      ${loanJoin}
      WHERE ${policyIn('ma.policy', loanPolicies)}
        AND ${loanMint(loanPolicies, '>')}
        AND ${loanMint(loanPolicies, '<')}
        AND ${noLiqMint(liquidation)}
        AND ${since(days)}
    `);
  }

  if (kind === 'liquidation') {
    return countTxs(`
      ${loanJoin}
      WHERE ${policyIs('ma.policy', liquidation)}
        AND mint.quantity > 0
        AND ${since(days)}
    `);
  }

  throw new Error(`unknown loan kind: ${kind}`);
}

// qToken sitting on a loan UTxO from a market we aren't already reporting.
function collateralQuery(qToken, otherLoanPolicies, days) {
  return countTxs(`
    FROM tx
    JOIN block ON block.id = tx.block_id
    JOIN ma_tx_mint mint ON mint.tx_id = tx.id
    JOIN multi_asset ma ON ma.id = mint.ident
    JOIN tx_out ON tx_out.tx_id = tx.id
    JOIN ma_tx_out ON ma_tx_out.tx_out_id = tx_out.id
    JOIN multi_asset collat ON collat.id = ma_tx_out.ident
    WHERE ${policyIn('ma.policy', otherLoanPolicies)}
      AND mint.quantity != 0
      AND ${policyIs('collat.policy', qToken)}
      AND ${since(days)}
  `);
}

function row(res) {
  const r = res.rows[0];
  return {
    count: Number(r?.tx_count ?? 0),
    fee: Number(r?.fee_sum ?? 0) / 1_000_000,
  };
}

function fmtAda(n) {
  return n.toLocaleString('en-US', { minimumFractionDigits: 6 });
}

function totals(markets) {
  let txs = 0;
  let fees = 0;
  for (const market of Object.values(markets)) {
    for (const { count, fee } of Object.values(market)) {
      txs += count;
      fees += fee;
    }
  }
  return { txs, fees };
}

function printMarket(name, stats) {
  console.log(`\n# ${name} Market`);
  for (const [k, v] of Object.entries(stats)) {
    console.log(`  - ${k}: count=${v.count}, fee_sum=${fmtAda(v.fee)} ADA`);
  }
}

async function statsFor(market, days, otherLoans) {
  const [supplies, withdrawals, creations, mods, liqs, collat] = await Promise.all([
    client.query(qTokenQuery(market.qToken, '>', days)),
    client.query(qTokenQuery(market.qToken, '<', days)),
    client.query(loanQuery(market, 'creation', days)),
    client.query(loanQuery(market, 'modification', days)),
    client.query(loanQuery(market, 'liquidation', days)),
    client.query(collateralQuery(market.qToken, otherLoans, days)),
  ]);

  return {
    supplies: row(supplies),
    withdrawals: row(withdrawals),
    loanCreations: row(creations),
    loanModifications: row(mods),
    loanLiquidations: row(liqs),
    collateralLoans: row(collat),
  };
}

async function main() {
  const otherLoans = otherMarketLoanPolicies(await loanPoliciesByMarket());
  await client.connect();
  console.log('Liqwid Markets Adoption Analytics:');

  for (const days of WINDOWS) {
    const report = {};
    for (const [name, market] of Object.entries(MARKETS)) {
      report[name] = await statsFor(market, days, otherLoans);
    }

    const { txs, fees } = totals(report);
    console.log(`\n========== Last ${days} days ==========`);
    for (const [name, stats] of Object.entries(report)) {
      printMarket(name, stats);
    }
    console.log('\nFinal Totals:');
    console.log(`Total Tx Count: ${txs}`);
    console.log(`Total Fees Sum: ${fmtAda(fees)} ADA`);
  }

  await client.end();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
