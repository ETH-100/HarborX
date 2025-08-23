// apps/web/wallets.js — wallet share counts ONLY known wallets (unknowns excluded)
// and excludes 0x1 from Top Storage Writers; removed Active Accounts & Declared Classes sections
import * as duckdb from "https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.28.0/+esm";

/** ---------- config & helpers ---------- **/
const $ = (id)=>document.getElementById(id);
const toAbs = (rel)=> new URL(rel, document.baseURI).href.replace(/#/g,"%23");

// Only confirmed wallet mappings (unknowns are excluded from stats)
const DEFAULT_WALLET_BY_CLASS = {
  "0x36078334509b514626504edc9fb252328d1a240e4e948bef8d0c08dff45927f": "Argent X v0.4.0",
  "0x1a736d6ed154502257f02b1ccdf4d9d1089f80811cd6acad48e6b6a9d1f2003": "Argent (Cairo 1)",
  "0x25ec026985a3bf9d0cc1fe17326b245dfdc3ff89b8fde106542a3ea56c5a918": "Argent X Proxy (v0.2.x)",
  "0x4c6d6cf894f8bc96bb9c525e6853e5483177841f7388f74a46cfda6f028c755": "OpenZeppelin Account",
};

function resolveDataSubdir(){
  const ov = (window.HX_DATA_SUBDIR
    || (document.querySelector('meta[name="hx-data-subdir"]')?.content || "")
    || new URLSearchParams(location.search).get("data_subdir")
    || new URLSearchParams(location.search).get("mode")
    || "").toLowerCase();
  if (ov === "fixed" || ov === "local") return ov;
  const host = location.hostname;
  if (host === "localhost" || host === "127.0.0.1") return "local";
  return "fixed";
}

async function sameOriginWorkerURL(url){
  const r = await fetch(url, {cache:"no-store"});
  if (!r.ok) throw new Error("fetch worker "+r.status);
  return URL.createObjectURL(new Blob([await r.text()], {type:"text/javascript"}));
}

function renderBarChart(el, rows, labelKey, valueKey){
  if (!el) return;
  const total = rows.reduce((s,r)=> s + Number(r[valueKey]||0), 0) || 1;
  el.innerHTML = rows.map(r=>{
    const v = Number(r[valueKey]||0);
    const pct = v*100/total;
    return `<div style="display:flex;align-items:center;gap:8px;margin:12px 0">
      <div style="min-width:280px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis" title="${r[labelKey]}">${r[labelKey]||""}</div>
      <div style="flex:1" class="pct"><div style="height:10px;background:#3b82f6;border-radius:999px;width:${Math.max(2,pct)}%"></div></div>
      <div style="min-width:90px;text-align:right">${v.toLocaleString()}</div>
    </div>`;
  }).join("");
}

function renderTable(el, rows, columns){
  if (!el) return;
  const head = `<tr>${columns.map(c=>`<th>${c}</th>`).join("")}</tr>`;
  const body = rows.map(r=>`<tr>${columns.map(c=>`<td>${(r[c]??"")}</td>`).join("")}</tr>`).join("");
  el.innerHTML = `<table style="border-collapse:collapse;width:100%;font-size:13px">
    <thead>${head}</thead>
    <tbody>${body}</tbody>
  </table>`;
}

/** ---------- tables-first: load views from _tables.json ---------- **/
async function ensureEmptyViews(conn){
  const empties = {
    storage_diffs: `
      SELECT CAST(NULL AS VARCHAR) AS address,
             CAST(NULL AS VARCHAR) AS key,
             CAST(NULL AS VARCHAR) AS value
      WHERE 1=0`,
    declared_classes: `
      SELECT CAST(NULL AS VARCHAR) AS class_hash,
             CAST(NULL AS VARCHAR) AS compiled_class_hash
      WHERE 1=0`,
    deployed_or_replaced: `
      SELECT CAST(NULL AS VARCHAR) AS address,
             CAST(NULL AS VARCHAR) AS class_hash
      WHERE 1=0`,
    nonces: `
      SELECT CAST(NULL AS VARCHAR) AS contract_address,
             CAST(NULL AS VARCHAR) AS nonce
      WHERE 1=0`,
  };
  for (const [name, sql] of Object.entries(empties)){
    await conn.query(`CREATE OR REPLACE VIEW ${name} AS ${sql}`);
  }
}

async function registerTablesFromJSON(conn){
  const sub = resolveDataSubdir();
  const trySubs = sub === "fixed" ? ["fixed","local"] : ["local","fixed"];
  let tables=null, usedSub=null, srcURL=null;
  for (const s of trySubs){
    const u = new URL(`data/${s}/state_diff/_tables.json`, document.baseURI);
    const r = await fetch(u.href,{cache:"no-store"});
    if (r.ok){ tables = await r.json(); usedSub = s; srcURL = u.href; break; }
  }
  if (!tables) throw new Error("missing _tables.json in both fixed and local");

  try { await conn.query("INSTALL httpfs;"); await conn.query("LOAD httpfs;"); } catch {}
  await ensureEmptyViews(conn);

  const CHUNK = 16;
  const loaded = [];
  for (const [viewName, filesRaw] of Object.entries(tables)){
    const files = (filesRaw||[]).filter(x=>typeof x==="string");
    if (!files.length){ loaded.push(`${viewName}(0)`); continue; }
    const chunks=[];
    for (let i=0;i<files.length;i+=CHUNK){
      const part = files.slice(i,i+CHUNK).map(f => `SELECT * FROM read_parquet('${toAbs(f)}')`).join(" UNION ALL ");
      const v = `v_${viewName}_${(i/CHUNK)|0}`;
      await conn.query(`CREATE OR REPLACE TEMP VIEW ${v} AS ${part}`);
      chunks.push(v);
    }
    await conn.query(`CREATE OR REPLACE VIEW ${viewName} AS ${chunks.map(v=>`SELECT * FROM ${v}`).join(" UNION ALL ")}`);
    loaded.push(`${viewName}(${files.length})`);
  }
  const source = resolveDataSubdir();
  if ($("meta")) $("meta").textContent = `Tables: ${loaded.join(", ")} · Source: ${source}`;
  if ($("source")) $("source").textContent = source;
}

/** ---------- wallet label mapping (unknowns excluded) ---------- **/
async function loadExternalWalletMap(){
  const sub = resolveDataSubdir();
  const trySubs = sub === "fixed" ? ["fixed","local"] : ["local","fixed"];
  for (const s of trySubs){
    const u = new URL(`data/${s}/wallet_class_map.json`, document.baseURI);
    try {
      const r = await fetch(u.href,{cache:"no-store"});
      if (r.ok) return await r.json(); // {class_hash: "Brand"}
    } catch(_e){}
  }
  return null;
}

function mergeMapping(baseMap, extra){
  // keep only non-empty & not "unknown"
  const m = {...baseMap};
  if (extra && typeof extra === "object"){
    for (const [k,v] of Object.entries(extra)){
      if (typeof k === "string" && typeof v === "string" && k.startsWith("0x")){
        const vv = v.trim();
        if (vv && vv.toLowerCase() !== "unknown"){
          m[k.toLowerCase()] = vv;
        }
      }
    }
  }
  return m;
}

async function createWalletMapTable(conn, mapping){
  await conn.query(`CREATE OR REPLACE TEMP TABLE wallet_map(class_hash TEXT PRIMARY KEY, wallet TEXT)`);
  const entries = Object.entries(mapping);
  if (!entries.length) return;
  const CHUNK = 128;
  for (let i=0;i<entries.length;i+=CHUNK){
    const part = entries.slice(i,i+CHUNK)
      .map(([h,w]) => `('${h.toLowerCase().replace(/'/g,"''")}','${w.replace(/'/g,"''")}')`)
      .join(",");
    if (part.length){
      await conn.query(`INSERT INTO wallet_map VALUES ${part}`);
    }
  }
}

/** ---------- page-specific rendering ---------- **/
async function main(){
  // Boot DuckDB
  if ($("meta")) $("meta").textContent = "Booting…";
  const bundles = duckdb.getJsDelivrBundles();
  const bundle = bundles.browser ?? bundles.mvp ?? Object.values(bundles)[0];
  const workerURL = await sameOriginWorkerURL(bundle.mainWorker);
  const db = new duckdb.AsyncDuckDB(new duckdb.ConsoleLogger(), new Worker(workerURL));
  await db.instantiate(bundle.mainModule);
  const conn = await db.connect();

  // Load tables
  if ($("meta")) $("meta").textContent = "Loading tables…";
  await registerTablesFromJSON(conn);

  // Build wallet_map（unknowns excluded)
  const ext = await loadExternalWalletMap();
  const mapping = mergeMapping(DEFAULT_WALLET_BY_CLASS, ext);
  await createWalletMapTable(conn, mapping);

  if ($("meta")) $("meta").textContent = "Ready";

  // ---- Charts & Tables ----

  // A) Wallet share by brand (ONLY known wallets; unknowns excluded via INNER JOIN)
  {
    const q = `
      SELECT w.wallet, COUNT(*) AS cnt
      FROM deployed_or_replaced d
      INNER JOIN wallet_map w USING(class_hash)
      GROUP BY 1
      ORDER BY 2 DESC
      LIMIT 12;
    `;
    const tbl = await conn.query(q);
    renderBarChart($("chart-classes"), tbl.toArray(), "wallet", "cnt");
  }

  // B) Top storage writers — exclude 0x1 and nulls
  {
    const q = `
      SELECT address, COUNT(*) AS writes
      FROM storage_diffs
      WHERE address IS NOT NULL
        AND lower(address) <> '0x1'
      GROUP BY 1
      ORDER BY 2 DESC
      LIMIT 20;
    `;
    const tbl = await conn.query(q);
    renderBarChart($("chart-storage"), tbl.toArray(), "address", "writes");
  }

  // C) Latest nonce sample
  {
    const q = `
      SELECT contract_address, MAX(nonce) AS latest_nonce
      FROM nonces
      GROUP BY 1
      ORDER BY 1
      LIMIT 50;
    `;
    const tbl = await conn.query(q);
    renderTable($("tbl-latest"), tbl.toArray(), ["contract_address","latest_nonce"]);
  }

  // Reload
  const reloadBtn = $("reload");
  if (reloadBtn){
    reloadBtn.onclick = async ()=>{
      if ($("meta")) $("meta").textContent = "Reloading…";
      await registerTablesFromJSON(conn);
      const ext2 = await loadExternalWalletMap();
      const mapping2 = mergeMapping(DEFAULT_WALLET_BY_CLASS, ext2);
      await createWalletMapTable(conn, mapping2);
      location.reload();
    };
  }
}

main().catch(e=>{
  console.error(e);
  if ($("meta")) $("meta").textContent = "Init error";
});
