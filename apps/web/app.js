// app.js — Arrow-first with Parquet fallback — manifest-aware by subdir/overrides
import * as duckdb from "https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.28.0/+esm";

const $id = (id) => document.getElementById(id);
const statusEl = $id("status");
const metaEl = $id("meta");
const errorEl = $id("error");
const resultEl = $id("result");
const sqlBox = $id("sql");
const examples = $id("examples");

const setStatus = (t, bg = "#eef5ff") => { if (statusEl) { statusEl.textContent = t; statusEl.style.background = bg; } };
const showError = (m) => { if (errorEl) errorEl.textContent = m || ""; };

// ---------- config helpers ----------
function getMeta(name) { const el = document.querySelector(`meta[name="${name}"]`); return el?.content || ""; }
function getQ(name) { return new URLSearchParams(location.search).get(name) || ""; }

function resolveDataSubdir() {
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

async function registerTablesFromJSON(conn){
  const sub = resolveDataSubdir();
  const trySubs = sub === "fixed" ? ["fixed","local"] : ["local","fixed"];
  let tables=null, usedSub=null;
  for (const s of trySubs){
    const u = new URL(`data/${s}/state_diff/_tables.json`, document.baseURI);
    const r = await fetch(u.href, {cache:"no-store"});
    if (r.ok){ tables = await r.json(); usedSub=s; break; }
  }
  if (!tables) throw new Error("missing _tables.json in both fixed and local");

  try { await conn.query("INSTALL httpfs;"); await conn.query("LOAD httpfs;"); } catch {}

  const EMPTY_SCHEMAS = {
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

  const toAbs = (rel) => new URL(rel, document.baseURI).href.replace(/#/g, "%23");
  const CHUNK = 16;
  const loaded = [];

  for (const viewName of Object.keys(EMPTY_SCHEMAS)){
    const files = (tables[viewName] || []).filter(x => typeof x === "string");
    if (!files.length){
      await conn.query(`CREATE OR REPLACE VIEW ${viewName} AS ${EMPTY_SCHEMAS[viewName]}`);
      loaded.push(`${viewName}(0)`);
      continue;
    }
    const chunks = [];
    for (let i=0;i<files.length;i+=CHUNK){
      const part = files.slice(i,i+CHUNK)
        .map(f => `SELECT * FROM read_parquet('${toAbs(f)}')`)
        .join(" UNION ALL ");
      const v = `v_${viewName}_${(i/CHUNK)|0}`;
      await conn.query(`CREATE OR REPLACE TEMP VIEW ${v} AS ${part}`);
      chunks.push(v);
    }
    await conn.query(
      `CREATE OR REPLACE VIEW ${viewName} AS ${chunks.map(v=>`SELECT * FROM ${v}`).join(" UNION ALL ")}`
    );
    loaded.push(`${viewName}(${files.length})`);
  }

  const metaEl = document.getElementById("meta");
  if (metaEl) metaEl.textContent = `Tables: ${loaded.join(", ")} · Source: /data/${usedSub}/state_diff/_tables.json`;
}

function resolveManifestURL() {
  const override = window.HX_MANIFEST_URL || getMeta("hx-manifest-url") || getQ("manifest");
  if (override) return new URL(override, document.baseURI);
  const sub = resolveDataSubdir();
  return new URL(`data/${sub}/manifest.json`, document.baseURI);
}

// ---------- render ----------
function renderTable(table) {
  const rows = table.toArray(); const cols = table.schema.fields.map(f => f.name);
  let html = "<table><thead><tr>"; for (const c of cols) html += `<th>${c}</th>`; html += "</tr></thead><tbody>";
  for (const r of rows) {
    html += "<tr>"; for (const c of cols) {
      let v = r[c];
      if (v && (v.BYTES_PER_ELEMENT || v instanceof ArrayBuffer)) {
        const b = v instanceof ArrayBuffer ? new Uint8Array(v) : new Uint8Array(v.buffer || v);
        const hex = Array.from(b.slice(0, 16)).map(x => x.toString(16).padStart(2, '0')).join('');
        v = `0x${hex}${b.length > 16 ? '…' : ''}`;
      }
      html += `<td>${(v === null || v === undefined) ? "" : String(v)}</td>`;
    } html += "</tr>";
  }
  html += "</tbody></table>"; resultEl.innerHTML = html;
}

// ---------- boot ----------
function pickPreferBrowser(bundles) { return bundles.browser ?? bundles.mvp ?? Object.values(bundles)[0]; }
async function sameOriginWorkerURL(url) { const r = await fetch(url, { cache: "no-store" }); if (!r.ok) throw new Error("fetch worker " + r.status); return URL.createObjectURL(new Blob([await r.text()], { type: "text/javascript" })); }

async function boot() {
  try {
    setStatus("Booting…");
    const bundles = duckdb.getJsDelivrBundles();
    const bundle = pickPreferBrowser(bundles);
    const workerURL = await sameOriginWorkerURL(bundle.mainWorker);
    const worker = new Worker(workerURL);
    const db = new duckdb.AsyncDuckDB(new duckdb.ConsoleLogger(), worker);
    await db.instantiate(bundle.mainModule);
    const conn = await db.connect();

    setStatus("Loading tables…");
    await registerTablesFromJSON(conn);
    setStatus("Ready");

    $id("run").onclick = async () => {
      const sql = ($id("sql").value || "").trim(); if (!sql) return;
      const t0 = performance.now();
      try { const tbl = await conn.query(sql); renderTable(tbl); setStatus(`Done in ${(performance.now() - t0).toFixed(0)} ms`); }
      catch (e) { console.error(e); setStatus("Error", "#ffecec"); showError(e?.message || String(e)); }
    };
    $id("fill").onclick = () => { const q = $id("examples").value; if (q) $id("sql").value = q; };
    $id("init").onclick = async () => {
      showError("");
      resultEl.innerHTML = "";
      setStatus("Reloading tables…");
      await registerTablesFromJSON(conn);
      setStatus("Ready");
    };
  } catch (e) { console.error(e); setStatus("Boot error", "#ffecec"); showError(e?.message || String(e)); }
}
boot();
