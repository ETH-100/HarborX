// app.js — Arrow-first with Parquet fallback — manifest-aware by subdir/overrides
import * as duckdb from "https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.28.0/+esm";

const $id = (id) => document.getElementById(id);
const statusEl = $id("status");
const metaEl   = $id("meta");
const errorEl  = $id("error");
const resultEl = $id("result");
const sqlBox   = $id("sql");
const examples = $id("examples");

const setStatus = (t, bg="#eef5ff") => { if (statusEl) { statusEl.textContent = t; statusEl.style.background = bg; }};
const showError = (m) => { if (errorEl) errorEl.textContent = m || ""; };

// ---------- config helpers ----------
function getMeta(name){ const el=document.querySelector(`meta[name="${name}"]`); return el?.content || ""; }
function getQ(name){ return new URLSearchParams(location.search).get(name) || ""; }

function resolveDataSubdir(){
  // Priority: global var -> meta -> query -> heuristic(hostname)
  const ov = (window.HX_DATA_SUBDIR || getMeta("hx-data-subdir") || getQ("data_subdir") || getQ("mode") || "").toLowerCase();
  if (ov === "fixed" || ov === "local") return ov;
  // Heuristic: 线上域名走 fixed，其余走 local（可按需调整）
  return location.hostname.endsWith("harborx.tech") ? "fixed" : "local";
}

function resolveManifestURL(){
  const override = window.HX_MANIFEST_URL || getMeta("hx-manifest-url") || getQ("manifest");
  if (override) return new URL(override, document.baseURI);
  const sub = resolveDataSubdir();
  return new URL(`data/${sub}/manifest.json`, document.baseURI);
}

// ---------- data loading ----------
async function loadManifest(){
  let manifestURL = resolveManifestURL();
  // 拉取 manifest；失败时在 fixed/local 间做一次兜底切换
  let res = await fetch(manifestURL.href, { cache: "no-store" });
  if (!res.ok) {
    const sub = resolveDataSubdir();
    const alt = sub === "fixed" ? "local" : "fixed";
    const fallback = new URL(`data/${alt}/manifest.json`, document.baseURI);
    res = await fetch(fallback.href, { cache: "no-store" });
    if (!res.ok) throw new Error(`manifest fetch failed (${manifestURL} and ${fallback})`);
    manifestURL = fallback;
  }
  const m = await res.json();
  // 相对路径以 manifest 所在目录为基准
  const toAbs = (a) => (Array.isArray(a)?a:[]).map(p=> new URL(p, manifestURL).href);
  return {arrow:toAbs(m.arrow), parquet:toAbs(m.parquet), manifestURL};
}

async function buildState(conn){
  const {arrow, parquet, manifestURL} = await loadManifest();
  let engine = arrow.length ? "arrow" : (parquet.length ? "parquet" : null);
  let files  = engine==="arrow"?arrow:engine==="parquet"?parquet:[];

  if(!engine) throw new Error("manifest has no files");

  // Enable httpfs/arrow extensions
  try{await conn.query("INSTALL httpfs;");}catch{}
  try{await conn.query("LOAD httpfs;");}catch{}
  try{await conn.query("INSTALL arrow;");}catch{}
  try{await conn.query("LOAD arrow;");}catch{}

  // Probe first file; fallback to parquet if arrow fails
  try{
    const probe = engine==="arrow"?`read_ipc('${files[0]}')`:`read_parquet('${files[0]}')`;
    await conn.query(`SELECT 1 FROM ${probe} LIMIT 1`);
  }catch(e){
    if(engine==="arrow" && parquet.length){ engine="parquet"; files=parquet; }
    else throw e;
  }

  // Chunked views to limit query string size
  const CHUNK=16, views=[];
  for(let i=0;i<files.length;i+=CHUNK){
    const group = files.slice(i,i+CHUNK).map(f=> engine==="arrow"?
      `SELECT * FROM read_ipc('${f}')`:`SELECT * FROM read_parquet('${f}')`).join(" UNION ALL ");
    const v=`v_chunk_${(i/CHUNK)|0}`;
    await conn.query(`CREATE OR REPLACE TEMP VIEW ${v} AS ${group}`);
    views.push(v);
  }
  await conn.query(`CREATE OR REPLACE VIEW state AS ${views.map(v=>`SELECT * FROM ${v}`).join(" UNION ALL ")}`);

  if (metaEl) {
    // 人类可读：显示使用的 manifest 与引擎
    const used = manifestURL.href.replace(location.origin,"");
    metaEl.textContent = `Files: ${files.length} · Engine: read_${engine} · Manifest: ${used}`;
  }
}

// ---------- render ----------
function renderTable(table){
  const rows=table.toArray(); const cols=table.schema.fields.map(f=>f.name);
  let html="<table><thead><tr>"; for(const c of cols) html+=`<th>${c}</th>`; html+="</tr></thead><tbody>";
  for(const r of rows){ html+="<tr>"; for(const c of cols){ let v=r[c];
    if(v && (v.BYTES_PER_ELEMENT || v instanceof ArrayBuffer)){
      const b=v instanceof ArrayBuffer?new Uint8Array(v):new Uint8Array(v.buffer||v);
      const hex=Array.from(b.slice(0,16)).map(x=>x.toString(16).padStart(2,'0')).join('');
      v=`0x${hex}${b.length>16?'…':''}`;
    }
    html+=`<td>${(v===null||v===undefined)?"":String(v)}</td>`;
  } html+="</tr>"; }
  html+="</tbody></table>"; resultEl.innerHTML=html;
}

// ---------- boot ----------
function pickPreferBrowser(bundles){ return bundles.browser ?? bundles.mvp ?? Object.values(bundles)[0]; }
async function sameOriginWorkerURL(url){ const r=await fetch(url,{cache:"no-store"}); if(!r.ok) throw new Error("fetch worker "+r.status); return URL.createObjectURL(new Blob([await r.text()],{type:"text/javascript"})); }

async function boot(){
  try{
    setStatus("Booting…");
    const bundles = duckdb.getJsDelivrBundles();
    const bundle  = pickPreferBrowser(bundles);
    const workerURL = await sameOriginWorkerURL(bundle.mainWorker);
    const worker = new Worker(workerURL);
    const db = new duckdb.AsyncDuckDB(new duckdb.ConsoleLogger(), worker);
    await db.instantiate(bundle.mainModule);
    const conn = await db.connect();

    setStatus("Loading data…");
    await buildState(conn);
    setStatus("Ready");

    $id("run").onclick = async ()=>{
      const sql=($id("sql").value||"").trim(); if(!sql) return;
      const t0=performance.now();
      try{ const tbl=await conn.query(sql); renderTable(tbl); setStatus(`Done in ${(performance.now()-t0).toFixed(0)} ms`); }
      catch(e){ console.error(e); setStatus("Error","#ffecec"); showError(e?.message||String(e)); }
    };
    $id("fill").onclick = ()=>{ const q=$id("examples").value; if(q) $id("sql").value=q; };
    $id("init").onclick = async ()=>{ showError(""); resultEl.innerHTML=""; setStatus("Rebuilding…"); await buildState(conn); setStatus("Ready"); };
  }catch(e){ console.error(e); setStatus("Boot error","#ffecec"); showError(e?.message||String(e)); }
}
boot();
