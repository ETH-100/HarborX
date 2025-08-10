// app.js — Arrow-first with Parquet fallback (pure frontend) — manifest-relative URL fix
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

function pickPreferBrowser(bundles){ return bundles.browser ?? bundles.mvp ?? Object.values(bundles)[0]; }
async function sameOriginWorkerURL(url){ const r=await fetch(url,{cache:"no-store"}); if(!r.ok) throw new Error("fetch worker "+r.status); return URL.createObjectURL(new Blob([await r.text()],{type:"text/javascript"})); }

async function loadManifest(){
  // Resolve file paths relative to *manifest location* (not the page)
  const manifestURL = new URL("data/manifest.json", document.baseURI);
  const res = await fetch(manifestURL.href, { cache: "no-store" });
  if (!res.ok) throw new Error(`manifest ${res.status}`);
  const m = await res.json();
  const toAbs = (a) => (Array.isArray(a)?a:[]).map(p=> new URL(p, manifestURL).href);
  return {arrow:toAbs(m.arrow), parquet:toAbs(m.parquet)};
}

async function buildState(conn){
  const {arrow, parquet} = await loadManifest();
  let engine = arrow.length ? "arrow" : (parquet.length ? "parquet" : null);
  let files  = engine==="arrow"?arrow:engine==="parquet"?parquet:[];
  if(!engine) throw new Error("manifest has no files");

  try{await conn.query("INSTALL httpfs;");}catch{}
  try{await conn.query("LOAD httpfs;");}catch{}
  try{await conn.query("INSTALL arrow;");}catch{}
  try{await conn.query("LOAD arrow;");}catch{}

  try{
    const probe = engine==="arrow"?`read_ipc('${files[0]}')`:`read_parquet('${files[0]}')`;
    await conn.query(`SELECT 1 FROM ${probe} LIMIT 1`);
  }catch(e){
    if(engine==="arrow" && parquet.length){ engine="parquet"; files=parquet; }
    else throw e;
  }

  const CHUNK=16, views=[];
  for(let i=0;i<files.length;i+=CHUNK){
    const group = files.slice(i,i+CHUNK).map(f=> engine==="arrow"?
      `SELECT * FROM read_ipc('${f}')`:`SELECT * FROM read_parquet('${f}')`).join(" UNION ALL ");
    const v=`v_chunk_${(i/CHUNK)|0}`;
    await conn.query(`CREATE OR REPLACE TEMP VIEW ${v} AS ${group}`);
    views.push(v);
  }
  await conn.query(`CREATE OR REPLACE VIEW state AS ${views.map(v=>`SELECT * FROM ${v}`).join(" UNION ALL ")}`);
  if (metaEl) metaEl.textContent = `Files: ${files.length} · Engine: read_${engine}`;
}

function renderTable(table){
  const rows=table.toArray(); const cols=table.schema.fields.map(f=>f.name);
  let html="<table><thead><tr>"; for(const c of cols) html+=`<th>${c}</th>`; html+="</tr></thead><tbody>";
  for(const r of rows){ html+="<tr>"; for(const c of cols){ let v=r[c];
    if(v && (v.BYTES_PER_ELEMENT || v instanceof ArrayBuffer)){ const b=v instanceof ArrayBuffer?new Uint8Array(v):new Uint8Array(v.buffer||v); const hex=Array.from(b.slice(0,16)).map(x=>x.toString(16).padStart(2,'0')).join(''); v=`0x${hex}${b.length>16?'…':''}`; }
    html+=`<td>${(v===null||v===undefined)?"":String(v)}</td>`; } html+="</tr>"; }
  html+="</tbody></table>"; resultEl.innerHTML=html;
}

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

    document.getElementById("run").onclick = async ()=>{
      const sql=(document.getElementById("sql").value||"").trim(); if(!sql) return;
      const t0=performance.now();
      try{ const tbl=await conn.query(sql); renderTable(tbl); setStatus(`Done in ${(performance.now()-t0).toFixed(0)} ms`); }
      catch(e){ console.error(e); setStatus("Error","#ffecec"); if (errorEl) errorEl.textContent = e?.message||String(e); }
    };
    document.getElementById("fill").onclick = ()=>{ const q=document.getElementById("examples").value; if(q) document.getElementById("sql").value=q; };
    document.getElementById("init").onclick = async ()=>{ if (errorEl) errorEl.textContent=""; if (resultEl) resultEl.innerHTML=""; setStatus("Rebuilding…"); await buildState(conn); setStatus("Ready"); };
  }catch(e){ console.error(e); setStatus("Boot error","#ffecec"); if (errorEl) errorEl.textContent = e?.message||String(e); }
}
boot();
