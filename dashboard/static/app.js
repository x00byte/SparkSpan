'use strict';
const $ = id => document.getElementById(id);
const esc = value => String(value ?? '').replace(/[&<>"']/g, x => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[x]));
const fmt = (value, digits=1) => value === null || value === undefined || !Number.isFinite(Number(value)) ? '—' : Number(value).toLocaleString(undefined,{maximumFractionDigits:digits,minimumFractionDigits:digits});
const stamp = value => value ? new Date(value*1000).toLocaleTimeString([], {hour:'2-digit',minute:'2-digit',second:'2-digit'}) : '—';
const duration = value => !value ? '0s' : value < 60 ? `${fmt(value,1)}s` : `${Math.floor(value/60)}m ${Math.floor(value%60)}s`;
let csrf = '', current = null, config = null, activeTab = 'live', paused = false, liveRows = [], windowRows = [], lastHistory = 0;
let lastModels = '', benchmarkActive = false, targets = [], lastTargetList = '';
const benchmarkSnapshot = () => $('bench-target').value === 'gateway' ? current : targets.find(t=>t.name===$('bench-target').value)?.snapshot;
async function api(path, method='GET', data=null) {
  const response = await fetch(path,{method,headers: method==='GET'?{}:{'Content-Type':'application/json','X-CSRF-Token':csrf},body:data===null?undefined:JSON.stringify(data)});
  let result;
  try { result = await response.json(); } catch { throw new Error(`Server returned HTTP ${response.status}`); }
  if (!response.ok) throw new Error(result.error || result.detail || `HTTP ${response.status}`);
  return result;
}
function message(id,text,error=false){$(id).textContent=text;$(id).classList.toggle('error',error);}
function showTab(){
  activeTab = location.hash.slice(1);
  if (!['live','windows','benchmarks','admin'].includes(activeTab)) activeTab='live';
  for(const view of document.querySelectorAll('.view')) view.hidden=view.id!==`view-${activeTab}`;
  for(const link of document.querySelectorAll('[data-tab]')) {
    if(link.dataset.tab===activeTab) link.setAttribute('aria-current','page'); else link.removeAttribute('aria-current');
  }
  if(activeTab==='admin' && !config) loadConfig();
  if(activeTab==='benchmarks') loadBenchmarks();
  if(activeTab==='windows') loadWindow();
  requestAnimationFrame(drawCharts);
}
window.addEventListener('hashchange',showTab);
function render(state){
  current=state;
  const g=state.gateway, live=state.live;
  $('last-update').textContent=stamp(state.updated_at);
  $('connection').textContent=state.error?'Gateway unavailable':'Live connection';
  $('connection-dot').className=`dot ${state.error?'error':'online'}`;
  $('poll-interval').textContent=state.poll_seconds;
  $('capacity-pill').textContent=`${g.capacity||0} accepting slots · ${(g.backends||[]).length} backends`;
  const running=(g.backends||[]).reduce((s,n)=>s+(n.healthy?n.running||0:0),0);
  const waiting=(g.backends||[]).reduce((s,n)=>s+(n.healthy?n.waiting||0:0),0);
  $('generation').textContent=fmt(live.output_tps);
  $('prefill').textContent=fmt(live.uncached_tps,0);
  $('running').textContent=state.error?'—':fmt(running,0);
  $('admission').textContent=`/ ${fmt(g.capacity,0)}`;
  $('queue').textContent=`${g.queued||0} gateway queued · ${waiting} native waiting`;
  $('ttft').textContent=fmt(live.ttft_s,2);
  $('cache-share').textContent=live.cache_percent===null||live.cache_percent===undefined?'—':`${fmt(live.cache_percent)}%`;
  $('prefill-mean').textContent=live.prefill_request_s===null||live.prefill_request_s===undefined?'—':`${fmt(live.prefill_request_s,2)} s/request`;
  $('prefill-compute').textContent=live.prefill_compute_tps===null||live.prefill_compute_tps===undefined?'—':`${fmt(live.prefill_compute_tps,0)} tok/prefill-s`;
  $('live-window').textContent=`${duration(live.elapsed_s)} measured · ${fmt((live.coverage||0)*100,0)}% complete coverage`;
  $('chart-coverage').textContent=live.warming?'Waiting for consecutive samples':`${duration(live.elapsed_s)} wall-time rate · ${live.gaps||0} incomplete intervals · ${live.resets||0} resets`;
  let notice='';
  if(state.error) notice=`The gateway could not be sampled (${state.error}). Rates are unavailable until valid consecutive samples return.`;
  else if(g.backends?.some(n=>n.draining)) notice=`Maintenance mode: ${g.capacity||0} production admission slots are accepting work. Draining backends remain visible while their existing requests finish.`;
  else if(live.elapsed_s && live.coverage<.999) notice='This window contains missing or reset counters. Incomplete aggregate rates are unavailable; collecting a fresh baseline.';
  if(!state.error && running>(g.capacity||0)) notice+=(notice?' ':'')+'Native running requests include older or direct traffic and can exceed this gateway’s new-admission limit.';
  $('notice').hidden=!notice;$('notice').textContent=notice;$('notice').classList.toggle('error',!!state.error);
  renderBackends(state);renderRouting(state);renderWindows(state);renderOptions(state);updateAdminStatus(state);
  if(!paused && Date.now()-lastHistory>9000){lastHistory=Date.now();loadLiveChart();}
}
function stateLabel(n){return n.draining?'Maintenance':n.stalled?'No progress':n.accepting?'Accepting':'Unavailable';}
function renderBackends(s){
  $('backend-cards').innerHTML=(s.gateway.backends||[]).map(n=>{
    const rates=s.backend_rates[n.name]||{}, kv=s.kv_percent[n.name];
    const foot=n.draining?(n.drained?'Drain observed: no gateway or native work in the latest successful poll.':'New gateway admission paused. Verify fresh native counts before stopping this backend.'):`${n.models?.length||0} advertised aliases · ${n.gateway_inflight||0} requests through this gateway`;
    return `<article class="backend-card"><div class="backend-top"><div><div class="backend-title">${esc(n.name)}</div><div class="backend-sub">${esc(n.host||'Independent backend')} · ${n.capacity} configured slots</div></div><span class="pill ${n.draining?'warm':n.accepting?'ready':'bad'}">${stateLabel(n)}</span></div><div class="backend-numbers"><div><span>Generation</span><strong>${fmt(rates.output_tps)}<small>tok/s</small></strong></div><div><span>Uncached prefill</span><strong>${fmt(rates.uncached_tps,0)}<small>tok/s</small></strong></div><div><span>Native running</span><strong>${n.healthy?fmt(n.running,0):'—'}<small>/ ${n.capacity}</small></strong></div></div><div class="backend-detail"><span>KV ${kv===undefined?'—':fmt(kv)+'%'}</span><progress max="100" value="${Number(kv)||0}" aria-label="${esc(n.name)} KV cache usage"></progress><span>${n.waiting||0} waiting</span><span>Metrics ${n.metrics_age_seconds===null||n.metrics_age_seconds===undefined?'unavailable':fmt(n.metrics_age_seconds,1)+'s old'}</span></div><div class="backend-foot">${foot}</div></article>`;
  }).join('')||'<p class="muted">No backend status is available yet.</p>';
}
function renderRouting(s){
  const models=s.models||[];
  $('routing').innerHTML=models.length?`<table><thead><tr><th>Model / alias</th><th>Matching backends</th><th>Context / request</th><th>Measured output</th></tr></thead><tbody>${models.map(m=>{
    const nodes=s.gateway.backends.filter(n=>n.models?.includes(m.id));const rates=s.model_rates[m.id];
    return `<tr><td>${esc(m.id)}</td><td>${nodes.map(n=>`<span class="pill ${n.accepting?'ready':'warm'}">${esc(n.name)} · ${n.accepting?'accepting':'paused'}</span>`).join('')}</td><td>${fmt(m.max_model_len,0)}</td><td>${rates?fmt(rates.output_tps)+' tok/s':'Alias · shared engine counters'}</td></tr>`;
  }).join('')}</tbody></table>`:'<div class="table-empty">No models currently advertised by an accepting backend.</div>';
}
function renderWindows(s){
  $('window-cards').innerHTML=[['60','1 minute'],['300','5 minutes'],['900','15 minutes'],['3600','1 hour']].map(([key,label])=>{
    const w=s.windows[key];return `<article class="window-card"><h2>${label}</h2><div class="rate">${fmt(w.output_tps)} <small>tok/s output</small></div><p>${fmt(w.uncached_tps,0)} tok/s uncached input</p><p>${fmt(w.completed,0)} completions · ${fmt(w.preemptions,0)} preemptions</p><div class="coverage">${duration(w.elapsed_s)} actual · ${fmt(w.coverage*100,0)}% coverage</div><p>${w.resets||0} resets · ${w.gaps||0} incomplete intervals</p></article>`;
  }).join('');
}
function options(select,items){const old=select.value;select.innerHTML=items.map(([value,label])=>`<option value="${esc(value)}">${esc(label)}</option>`).join('');if(items.some(x=>x[0]===old))select.value=old;}
function renderOptions(s){
  s=benchmarkSnapshot();
  const signature=JSON.stringify([$('bench-target').value,s?.models,s?.gateway.backends?.map(n=>[n.name,n.accepting,n.models])]);
  if(signature===lastModels)return;lastModels=signature;
  options($('bench-model'),(s?.models||[]).map(m=>[m.id,m.id]));updateBackendOptions();
}
function updateBackendOptions(){
  const model=$('bench-model').value,scope=$('bench-target').value,production=scope==='gateway';
  options($('bench-backend'),[['',production?'Automatic · gateway scheduling':scope?'Direct · selected test engine':'Choose a scope first'],...(production?(current?.gateway.backends||[]):[]).filter(n=>n.accepting&&n.models?.includes(model)).map(n=>[n.name,n.name])]);
  $('bench-backend').disabled=!production;
}
function svg(tag,attrs={},text=null){const node=document.createElementNS('http://www.w3.org/2000/svg',tag);for(const[k,v]of Object.entries(attrs))node.setAttribute(k,v);if(text!==null)node.textContent=text;return node;}
function drawChart(id,rows){
  const holder=$(id);if(!holder||holder.clientWidth===0)return;
  const valid=rows.filter(r=>r.duration_s>0);holder.replaceChildren();
  if(!valid.length){const p=document.createElement('div');p.className='chart-empty';p.textContent='Waiting for consecutive native counter samples';holder.append(p);return;}
  const width=Math.max(300,holder.clientWidth),height=220,left=48,right=55,top=15,bottom=30;
  const chart=svg('svg',{viewBox:`0 0 ${width} ${height}`,'aria-hidden':'true'});holder.append(chart);
  const stride=Math.max(1,Math.ceil(valid.length/600));const points=[];
  for(let i=0;i<valid.length;i+=stride){const group=valid.slice(i,i+stride),seconds=group.reduce((a,r)=>a+r.duration_s,0),complete=group.every(r=>r.complete);let out=0,input=0,hasInput=true;for(const r of group)for(const d of r.deltas){out+=d.values.output||0;if(d.values.uncached===undefined)hasInput=false;input+=d.values.uncached||0;}points.push({time:group.at(-1).end,out:complete?out/seconds:null,input:complete&&hasInput?input/seconds:null});}
  const start=valid[0].start,end=valid.at(-1).end,span=Math.max(1,end-start),maxOut=Math.max(1,...points.map(p=>p.out||0))*1.12,maxIn=Math.max(1,...points.map(p=>p.input||0))*1.12;
  const x=t=>left+(t-start)/span*(width-left-right),y=(v,max)=>height-bottom-(v/max)*(height-top-bottom);
  for(let i=0;i<=4;i++){const pos=top+(height-top-bottom)*i/4;chart.append(svg('line',{x1:left,x2:width-right,y1:pos,y2:pos,class:'gridline'}));chart.append(svg('text',{x:left-8,y:pos+3,'text-anchor':'end'},fmt(maxOut*(1-i/4),0)));chart.append(svg('text',{x:width-right+8,y:pos+3},fmt(maxIn*(1-i/4),0)));}
  const ticks=width<500?2:4;for(let i=0;i<=ticks;i++){const t=start+span*i/ticks;chart.append(svg('text',{x:x(t),y:height-7,'text-anchor':'middle'},new Date(t*1000).toLocaleTimeString([],{hour:'2-digit',minute:'2-digit',...(span<300?{second:'2-digit'}:{})})));}
  for(const[field,max,color]of[['out',maxOut,'#f1c58e'],['input',maxIn,'#97c0ff']]){
    let path='',pen=false;
    for(const p of points){if(p[field]===null){pen=false;continue;}path+=`${pen?'L':'M'}${x(p.time).toFixed(2)} ${y(p[field],max).toFixed(2)} `;pen=true;}
    chart.append(svg('path',{d:path,fill:'none',stroke:color,'stroke-width':2,'stroke-linejoin':'round','stroke-linecap':'round'}));
  }
}
function drawCharts(){drawChart('live-chart',liveRows);drawChart('window-chart',windowRows);}
async function loadLiveChart(){try{liveRows=(await api('/api/history?seconds=900')).rows;drawChart('live-chart',liveRows);}catch{}}
async function loadWindow(){try{const seconds=$('history-window').value;windowRows=(await api(`/api/history?seconds=${seconds}`)).rows;drawChart('window-chart',windowRows);const elapsed=windowRows.length?windowRows.at(-1).end-windowRows[0].start:0;$('window-detail').textContent=`${duration(elapsed)} of recorded wall time · gaps are preserved · separate axes in tok/s`;}catch(e){$('window-detail').textContent=e.message;}}
function updateAdminStatus(s){for(const field of document.querySelectorAll('[data-backend-state]')){const n=s.gateway.backends.find(n=>n.name===field.dataset.backendState);field.textContent=n?`${stateLabel(n)} · ${n.accepting?n.capacity:0} accepting / ${n.capacity} configured slots · ${n.gateway_inflight} gateway in flight · ${n.running} native running · ${n.waiting} waiting · ${n.drained?'drain observed':n.healthy?'fresh native poll':'native poll unavailable'}`:'Not present in live gateway';}}
async function loadConfig(){
  try{config=await api('/api/config');$('config-revision').textContent=`Revision ${config.revision.slice(0,12)} · live edits preserve secrets and engine identities`;
    $('admin-backends').innerHTML=config.config.backends.map(n=>`<div class="admin-row" data-admin-backend="${esc(n.name)}"><div><h3>${esc(n.name)}</h3><p>${esc(n.url)}</p><div class="backend-state" data-backend-state="${esc(n.name)}"></div></div><label>Admission slots<input class="capacity-input" type="number" min="1" max="${n.max_capacity||8}" value="${n.capacity||8}" required></label><label class="check"><input class="draining-input" type="checkbox" ${n.draining?'checked':''}><span>Drain for maintenance<br><small class="muted">Finish existing work; stop new admission</small></span></label></div>`).join('');
    const hosts=[...new Set(config.config.backends.map(n=>n.host).filter(Boolean))];
    $('host-budgets').innerHTML=hosts.map(host=>`<label data-host="${esc(host)}">${esc(host)}<input type="number" min="1" max="1024" placeholder="No shared limit" value="${config.config.host_limits?.[host]||''}"></label>`).join('')||'<p class="muted small">No named hosts configured. Add host identities through a deployment draft.</p>';
    $('draft-config').value=JSON.stringify(config.config,null,2);$('admin-confirm').checked=false;renderRevisions();if(current)updateAdminStatus(current);
  }catch(e){message('admin-message',e.message,true);}
}
function renderRevisions(){
  $('revision-list').innerHTML=config.revisions.length?config.revisions.map(r=>`<div class="revision"><span>${esc(r.id)}<br>${new Date(r.created_at*1000).toLocaleString()}</span><button type="button" class="quiet rollback" data-revision="${esc(r.id)}">Restore admission</button></div>`).join(''):'<p class="muted small">No dashboard edits yet. A revision is saved before each live change.</p>';
}
$('admin-form').addEventListener('submit',async event=>{
  event.preventDefault();if(!config||!$('admin-confirm').checked)return;
  const backends=[...document.querySelectorAll('[data-admin-backend]')].map(row=>({name:row.dataset.adminBackend,capacity:Number(row.querySelector('.capacity-input').value),draining:row.querySelector('.draining-input').checked}));
  const host_limits={};for(const row of document.querySelectorAll('[data-host]')){const value=row.querySelector('input').value;if(value!=='')host_limits[row.dataset.host]=Number(value);}
  try{const result=await api('/api/config/apply','POST',{revision:config.revision,backends,host_limits});await loadConfig();message('admin-message',result.message);}catch(e){message('admin-message',e.message,true);}
});
$('revision-list').addEventListener('click',async event=>{const button=event.target.closest('.rollback');if(!button||!config)return;if(!confirm('Restore this revision’s admission settings? This changes live routing limits.'))return;try{const result=await api('/api/config/rollback','POST',{revision:config.revision,backup:button.dataset.revision});await loadConfig();message('admin-message',result.message);}catch(e){message('admin-message',e.message,true);}});
$('save-draft').addEventListener('click',async()=>{try{const result=await api('/api/config/draft','POST',{revision:config.revision,config:JSON.parse($('draft-config').value)});message('draft-message',`${result.message}\nDraft ${result.id}`);}catch(e){message('draft-message',e.message,true);}});
function downloadJSON(data,name){const url=URL.createObjectURL(new Blob([JSON.stringify(data,null,2)],{type:'application/json'}));const link=document.createElement('a');link.href=url;link.download=name;link.click();setTimeout(()=>URL.revokeObjectURL(url),1000);}
$('export-config').addEventListener('click',()=>{if(config)downloadJSON(config.config,'sparkspan-sanitized-config.json');});
$('reload-config').addEventListener('click',()=>{message('admin-message','');loadConfig();});
$('benchmark-form').addEventListener('submit',async event=>{
  event.preventDefault();if(!$('bench-confirm').checked)return;
  const body={confirmed:true,target:$('bench-target').value,model:$('bench-model').value,backend:$('bench-backend').value,concurrency:Number($('bench-concurrency').value),requests:Number($('bench-requests').value),max_tokens:Number($('bench-tokens').value),stream:$('bench-stream').value==='true',temperature:Number($('bench-temperature').value),prompt:$('bench-prompt').value};
  $('bench-run').disabled=true;
  try{const job=await api('/api/benchmarks/run','POST',body);renderJob(job);message('bench-message',`Benchmark started: ${job.options.target}.`);$('bench-confirm').checked=false;}catch(e){message('bench-message',e.message,true);$('bench-run').disabled=false;}
});
$('bench-cancel').addEventListener('click',async()=>{try{const result=await api('/api/benchmarks/cancel','POST',{});renderJob(result.active);message('bench-message','Cancelled. Active upstream connections were closed.');await loadBenchmarks();}catch(e){message('bench-message',e.message,true);}});
function renderJob(job){
  if(!job)return;const s=job.summary||{};benchmarkActive=job.state==='running';$('bench-run').disabled=benchmarkActive;$('bench-cancel').disabled=!benchmarkActive;$('bench-state').textContent=job.state;$('bench-progress').max=job.options.requests;$('bench-progress').value=job.progress;
  const items=[['Wall-time generation',s.aggregate_output_tps===null||s.aggregate_output_tps===undefined?'—':`${fmt(s.aggregate_output_tps)} tok/s`],['Actual elapsed',duration(s.elapsed_s)],['Requests complete',`${job.progress} / ${job.options.requests}`],['Mean first token',s.mean_ttft_ms===null||s.mean_ttft_ms===undefined?'—':`${fmt(s.mean_ttft_ms,0)} ms`],['Output tokens',fmt(s.output_tokens,0)],['Errors / missing usage',`${s.errors||0} / ${s.usage_missing||0}`]];
  $('bench-summary').innerHTML=items.map(([label,value])=>`<div><span>${label}</span><strong>${value}</strong></div>`).join('');
  $('bench-export').innerHTML=job.state==='running'?'':`<a href="/api/benchmarks/${encodeURIComponent(job.id)}/export?format=json">Export JSON</a><a href="/api/benchmarks/${encodeURIComponent(job.id)}/export?format=csv">Export CSV</a>`;
}
async function loadBenchmarks(){try{const result=await api('/api/benchmarks');if(result.active)renderJob(result.active);$('bench-history').innerHTML=result.history.length?`<table><thead><tr><th>Started</th><th>Model / routing</th><th>Test</th><th>Elapsed</th><th>Output tok/s</th><th>State</th><th>Export</th></tr></thead><tbody>${result.history.map(job=>`<tr><td>${new Date(job.started_at*1000).toLocaleString()}</td><td>${esc(job.options.model)}<br><span class="muted">${esc(job.options.target||'Production gateway')} · ${esc(job.options.backend||'Automatic')}</span></td><td>C${job.options.concurrency} · ${job.options.requests} requests<br><span class="muted">${job.options.stream?'Streaming':'Non-streaming'}</span></td><td>${duration(job.summary.elapsed_s)}</td><td>${fmt(job.summary.aggregate_output_tps)}</td><td>${esc(job.state)}</td><td><a href="/api/benchmarks/${encodeURIComponent(job.id)}/export?format=json">JSON</a> · <a href="/api/benchmarks/${encodeURIComponent(job.id)}/export?format=csv">CSV</a></td></tr>`).join('')}</tbody></table>`:'<div class="table-empty">No benchmark jobs recorded. Loading this page does not start one.</div>';}catch(e){message('bench-message',e.message,true);}}
$('bench-target').addEventListener('change',()=>{lastModels='';if(current)renderOptions(current);});$('bench-model').addEventListener('change',updateBackendOptions);$('refresh-benchmarks').addEventListener('click',loadBenchmarks);$('history-window').addEventListener('change',loadWindow);
$('pause-chart').addEventListener('click',()=>{paused=!paused;$('pause-chart').textContent=paused?'Resume chart':'Pause chart';$('pause-chart').setAttribute('aria-pressed',String(paused));if(!paused)loadLiveChart();});
new ResizeObserver(drawCharts).observe($('main'));
setInterval(()=>{loadTargets();if(activeTab==='benchmarks'&&(benchmarkActive||!$('bench-history').textContent))loadBenchmarks();if(activeTab==='windows')loadWindow();},2500);
async function start(){
  showTab();loadTargets();try{csrf=(await api('/api/session')).csrf_token;render(await api('/api/state'));loadLiveChart();}catch(e){$('connection').textContent='Dashboard unavailable';$('notice').hidden=false;$('notice').textContent=e.message;}
  const events=new EventSource('/api/events');events.addEventListener('state',event=>{try{render(JSON.parse(event.data));}catch(e){console.error('Invalid live update',e);}});events.onerror=()=>{$('connection').textContent='Reconnecting';$('connection-dot').className='dot error';};
}
start();

async function loadTargets(){
  try{targets=(await api('/api/targets')).targets;
    const targetItems=[['','Choose explicitly before running'],['gateway','Production gateway · affects live traffic'],...targets.map(t=>[t.name,`Isolated test · ${t.label}`])];const signature=JSON.stringify(targetItems);if(signature!==lastTargetList){options($('bench-target'),targetItems);lastTargetList=signature;}
    $('test-target-section').hidden=!targets.length;
    $('test-target-cards').innerHTML=targets.map(t=>{const snap=t.snapshot,live=snap.live,ready=!snap.error&&snap.models.length;return `<article class="backend-card"><div class="backend-top"><div><div class="backend-title">${esc(t.label)}</div><div class="backend-sub">Isolated test · never in production aggregate</div></div><span class="pill ${ready?'ready':'warm'}">${ready?'Test engine ready':'Loading / unavailable'}</span></div><div class="backend-numbers"><div><span>Test generation</span><strong>${fmt(live.output_tps)}<small>tok/s</small></strong></div><div><span>Uncached prefill</span><strong>${fmt(live.uncached_tps,0)}<small>tok/s</small></strong></div><div><span>Context</span><strong>${fmt(snap.models[0]?.max_model_len,0)}</strong></div></div><div class="backend-foot">${snap.models.map(m=>esc(m.id)).join(' · ')||'Waiting for engine health and model discovery. No test is running automatically.'}</div></article>`;}).join('');
    if(current)renderOptions(current);
  }catch{}
}
