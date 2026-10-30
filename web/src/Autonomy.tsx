import React, { useEffect, useState } from 'react';

export function Autonomy({ api }: { api: (url: string) => Promise<any> }) {
  const [data,setData]=useState<any>(null), [error,setError]=useState('');
  useEffect(()=>{let stopped=false; const refresh=async()=>{try{const d=await api('/autonomy');if(!stopped){setData(d);setError('');}}catch{if(!stopped)setError('Não foi possível consultar o observador.');}};
    void refresh();const timer=setInterval(refresh,15000);return()=>{stopped=true;clearInterval(timer);};},[api]);
  if(error)return <section role="alert">{error}</section>;
  if(!data)return <p>Consultando observador independente…</p>;
  const q=data.qualification, stale=Date.now()-Date.parse(data.observedAt)>90000;
  return <section className="autonomy-panel">
    <h2>Autonomia do AgentOS</h2>
    <p><strong>{stale?'Observação desatualizada':data.qualified?'Meta comprovada':'Em validação'}</strong> · GLM exclusivo · até 12 sessões / 6 chamadas simultâneas</p>
    <div className="autonomy-cards">
      <article><strong>{data.finalDeliveries?.length??0}</strong><span>Entregas finais aceitas do objetivo</span></article>
      <article><strong>{q.autonomousPassed} / {q.total}</strong><span>Ensaios autônomos · meta 54 / 60</span></article>
      <article><strong>{data.gate?.effectiveActive??'?' } / 6</strong><span>Chamadas ao provedor · {data.gate?.queued??'?'} na fila</span></article>
      <article><strong>{data.active?.length??0}</strong><span>Sessões ativas no sistema</span></article>
    </div>
    <p>{data.status==='schema_pending'?'Atualização de banco pendente.':data.status==='no_verified_delivery'?'Ainda não há entrega final aceita neste objetivo.':data.status==='model_policy_pending'?'Há execuções anteriores à política GLM para reconciliar.':data.status==='observation_failed'?'A leitura falhou; atividade atual não confirmada.':'Observação em andamento.'}</p>
    <p>{q.reason} {q.pending} ensaios pendentes.</p>
    <p>Observador: {new Date(data.observedAt).toLocaleString('pt-BR')} · Janela observada: {Number(q.observedHours??0).toFixed(2)} h. Isso não comprova 24 h de autonomia.</p>
    <details><summary>Ver execuções e evidências</summary>
      <ul>{(data.active??[]).map((r:any)=><li key={r.id}>{r.title} · {r.stage} · {r.model} · {r.state}</li>)}</ul>
      <p>Aceites de supervisão ficam fora da contagem de entregas finais.</p>
      <pre>{JSON.stringify({status:data.status,schema:data.schema,acceptances24h:data.acceptances24h,errors:data.errors},null,2)}</pre>
    </details>
  </section>;
}
