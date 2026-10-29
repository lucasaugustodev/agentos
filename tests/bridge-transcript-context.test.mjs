import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import vm from 'node:vm';

// Exercise the exact production call site without importing the bridge's
// credential adapters or starting its stdin RPC loop. Every pi-ai entry point,
// provider- and api-level alike, is typed for a TranscriptContext and reads the
// prompt and the tool declarations from the transcript itself; only
// normalizeContext() folds Context.systemPrompt and Context.tools into the
// leading system message. A call made with the raw Context shape still streams
// and still bills tokens, so nothing fails loudly: the model simply never sees
// a tool, answers in prose, and the delivery contract rejects the turn.
const source=await readFile(new URL('../bridge/server.mjs',import.meta.url),'utf8');
const parts=source.match(/const context = \{[\s\S]*?\n      \);/gu);
assert.equal(parts?.length,1,'one production generation call site');
const call=new vm.Script(
 '((params, provider, model, normalizeContext, auth, requestSignal, emit) => {'+parts[0]+'\n return stream;})',
).runInNewContext(Object.create(null),{timeout:1000});

const tool={name:'bash',description:'Executa um comando shell',parameters:{type:'object',properties:{command:{type:'string'}}}};
const params={system:'Use as ferramentas disponiveis.',messages:[{role:'user',content:'Liste os arquivos.',timestamp:0}],tools:[tool]};

// The vendored semantics of @earendil-works/pi-ai utils/transcript.js.
const normalizeContext=context=>{
 const hasPrompt=context.systemPrompt!==undefined&&context.systemPrompt.length>0;
 const hasTools=context.tools!==undefined&&context.tools.length>0;
 const initial=hasPrompt||hasTools
  ?{role:'system',content:context.systemPrompt??'',...(hasTools?{toolsAdded:context.tools}:{}),timestamp:0}
  :undefined;
 return {messages:initial?[initial,...context.messages]:context.messages};
};
const invoke=(provider,params,fold)=>call(params,provider,{id:"glm-5.3-flash"},fold,{apiKey:"k"},undefined,()=>{});
const capture=()=>{
 const seen={};
 return {seen,provider:{streamSimple:(model,context)=>{seen.context=context;return {result:async()=>({})};}}};
};

test('an SDK provider receives the prompt and tools inside the transcript',()=>{
 const {seen,provider}=capture();
 invoke(provider,params,normalizeContext);
 // A provider that reads only context.messages must still find both.
 assert.deepEqual(Object.keys(seen.context),['messages']);
 const [head]=seen.context.messages;
 assert.equal(head.role,'system');
 assert.equal(head.content,'Use as ferramentas disponiveis.');
 assert.deepEqual(head.toolsAdded,[tool]);
});

test('the declared tools are never dropped on the way to the provider',()=>{
 const {seen,provider}=capture();
 invoke(provider,params,normalizeContext);
 const declared=seen.context.messages.flatMap(m=>m.toolsAdded??[]).map(t=>t.name);
 assert.deepEqual(declared,['bash'],'a turn with no declared tool can only answer in prose');
});

test('the bundled fallback still receives the raw Context shape',()=>{
 // normalizeContext is null exactly when the SDK is absent, and the fallback
 // reads systemPrompt and tools itself; folding there would hide both.
 const {seen,provider}=capture();
 invoke(provider,params,null);
 assert.equal(seen.context.systemPrompt,'Use as ferramentas disponiveis.');
 assert.deepEqual(seen.context.tools,[tool]);
});

test('the bundled transport is not folded while the SDK is installed',()=>{
 // The GLM reasoning route selects the bundled transport even with the SDK
 // present, so normalizeContext exists. The target decides, not its presence.
 const {seen,provider}=capture();
 provider.acceptsRawContext=true;
 invoke(provider,params,normalizeContext);
 assert.equal(seen.context.systemPrompt,'Use as ferramentas disponiveis.');
 assert.deepEqual(seen.context.tools,[tool]);
});

test('an empty prompt and no tools leave the transcript untouched',()=>{
 const {seen,provider}=capture();
 invoke(provider,{messages:[{role:"user",content:"oi",timestamp:0}]},normalizeContext);
 assert.equal(seen.context.messages.length,1);
 assert.equal(seen.context.messages[0].role,'user');
});
