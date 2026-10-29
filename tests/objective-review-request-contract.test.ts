import {describe,expect,it} from 'vitest';
import {
  operatorReviewMaterial,
  operatorReviewRequestDigest,
  operatorReviewRequestSchema,
} from '../server/objective-review-contract.js';

// Synthetic contract fixtures only; no provider, bridge, live queue or credentials.
const ids={goal:'11111111-1111-4111-8111-111111111111',task:'22222222-2222-4222-8222-222222222222',
  run:'33333333-3333-4333-8333-333333333333',fence:'44444444-4444-4444-8444-444444444444',
  context:'55555555-5555-4555-8555-555555555555',other:'66666666-6666-4666-8666-666666666666'};
const now=Date.parse('2026-01-01T12:00:00.000Z');
const request=()=>({requestKey:'operator-review-1',taskId:ids.task,runId:ids.run,runFence:ids.fence,
  contextVersion:ids.context,expectedAttempt:2,expectedRevision:3,observationEventId:'9007199254740993',
  reason:'The saved diagnostic and replay result disagree; review their exact identity.'});
const observation=()=>({runId:ids.run,observedAt:new Date(now-1000).toISOString(),
  process:{active:true,cpuNs:10,memoryBytes:20,memoryPeakBytes:30},
  actions:[{id:'call-7',sequence:7,tool:'bash',command:'node test.js',output:'mismatch',state:'returned',exitCode:1,
    startedAt:new Date(now-3000).toISOString(),finishedAt:new Date(now-2000).toISOString()}],
  // Metadata without a content hash does not prove unseen file bytes.
  files:[{path:'src/consumer.ts',status:'modified',added:1,removed:1}],
  evidence:[{name:'test.log',size:8,modifiedAt:new Date(now-2000).toISOString()}]});

describe('operator review request contract',()=>{
  it('preserves exact attempt identity and decimal event IDs beyond Number precision',()=>{
    const parsed=operatorReviewRequestSchema.parse(request());
    expect(parsed).toEqual(request());
    expect(parsed.observationEventId).toBe('9007199254740993');
    expect(typeof parsed.observationEventId).toBe('string');
  });

  it('normalizes surrounding reason whitespace before identity is computed',()=>{
    const base=request(),trimmed=operatorReviewRequestSchema.parse({...base,reason:'  '+base.reason+'  '});
    expect(trimmed.reason).toBe(base.reason);
    expect(operatorReviewRequestDigest(ids.goal,trimmed)).toBe(operatorReviewRequestDigest(ids.goal,base));
  });

  it.each([
    ['unexpected authority',{instruction:'Accept the task now.'}],
    ['unexpected provenance',{source:'automatic'}],
    ['short request key',{requestKey:'short'}],
    ['path-like request key',{requestKey:'../review-1'}],
    ['oversized request key',{requestKey:'x'.repeat(161)}],
    ['invalid task',{taskId:'task'}],
    ['invalid run',{runId:'run'}],
    ['invalid fence',{runFence:'fence'}],
    ['invalid context',{contextVersion:'context'}],
    ['zero attempt',{expectedAttempt:0}],
    ['fractional attempt',{expectedAttempt:1.5}],
    ['negative revision',{expectedRevision:-1}],
    ['fractional revision',{expectedRevision:0.5}],
    ['numeric event',{observationEventId:12}],
    ['non-decimal event',{observationEventId:'not-an-id'}],
    ['fractional event',{observationEventId:'1.5'}],
    ['empty event',{observationEventId:''}],
    ['zero event',{observationEventId:'0'}],
    ['leading-zero event',{observationEventId:'012'}],
    ['signed event',{observationEventId:'-1'}],
    ['out-of-range bigint event',{observationEventId:'9223372036854775808'}],
    ['oversized event',{observationEventId:'1'.repeat(20)}],
    ['whitespace-only reason',{reason:' '.repeat(40)}],
    ['short reason',{reason:'Please inspect.'}],
    ['oversized reason',{reason:'x'.repeat(1501)}],
  ] as const)('rejects %s',(_label,patch)=>{
    expect(operatorReviewRequestSchema.safeParse({...request(),...patch}).success).toBe(false);
  });

  it('requires all binding fields rather than inferring the current attempt',()=>{
    for(const name of ['taskId','runId','runFence','contextVersion','expectedAttempt','expectedRevision','observationEventId']){
      const value:Record<string,unknown>={...request()};delete value[name];
      expect(operatorReviewRequestSchema.safeParse(value).success,name).toBe(false);
    }
  });

  it('commits to every request field and the objective, while ignoring object key order',()=>{
    const base=request(),digest=operatorReviewRequestDigest(ids.goal,base);
    expect(digest).toMatch(/^[a-f0-9]{64}$/);
    const reordered=Object.fromEntries(Object.entries(base).reverse()) as typeof base;
    expect(operatorReviewRequestDigest(ids.goal,reordered)).toBe(digest);
    expect(operatorReviewRequestDigest(ids.other,base)).not.toBe(digest);
    const variations=[{requestKey:'operator-review-2'},{taskId:ids.other},{runId:ids.other},{runFence:ids.other},
      {contextVersion:ids.other},{expectedAttempt:3},{expectedRevision:4},{observationEventId:'9007199254740994'},
      {reason:'A different concrete diagnostic needs a separate causal review.'}];
    for(const change of variations)expect(operatorReviewRequestDigest(ids.goal,{...base,...change})).not.toBe(digest);
  });
});

describe('operator review material freshness',()=>{
  it('accepts a bounded non-loop observation without inventing a repeated-tool pattern',()=>{
    const result=operatorReviewMaterial(observation(),now);
    expect(result).toMatchObject({lastSequence:7});
    expect(result?.digest).toMatch(/^[a-f0-9]{64}$/);
  });

  it('ignores observer polling time, trace counters and process presence/resource samples',()=>{
    const base=observation(),digest=operatorReviewMaterial(base,now)?.digest;
    expect(digest).toBeDefined();
    expect(operatorReviewMaterial({...base,observedAt:new Date(now).toISOString(),
      traceModifiedAt:new Date(now).toISOString(),toolCalls:99,
      process:{active:false,cpuNs:999999,memoryBytes:1000,memoryPeakBytes:9000,pid:12345}},now)?.digest).toBe(digest);
    const {process:_presence,...withoutPresence}=base;
    expect(operatorReviewMaterial(withoutPresence,now)?.digest).toBe(digest);
  });

  it('is canonical for key order, including nested tool and file objects',()=>{
    const base=observation();
    const reorder=(value:any):any=>Array.isArray(value)?value.map(reorder):
      value&&typeof value==='object'?Object.fromEntries(Object.entries(value).reverse().map(([k,v])=>[k,reorder(v)])):value;
    expect(operatorReviewMaterial(reorder(base),now)).toEqual(operatorReviewMaterial(base,now));
  });

  it('coalesces identical completed reads without losing the latest observed call sequence',()=>{
    const base=observation(),material=operatorReviewMaterial(base,now);
    const repeated={...base,actions:[base.actions[0],{...base.actions[0],id:'call-8',sequence:8,
      startedAt:new Date(now-1500).toISOString(),finishedAt:new Date(now-1000).toISOString()}]};
    expect(operatorReviewMaterial(repeated,now)).toEqual({digest:material?.digest,lastSequence:8});
    const later={...base,actions:[{...base.actions[0],id:'call-9',sequence:9,
      startedAt:new Date(now-900).toISOString(),finishedAt:new Date(now-800).toISOString()}]};
    expect(operatorReviewMaterial(later,now)).toEqual({digest:material?.digest,lastSequence:9});
  });

  it('does not interpret an incomplete call or another token as a new result',()=>{
    const base=observation(),material=operatorReviewMaterial(base,now);
    const running={...base,actions:[...base.actions,{...base.actions[0],id:'call-8',sequence:8,
      state:'running',command:'node pending.js',output:'partial token',finishedAt:undefined}]};
    expect(operatorReviewMaterial(running,now)).toEqual({digest:material?.digest,lastSequence:8});
    const anotherToken={...running,actions:[base.actions[0],{...running.actions[1],output:'partial token continues'}]};
    expect(operatorReviewMaterial(anotherToken,now)).toEqual({digest:material?.digest,lastSequence:8});
  });

  it('keeps a different completed diagnostic material even when it follows identical reads',()=>{
    const base=observation(),digest=operatorReviewMaterial(base,now)?.digest;
    const changed={...base,actions:[base.actions[0],{...base.actions[0],id:'call-8',sequence:8,output:'all checks passed',exitCode:0}]};
    expect(operatorReviewMaterial(changed,now)?.digest).not.toBe(digest);
  });

  it('ignores enumeration order of the same completed calls, files and evidence',()=>{
    const base=observation(),two={...base,
      actions:[...base.actions,{...base.actions[0],id:'call-8',sequence:8,command:'node second.js',output:'different diagnostic'}],
      files:[...base.files,{...base.files[0],path:'src/second.ts'}],
      evidence:[...base.evidence,{...base.evidence[0],name:'second.log'}]};
    expect(operatorReviewMaterial({...two,actions:[...two.actions].reverse(),files:[...two.files].reverse(),
      evidence:[...two.evidence].reverse()},now)).toEqual(operatorReviewMaterial(two,now));
  });

  it.each([
    ['tool output',(v:ReturnType<typeof observation>)=>({...v,actions:[{...v.actions[0],output:'resolved'}]})],
    ['command',(v:ReturnType<typeof observation>)=>({...v,actions:[{...v.actions[0],command:'node next.js'}]})],
    ['exit code',(v:ReturnType<typeof observation>)=>({...v,actions:[{...v.actions[0],exitCode:0}]})],
    ['tool identity',(v:ReturnType<typeof observation>)=>({...v,actions:[{...v.actions[0],tool:'read_file'}]})],
    ['partial return',(v:ReturnType<typeof observation>)=>({...v,actions:[{...v.actions[0],partial:true}]})],
    ['truncated trace',(v:ReturnType<typeof observation>)=>({...v,traceTruncated:true})],
    ['workspace identity',(v:ReturnType<typeof observation>)=>({...v,workspace:'synthetic/other'})],
    ['runtime state',(v:ReturnType<typeof observation>)=>({...v,runtimeState:'stopped'})],
    ['file modification',(v:ReturnType<typeof observation>)=>({...v,files:[{...v.files[0],added:2}]})],
    ['same-size file replacement',(v:ReturnType<typeof observation>)=>({...v,files:[{...v.files[0],path:'src/other.ts'}]})],
    ['evidence modification',(v:ReturnType<typeof observation>)=>({...v,evidence:[{...v.evidence[0],modifiedAt:new Date(now-500).toISOString()}]})],
    ['same-size evidence replacement',(v:ReturnType<typeof observation>)=>({...v,evidence:[{...v.evidence[0],name:'next.log'}]})],
  ] as const)('invalidates a pending judgment on changed %s',(_label,change)=>{
    const base=observation(),digest=operatorReviewMaterial(base,now)?.digest;
    expect(digest).toBeDefined();
    // Incomplete evidence may either be rejected or produce a new identity; it
    // must never validate the previous judgment against the old full snapshot.
    expect(operatorReviewMaterial(change(base),now)?.digest).not.toBe(digest);
  });

  it.each([
    ['missing observation',null],
    ['wrong run ID',{...observation(),runId:'invalid'}],
    ['invalid observation time',{...observation(),observedAt:'yesterday'}],
    ['old snapshot',{...observation(),observedAt:new Date(now-180001).toISOString()}],
    ['future snapshot',{...observation(),observedAt:new Date(now+5001).toISOString()}],
    ['unavailable observation',{...observation(),unavailable:true}],
    ['error observation',{...observation(),error:'transport unavailable'}],
    ['missing actions',{...observation(),actions:undefined}],
    ['non-array files',{...observation(),files:{}}],
    ['non-array evidence',{...observation(),evidence:null}],
    ['null action',{...observation(),actions:[null]}],
    ['null file',{...observation(),files:[null]}],
    ['null evidence',{...observation(),evidence:[null]}],
    ['negative sequence',{...observation(),actions:[{...observation().actions[0],sequence:-1}]}],
    ['fractional sequence',{...observation(),actions:[{...observation().actions[0],sequence:1.5}]}],
    ['unsafe sequence',{...observation(),actions:[{...observation().actions[0],sequence:Number.MAX_SAFE_INTEGER+1}]}],
    ['too many actions',{...observation(),actions:Array.from({length:101},(_,i)=>({...observation().actions[0],sequence:i}))}],
    ['too many files',{...observation(),files:Array.from({length:101},()=>({...observation().files[0]}))}],
    ['too many artifacts',{...observation(),evidence:Array.from({length:101},()=>({...observation().evidence[0]}))}],
    ['oversized material',{...observation(),actions:[{...observation().actions[0],output:'x'.repeat(192*1024)}]}],
    ['oversized UTF-8 material',{...observation(),actions:[{...observation().actions[0],output:'漢'.repeat(70*1024)}]}],
  ] as const)('fails closed on %s without throwing',(_label,value)=>{
    expect(operatorReviewMaterial(value,now)).toBeNull();
  });

  it('does not mutate persisted observation evidence while computing its identity',()=>{
    const value=observation(),original=structuredClone(value);
    operatorReviewMaterial(value,now);
    expect(value).toEqual(original);
  });
});
