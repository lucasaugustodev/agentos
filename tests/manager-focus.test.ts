import {it,expect} from 'vitest';
import {selectManagerFocus,focusManagerTasks,selectManagerCaseWindow,managerFocusSourceFingerprint,managerFocusAgingMilliseconds,managerFocusServiceHistory} from '../server/manager-focus.js';
import {administrativeRecoveryEpisodeDigest} from '../server/manager-administrative-recovery.js';

const administrativeFocusCase=(id='administrative',ready='81'):any=>{
 const entry={id,goal_id:'goal',task_id:'task-'+id,state:'open',decision_run_id:null,has_live_run:false,
  work_kind:'planning',base_workspace:'project/'+id,current_task_state:'recovery_blocked',current_run_state:'failed',
  current_run_id:'run-'+id,source_run_id:'run-'+id,current_run_fence:'fence-'+id,current_context_version:'context-'+id,
  current_attempt:1,task_attempt_count:1,current_error_code:'OPERATOR_RECOVERY_REQUESTED',
  pending_dependencies:0,fresh_handoffs:0,created_at:'2026-10-17T04:00:00Z',evidence:[{preserved:'exact'}]};
 const identity={goalId:entry.goal_id,caseId:id,taskId:entry.task_id,runId:entry.current_run_id,
  runFence:entry.current_run_fence,contextVersion:entry.current_context_version,attempt:1,planningRequestId:'request-'+id,
  requestId:'administrative-request-'+id,requestDigest:'a'.repeat(64),sourceDigest:'b'.repeat(64),
  bridgeReceiptDigest:'c'.repeat(64),readinessEventId:ready};
 return {...entry,administrativeRecoveryReview:{version:1,authority:'review_only',...identity,
  episodeDigest:administrativeRecoveryEpisodeDigest(identity),readyAt:entry.created_at,priorityConsumed:false,
  lastCompletedReviewWasAdministrative:false,actionValidationRequired:true}};
};
const ordinaryFocusCase=(id='delivery'):any=>({id,goal_id:'goal',task_id:'task-'+id,state:'open',decision_run_id:null,
 has_live_run:false,current_task_state:'failed',current_run_state:'failed',current_run_id:'run-'+id,current_attempt:1,
 pending_dependencies:0,fresh_handoffs:1,created_at:'2026-10-16T00:00:00Z'});

it('offers the just-stopped administrative planner its first review without waiting six minutes or a delivery quantum',()=>{
 const cases=[...Array.from({length:28},(_,i)=>ordinaryFocusCase('delivery-'+i)),administrativeFocusCase()];
 const before=structuredClone(cases),prior=selectManagerFocus(cases.slice(0,28),null);
 const selected=selectManagerFocus(cases,prior,undefined,[],Date.parse(cases[28].created_at))!;
 expect(selected).toMatchObject({caseId:'administrative',selectionReason:'administrative_first_review',
  administrativeRecoveryEpisodeDigest:cases[28].administrativeRecoveryReview.episodeDigest,remainingCases:28});
 expect(selectManagerCaseWindow(cases,selected).map(c=>c.id)).toEqual(['administrative']);
 expect(selected).not.toHaveProperty('retryAuthorized');expect(selected).not.toHaveProperty('accepted');
 expect(cases).toEqual(before);
});

it('does not repeat an administrative priority when artifacts change the ordinary source fingerprint',()=>{
 const administrative=administrativeFocusCase(),delivery=ordinaryFocusCase(),first=selectManagerFocus([delivery,administrative],null)!;
 const changed={...administrative,heartbeat_at:'2026-10-17T04:01:00Z',current_artifacts:[{id:'new-log',digest:'d'.repeat(64)}],
  administrativeRecoveryReview:{...administrative.administrativeRecoveryReview,priorityConsumed:true,lastCompletedReviewWasAdministrative:true}};
 expect(managerFocusSourceFingerprint(changed)).not.toBe(first.sourceFingerprint);
 expect(administrativeRecoveryEpisodeDigest(changed.administrativeRecoveryReview)).toBe(first.administrativeRecoveryEpisodeDigest);
 expect(selectManagerFocus([delivery,changed],first,undefined,[],Date.parse(changed.created_at))).toMatchObject({caseId:delivery.id});
});

it('alternates completed administrative loans with ordinary service, including across fresh request identities',()=>{
 const a=administrativeFocusCase('a','81'),b=administrativeFocusCase('b','82'),delivery=ordinaryFocusCase();
 const first=selectManagerFocus([delivery,b,a],null)!;expect(first.caseId).toBe('a');
 a.administrativeRecoveryReview.priorityConsumed=true;
 for(const entry of [a,b])entry.administrativeRecoveryReview.lastCompletedReviewWasAdministrative=true;
 const ordinary=selectManagerFocus([a,b,delivery],first)!;
 expect(ordinary.caseId).toBe(delivery.id);expect(ordinary).not.toHaveProperty('administrativeRecoveryEpisodeDigest');
 for(const entry of [a,b])entry.administrativeRecoveryReview.lastCompletedReviewWasAdministrative=false;
 expect(selectManagerFocus([delivery,b,a],ordinary)).toMatchObject({caseId:'b',selectionReason:'administrative_first_review',
  administrativeRecoveryEpisodeDigest:b.administrativeRecoveryReview.episodeDigest});
});

it('serves another pending administrative episode when there is no ordinary stopped case to starve',()=>{
 const a=administrativeFocusCase('a','81'),b=administrativeFocusCase('b','82');
 a.administrativeRecoveryReview.priorityConsumed=true;
 for(const entry of [a,b])entry.administrativeRecoveryReview.lastCompletedReviewWasAdministrative=true;
 expect(selectManagerFocus([a,b],{caseId:'a'})).toMatchObject({caseId:'b',selectionReason:'administrative_first_review'});
});

it.each([
 {has_live_run:true},{current_run_state:'reconciling'},{current_task_state:'accepted'},{source_run_id:'older-run'},
 {current_run_fence:'new-fence'},{current_context_version:'new-context'},{task_attempt_count:2},
 {state:'decided'},{state:'delegated'},{state:'waiting_evidence'},{decision_run_id:'unresolved-review'},
 {current_error_code:'MODEL_TIMEOUT'},
])('does not lend administrative review on changed ownership or source: %j',guard=>{
 const delivery=ordinaryFocusCase(),administrative={...administrativeFocusCase(),...guard};
 expect(selectManagerFocus([delivery,administrative],null)?.selectionReason).not.toBe('administrative_first_review');
});

it('does not accept a transplanted or noise-rearmed administrative episode digest',()=>{
 const delivery=ordinaryFocusCase(),administrative=administrativeFocusCase();
 for(const hint of [{caseId:'other'},{requestId:'new-request'},{readinessEventId:'90'},{runFence:'other'},
  {bridgeReceiptDigest:null},{sourceDigest:'wrong'},{authority:'execute'},{actionValidationRequired:false},
  {priorityConsumed:undefined},{lastCompletedReviewWasAdministrative:undefined}]){
  const changed={...administrative,administrativeRecoveryReview:{...administrative.administrativeRecoveryReview,...hint}};
  expect(selectManagerFocus([delivery,changed],null)?.selectionReason).not.toBe('administrative_first_review');
 }
});

it('preserves a terminal same-goal coordinator as history during the administrative first review',()=>{
 const administrative={...administrativeFocusCase(),decision_run_id:'old',decision_review_id:'old',
  decision_review_goal_id:'goal',decision_review_stage:'coordinate',decision_review_state:'submitted',
  decision:{kind:'wait',reason:'Historical judgment'}};
 const cases=[ordinaryFocusCase(),administrative],before=structuredClone(cases);
 expect(selectManagerFocus(cases,null)).toMatchObject({caseId:administrative.id,selectionReason:'administrative_first_review'});
 for(const guard of [{decision_review_state:'running'},{decision_review_goal_id:'foreign'},
  {decision_review_stage:'work'},{decision_review_id:'foreign'}])
  expect(selectManagerFocus([cases[0],{...administrative,...guard}],null)?.selectionReason).not.toBe('administrative_first_review');
 expect(cases).toEqual(before);
});

it('shares equal urgency across independent scopes after three completed reviews without closing or rewriting cases',()=>{
 const cases=['a','b','c'].map(id=>({id,task_id:'task-'+id,base_workspace:'project/'+id,
   work_kind:'repair',pending_dependencies:0,current_run_id:'run-'+id,current_attempt:2,
   current_run_state:'failed',current_error_code:'RESULT_FORMAT',evidence:[{immutable:id}]}));
 const before=structuredClone(cases),first=selectManagerFocus(cases,null)!;
 const reviews=Array.from({length:3},()=>({caseId:first.caseId,sourceFingerprint:first.sourceFingerprint}));
 expect(selectManagerFocus(cases,first,undefined,reviews.slice(0,2))?.caseId).toBe('a');
 const second=selectManagerFocus(cases,first,undefined,reviews)!;
 expect(second).toMatchObject({caseId:'b',scope:'project/b',selectionReason:'review_quantum'});
 const nextReviews=[...Array.from({length:3},()=>({caseId:second.caseId,sourceFingerprint:second.sourceFingerprint})),...reviews];
 const third=selectManagerFocus(cases,second,undefined,nextReviews)!;
 expect(third.caseId).toBe('c');
 const finalReviews=[...Array.from({length:3},()=>({caseId:third.caseId,sourceFingerprint:third.sourceFingerprint})),...nextReviews];
 expect(selectManagerFocus(cases,third,undefined,finalReviews)?.caseId).toBe('a');
 expect(cases).toEqual(before);
});

it('keeps new delivery evidence and higher semantic urgency ahead of review rotation',()=>{
 const prior={id:'a',task_id:'a',work_kind:'repair',pending_dependencies:1,current_run_id:'old',current_attempt:4};
 const other={id:'b',task_id:'b',work_kind:'repair',pending_dependencies:1};
 const focus=selectManagerFocus([prior,other],null)!;
 const reviews=Array.from({length:3},()=>({caseId:prior.id,sourceFingerprint:focus.sourceFingerprint}));
 for(const changed of [{current_run_id:'new'},{current_attempt:5},{pending_dependencies:0},
   {fresh_handoffs:1},{current_artifacts:[{id:'patch',digest:'immutable-hash'}]}])
  expect(selectManagerFocus([{...prior,...changed},other],focus,undefined,reviews)?.caseId).toBe('a');
 const delivery={id:'delivery',task_id:'delivery',pending_dependencies:0,fresh_handoffs:1};
 expect(selectManagerFocus([prior,other,delivery],focus,undefined,reviews)?.caseId).toBe('delivery');
 expect(selectManagerFocus([prior],focus,undefined,reviews)?.caseId).toBe('a');
 expect(managerFocusSourceFingerprint(prior)).toBe(focus.sourceFingerprint);
});

it('prefers another equally urgent scope and does not interpret legacy or interrupted review sequences as an exhausted quantum',()=>{
 const cases=[{id:'a',task_id:'a',base_workspace:'project/a',work_kind:'repair'},
  {id:'same-scope',task_id:'s',base_workspace:'project/a',work_kind:'repair'},
  {id:'b',task_id:'b',base_workspace:'project/b',work_kind:'repair'}];
 const focus=selectManagerFocus(cases,null)!;
 const completed={caseId:focus.caseId,sourceFingerprint:focus.sourceFingerprint};
 expect(selectManagerFocus(cases,focus,undefined,[completed,completed,completed])?.caseId).toBe('b');
 expect(selectManagerFocus(cases,focus,undefined,[{caseId:'a'},{caseId:'a'},{caseId:'a'}])?.caseId).toBe('a');
 expect(selectManagerFocus(cases,focus,undefined,[completed,{caseId:'b'},completed])?.caseId).toBe('a');
});

it('fits evidence-light stopped transport cases before a large historical case while preserving complete evidence',()=>{
 const focal={id:'focal',task_id:'delivery',current_run_state:'submitted'};
 const large={id:'large',work_kind:'diagnostic',current_run_state:'failed',current_error_code:'MODEL_TIMEOUT',
   current_artifacts:[],evidence:[{summary:'Old attempt details '.repeat(3000)}]};
 const compact=Array.from({length:7},(_,i)=>({id:'compact-'+i,work_kind:'diagnostic',current_run_state:'failed',
   current_error_code:'MODEL_TIMEOUT',current_artifacts:[],evidence:[{code:'MODEL_TIMEOUT',attempt:i+1}]}));
 const cases=[focal,large,...compact],before=structuredClone(cases);
 const window=selectManagerCaseWindow(cases,{caseId:'focal'});
 expect(window.map(entry=>entry.id)).toEqual(['focal',...compact.slice(0,5).map(entry=>entry.id)]);
 expect(window.slice(1).map(entry=>entry.evidence)).toEqual(compact.slice(0,5).map(entry=>entry.evidence));
 expect(cases).toEqual(before);
});

it('keeps a canonical delivery focus and batches only confirmed stopped transport failures without mutating evidence',()=>{
 const focal={id:'delivery',task_id:'canonical',pending_dependencies:0,current_run_state:'submitted',current_error_code:'ARTIFACT_PATH'};
 const failures=Array.from({length:10},(_,i)=>({id:'transport-'+i,task_id:'worker-'+i,work_kind:'diagnostic',
   current_run_state:'failed',current_error_code:'MODEL_TIMEOUT',evidence:[{attempt:3,code:'MODEL_TIMEOUT'}]}));
 const others=['STOP_UNCONFIRMED','AUTH_REQUIRED','QUOTA_EXCEEDED','RESULT_FORMAT'].map(code=>({id:code,current_run_state:'failed',current_error_code:code}));
 const actor={id:'live',current_run_state:'running',current_error_code:'MODEL_TIMEOUT'};
 const cases=[focal,...failures,...others,actor],before=structuredClone(cases);
 const window=selectManagerCaseWindow(cases,{caseId:focal.id});
 expect(window).toHaveLength(6);expect(window[0]).toEqual(focal);
 expect(window.slice(1).every(entry=>entry.operationalRecovery && entry.current_run_state==='failed' && entry.evidence.length===1)).toBe(true);
 expect(window.some(entry=>others.includes(entry)||entry.id===actor.id)).toBe(false);
 expect(cases).toEqual(before);
});

it('consumes available delivery evidence before another auxiliary plan without rotating equal priorities',()=>{
 const planning={id:'old-plan',task_id:'planner',work_kind:'planning',pending_dependencies:0};
 const blocked={id:'blocked-delivery',task_id:'blocked',pending_dependencies:2};
 const delivery={id:'deliver',task_id:'consumer',pending_dependencies:0,fresh_handoffs:3};
 const other={...delivery,id:'other'};
 const previous=selectManagerFocus([planning],null);
 expect(selectManagerFocus([planning,blocked,delivery],previous)?.caseId).toBe('deliver');
 expect(selectManagerFocus([other,delivery,planning],{caseId:'deliver'})?.caseId).toBe('deliver');
 expect(selectManagerFocus([planning,{...delivery,fresh_handoffs:0}],previous)?.caseId).toBe('deliver');
});

it('preserves the canonical delivery graph while making unrelated same-scope auxiliaries recoverable and keeping their live identities',()=>{
 const tasks=[{id:'final',baseWorkspace:'project/a',stage:'verify',state:'blocked_dependencies',dependencies:['implementation'],criteria:[{id:'final'}]},
  {id:'implementation',baseWorkspace:'project/a',stage:'integrate',state:'recovery_blocked',dependencies:['required-fix'],criteria:[{id:'verified'}]},
  {id:'required-fix',baseWorkspace:'project/a',workKind:'repair',state:'running',live:true,dependencies:[],criteria:[{id:'patch'}]},
  ...Array.from({length:100},(_,i)=>({id:'unrelated-'+i,baseWorkspace:'project/a',workKind:'diagnostic',state:i?'blocked_dependencies':'running',
    expectedAttempt:5,live:!i,dependencies:['source-'+i],criteria:[{id:'case-'+i}],intent:{text:'Distinct test '.repeat(100)}}))];
 const before=structuredClone(tasks),result=focusManagerTasks(tasks,{scope:'project/a',taskId:'implementation',contextMode:'delivery_graph'});
 expect(result.slice(0,3)).toEqual(tasks.slice(0,3));
 expect(result[3]).toMatchObject({id:'unrelated-0',live:true,expectedAttempt:5,selection:{detailOmitted:true,
   recovery:{kind:'task_metadata',taskId:'unrelated-0',expectedAttempt:5}}});
 expect(result[3]).not.toHaveProperty('criteria');expect(result[3]).not.toHaveProperty('intent');
 expect(result.every(t=>t.id)).toBe(true);expect(JSON.stringify(result).length).toBeLessThan(JSON.stringify(tasks).length/2);
 expect(tasks).toEqual(before);
});

it('keeps exact focal producers and immediate consumers without importing every sibling delivery dependency tree',()=>{
 const tasks=[{id:'focal',baseWorkspace:'project/a',state:'recovery_blocked',workKind:'diagnostic',dependencies:['patch'],
   criteria:[{id:'rtp',exact:'preserved'}],intent:{text:'Original focal instructions'}},
  {id:'patch',baseWorkspace:'project/a',state:'accepted',workKind:'repair',dependencies:['source'],criteria:[{id:'patch'}]},
  {id:'source',baseWorkspace:'project/a',state:'accepted',dependencies:[],criteria:[{id:'source'}]},
  {id:'consumer',baseWorkspace:'project/a',state:'blocked_dependencies',stage:'integrate',dependencies:['focal','sibling'],criteria:[{id:'integration'}]},
  {id:'final',baseWorkspace:'project/a',state:'blocked_dependencies',stage:'verify',dependencies:['consumer'],criteria:[{id:'qa'}]},
  ...Array.from({length:80},(_,i)=>({id:i?'sibling-'+i:'sibling',baseWorkspace:'project/a',state:'blocked_dependencies',
   stage:'integrate',dependencies:i?[]:['sibling-1'],criteria:[{id:'exact-'+i}],intent:{text:'Other canonical instructions '.repeat(1000)}}))];
 const original=structuredClone(tasks),result=focusManagerTasks(tasks,{scope:'project/a',taskId:'focal',contextMode:'delivery_graph'});
 expect(result.slice(0,4)).toEqual(tasks.slice(0,4));
 expect(result[4]).toMatchObject({id:'final',stage:'verify',selection:{fullSourcesPreserved:true,recovery:{kind:'task_metadata',taskId:'final'}}});
 expect(result.slice(5).every(task=>task.selection?.detailOmitted===true)).toBe(true);
 expect(result.map(task=>task.id)).toEqual(tasks.map(task=>task.id));
 expect(JSON.stringify(result).length).toBeLessThan(JSON.stringify(tasks).length/20);
 expect(tasks).toEqual(original);
});

it('keeps the same unresolved decision across reordered observations and moves on after application',()=>{
 const a={id:'case-a',task_id:'task-a',base_workspace:'research/docs'},
  b={id:'case-b',task_id:'task-b',base_workspace:'engineering/backend'};
 const first=selectManagerFocus([a,b],null);
 expect(selectManagerFocus([b,a],first)).toMatchObject({caseId:first?.caseId,taskId:first?.taskId,
   scope:first?.scope,sourceFingerprint:first?.sourceFingerprint});
 expect(selectManagerFocus([b],first)).toMatchObject({caseId:'case-b',scope:'engineering/backend'});
 expect(selectManagerFocus([],first)).toBeNull();
});
it('isolates decision context while preserving global live identities and recoverable criteria',()=>{
 const tasks=[{id:'task-a',baseWorkspace:'research/docs',criteria:[{id:'mandatory'}],intent:{text:'Exact task'},live:false},
  {id:'task-b',baseWorkspace:'engineering/backend',state:'running',expectedAttempt:4,live:true,
   criteria:[{id:'other'}],intent:{text:'Unrelated'.repeat(10000)},dependencies:['source-b']}];
 const before=structuredClone(tasks),result=focusManagerTasks(tasks,{scope:'research/docs',taskId:'task-a'});
 expect(result[0]).toEqual(tasks[0]);
 expect(result[1]).toMatchObject({id:'task-b',live:true,expectedAttempt:4,dependencySummary:{count:1},
  selection:{detailOmitted:true,omittedFields:['dependencies'],fullSourcesPreserved:true,
    recovery:{kind:'task',taskId:'task-b',expectedAttempt:4,graphApi:'GET /api/goals/:goalId'}}});
 expect(result[1]).not.toHaveProperty('dependencies');
 expect(result[1]).not.toHaveProperty('intent');expect(tasks).toEqual(before);
 expect(focusManagerTasks(tasks,null)).toEqual(tasks);
});
it('keeps the complete focal graph while preventing other scopes from copying large dependency histories',()=>{
 const dependencies=Array.from({length:500},(_,i)=>'dependency-'+i);
 const tasks=[{id:'focal',baseWorkspace:'project/a',dependencies,criteria:[{id:'mandatory'}]},
  {id:'other',baseWorkspace:'project/b',dependencies,state:'blocked_dependencies',expectedAttempt:7}];
 const original=structuredClone(tasks),projected=focusManagerTasks(tasks,{scope:'project/a',taskId:'focal'});
 expect(projected[0]).toEqual(tasks[0]);
 expect(projected[1]).toMatchObject({id:'other',expectedAttempt:7,dependencySummary:{count:500},
   selection:{fullSourcesPreserved:true,recovery:{graphTable:'dependencies',taskColumn:'task_id'}}});
 expect(projected[1].dependencySummary.sha256).toMatch(/^[a-f0-9]{64}$/);
 expect(JSON.stringify(projected[1]).length).toBeLessThan(1000);
 expect(tasks).toEqual(original);
});


it('serves one aged lower-priority case after every three high-priority reviews across 24 decisions',()=>{
 const now=Date.parse('2026-10-11T14:30:00Z'),created=new Date(now-managerFocusAgingMilliseconds-1).toISOString();
 const high={id:'handoff',task_id:'consumer',state:'open',decision_run_id:null,has_live_run:false,
  base_workspace:'project/high',pending_dependencies:0,fresh_handoffs:11,current_run_id:'high-run',current_attempt:176,
  current_run_state:'submitted',created_at:created,evidence:[{contract:'exact high handoff'}]};
 const low={id:'oom',task_id:'other-consumer',state:'open',decision_run_id:null,has_live_run:false,
  base_workspace:'project/low',pending_dependencies:0,fresh_handoffs:0,current_run_id:'low-run',current_attempt:80,
  current_run_state:'failed',current_error_code:'PROVIDER_ERROR',created_at:created,evidence:[{contract:'exact low evidence'}]};
 const cases=[high,low],before=structuredClone(cases),reviews:any[]=[],seen:string[]=[];let previous:any=null;
 for(let i=0;i<24;i++){
  const next=selectManagerFocus(cases,previous,undefined,reviews,now+i*120000)!;seen.push(next.caseId);
  if(i%4===3)expect(next.selectionReason).toBe('aged_review_quantum');
  reviews.unshift({caseId:next.caseId,sourceFingerprint:next.sourceFingerprint});previous=next;
 }
 expect(seen).toEqual(Array.from({length:6},()=>['handoff','handoff','handoff','oom']).flat());
 expect(selectManagerFocus(cases,previous,undefined,reviews,now+24*120000)?.caseId).toBe('handoff');
 expect(selectManagerCaseWindow(cases,{caseId:'handoff'}).map(c=>c.id)).toEqual(['handoff']);
 expect(cases).toEqual(before); // No auto-retry, source relabeling, handoff consumption or evidence edits.
});

it('measures aging from creation without resetting the wait on updated evidence or feedback',()=>{
 const now=Date.parse('2026-10-11T14:30:00Z');
 const high={id:'high',task_id:'high',state:'open',pending_dependencies:0,fresh_handoffs:1,current_attempt:5,current_run_state:'submitted'};
 const low={id:'low',task_id:'low',state:'open',decision_run_id:null,has_live_run:false,pending_dependencies:0,
  fresh_handoffs:0,current_attempt:3,current_run_state:'failed',created_at:new Date(now-managerFocusAgingMilliseconds).toISOString(),
  updated_at:new Date(now).toISOString()};
 const focus=selectManagerFocus([high,low],null,undefined,[],now)!;
 const reviews=Array.from({length:3},(_,i)=>({caseId:'high',sourceFingerprint:managerFocusSourceFingerprint({...high,fresh_handoffs:i+1})}));
 const changed={...high,current_artifacts:[{id:'new-artifact',digest:'current-proof'}]};
 expect(selectManagerFocus([changed,low],focus,undefined,reviews,now)).toMatchObject({caseId:'low',selectionReason:'aged_review_quantum'});
 const fresh={...low,created_at:new Date(now-managerFocusAgingMilliseconds+1).toISOString()};
 expect(selectManagerFocus([changed,fresh],focus,undefined,reviews,now)?.caseId).toBe('high');
 expect(selectManagerFocus([high,low],focus,undefined,reviews.slice(0,2),now)?.caseId).toBe('high');
 expect(selectManagerFocus([high,low],focus,undefined,[{caseId:'high'},{caseId:'high'},{caseId:'high'}],now)?.caseId).toBe('high');
});

it('does not borrow a live, uncertain, already decided/delegated or unbound-age case',()=>{
 const now=Date.parse('2026-10-11T14:30:00Z');
 const high={id:'high',task_id:'high',state:'open',pending_dependencies:0,fresh_handoffs:1,current_run_state:'submitted'};
 const low={id:'low',task_id:'low',state:'open',decision_run_id:null,has_live_run:false,pending_dependencies:0,
  fresh_handoffs:0,current_attempt:3,current_run_state:'failed',created_at:new Date(now-3600000).toISOString()};
 const focus=selectManagerFocus([high,low],null,undefined,[],now)!;
 const reviews=Array.from({length:3},()=>({caseId:'high',sourceFingerprint:focus.sourceFingerprint}));
 for(const guard of [{has_live_run:true},{has_live_run:undefined},{state:'decided'},{state:'delegated'},
   {state:'waiting_evidence'},{decision_run_id:'owner-review'},{current_run_state:'running'},
   {current_run_state:'queued'},{current_run_state:'cancelling'},{current_run_state:'reconciling'},
   {current_run_state:'unknown'},{created_at:undefined},{created_at:'invalid'},{created_at:new Date(now+1).toISOString()}])
  expect(selectManagerFocus([high,{...low,...guard}],focus,undefined,reviews,now)?.caseId).toBe('high');
 expect(selectManagerFocus([high,low],focus,undefined,reviews,NaN)?.caseId).toBe('high');
});

it.each(['submitted','failed','cancelled'])('borrows an open case with a bound historical %s review without rewriting its decision',state=>{
 const now=Date.parse('2026-10-17T03:51:28Z');
 const high={id:'high',task_id:'high',state:'open',pending_dependencies:0,fresh_handoffs:6,current_run_state:'submitted'};
 const repair={id:'repair',task_id:'repair-task',goal_id:'goal',work_kind:'repair',state:'open',
  decision_run_id:'old-review',decision_review_id:'old-review',decision_review_goal_id:'goal',
  decision_review_stage:'coordinate',decision_review_state:state,decision:{kind:'revise',instruction:'Preserved history'},
  has_live_run:false,pending_dependencies:0,fresh_handoffs:11,current_run_id:'repair-run',current_attempt:1,
  current_run_state:'submitted',current_task_state:'recovery_blocked',created_at:'2026-10-14T16:25:57Z',
  evidence:[{kind:'repair_source_proof_rejected',authority:'review_only',causeDigest:'a'.repeat(64)}]};
 const cases=[high,repair],before=structuredClone(cases),focus=selectManagerFocus(cases,null,undefined,[],now)!;
 const reviews=Array.from({length:3},(_,i)=>({eventId:String(30-i),caseId:high.id,sourceFingerprint:focus.sourceFingerprint,valid:true}));
 expect(selectManagerFocus(cases,focus,undefined,reviews.slice(0,2),now)?.caseId).toBe(high.id);
 expect(selectManagerFocus(cases,focus,undefined,reviews,now)).toMatchObject({caseId:repair.id,selectionReason:'aged_review_quantum'});
 expect(selectManagerFocus(cases,focus,undefined,reviews,now)).not.toHaveProperty('retryAuthorized');
 expect(selectManagerCaseWindow(cases,{caseId:high.id}).map(c=>c.id)).toEqual([high.id]);
 expect(cases).toEqual(before);
});

it.each([
 {decision_review_state:'queued'},{decision_review_state:'running'},{decision_review_state:'cancelling'},
 {decision_review_state:'reconciling'},{decision_review_state:'unknown'},{decision_review_state:null},
 {decision_review_id:null},{decision_review_id:'other-review'},{decision_review_goal_id:'another-goal'},
 {decision_review_goal_id:null},{decision_review_stage:'work'},{goal_id:undefined},
 {has_live_run:true},{has_live_run:undefined},{state:'decided'},{state:'delegated'},
 {state:'waiting_evidence'},{current_run_state:'reconciling'},
])('does not borrow historical review with unresolved identity or ownership [case %#]: %j',guard=>{
 const now=Date.parse('2026-10-17T03:51:28Z'),high={id:'high',state:'open',pending_dependencies:0,fresh_handoffs:6,current_run_state:'submitted'};
 const repair={id:'repair',task_id:'repair-task',goal_id:'goal',work_kind:'repair',state:'open',
  decision_run_id:'old-review',decision_review_id:'old-review',decision_review_goal_id:'goal',
  decision_review_stage:'coordinate',decision_review_state:'submitted',has_live_run:false,pending_dependencies:0,
  current_run_state:'submitted',current_attempt:1,created_at:'2026-10-14T16:25:57Z',...guard};
 const focus=selectManagerFocus([high,repair],null,undefined,[],now)!;
 const reviews=Array.from({length:3},()=>({caseId:high.id,sourceFingerprint:focus.sourceFingerprint}));
 expect(selectManagerFocus([high,repair],focus,undefined,reviews,now)?.caseId).toBe(high.id);
});

it('keeps urgent handoff preference but serves the least reviewed lower case before using priority as a tie breaker',()=>{
 const now=Date.parse('2026-10-11T14:30:00Z'),created_at=new Date(now-3600000).toISOString();
 const high={id:'high',task_id:'high',state:'open',pending_dependencies:0,fresh_handoffs:1,current_run_state:'submitted'};
 const lower=(id:string)=>({id,task_id:id,state:'open',decision_run_id:null,has_live_run:false,pending_dependencies:0,
  fresh_handoffs:0,current_attempt:3,current_run_state:'failed',created_at});
 const a=lower('a'),b=lower('b'),aux={...lower('aux'),work_kind:'repair'},focus=selectManagerFocus([high,a,b,aux],null,undefined,[],now)!;
 const quantum=Array.from({length:3},()=>({caseId:'high',sourceFingerprint:focus.sourceFingerprint}));
 expect(selectManagerFocus([high,a,b,aux],focus,undefined,[...quantum,{caseId:'a',sourceFingerprint:managerFocusSourceFingerprint(a)}],now)?.caseId).toBe('b');
 expect(selectManagerFocus([high,a,b],{caseId:'a'},undefined,quantum,now)?.caseId).toBe('high');
 expect(selectManagerFocus([high,a,b,aux],focus,undefined,[...quantum,
  {caseId:'a',sourceFingerprint:managerFocusSourceFingerprint(a)},
  {caseId:'b',sourceFingerprint:managerFocusSourceFingerprint(b)}],now)?.caseId).toBe('aux');
});

it('reserves three of four reviews for the urgent case while all aged priority classes receive attention over 24 reviews',()=>{
 const now=Date.parse('2026-10-11T14:30:00Z'),created_at=new Date(now-3600000).toISOString();
 const high={id:'high',task_id:'high',state:'open',pending_dependencies:0,fresh_handoffs:1,current_run_state:'submitted'};
 const lower=(id:string,work_kind?:string)=>({id,task_id:id,state:'open',decision_run_id:null,has_live_run:false,pending_dependencies:0,
  work_kind,fresh_handoffs:0,current_attempt:3,current_run_state:'failed',created_at});
 const cases=[high,lower('canonical'),lower('repair','repair'),lower('planning','planning')],before=structuredClone(cases);
 let history:any=null,previous:any=null;const reviews:any[]=[],seen:string[]=[];
 for(let i=0;i<24;i++){
  history=managerFocusServiceHistory(cases,history,reviews.slice(0,40));
  const next=selectManagerFocus(cases,previous,undefined,reviews.slice(0,40),now+i*120000,history)!;
  seen.push(next.caseId);reviews.unshift({caseId:next.caseId,sourceFingerprint:next.sourceFingerprint,eventId:String(i+1),valid:true});previous=next;
 }
 expect(seen.filter(id=>id==='high')).toHaveLength(18);
 expect(seen.filter(id=>id!=='high')).toEqual(['canonical','repair','planning','canonical','repair','planning']);
 expect(cases).toEqual(before);
});

it('preserves fair order across restart and a 40-event window for 50 waiting cases over 240 reviews',()=>{
 const now=Date.parse('2026-10-11T14:30:00Z'),created_at=new Date(now-3600000).toISOString();
 const high={id:'high',task_id:'high',state:'open',pending_dependencies:0,fresh_handoffs:1,current_run_state:'submitted'};
 const lower=Array.from({length:50},(_,i)=>({id:'lower-'+String(i).padStart(2,'0'),task_id:'task-'+i,state:'open',decision_run_id:null,
  has_live_run:false,pending_dependencies:0,fresh_handoffs:0,current_attempt:3,current_run_state:'failed',created_at,
  work_kind:i%3===0?undefined:i%3===1?'repair':'planning'}));
 const cases=[high,...lower],reviews:any[]=[],seen:string[]=[];let previous:any=null,history:any=null;
 for(let i=0;i<240;i++){
  // Only controller JSON survives restart; history is never rebuilt from just the last forty events.
  history=managerFocusServiceHistory(cases,JSON.parse(JSON.stringify(history)),reviews.slice(0,40));
  const next=selectManagerFocus(cases,previous,undefined,reviews.slice(0,40),now+i*120000,history)!;
  seen.push(next.caseId);reviews.unshift({caseId:next.caseId,sourceFingerprint:next.sourceFingerprint,
   eventId:(90071992547409930n+BigInt(i)).toString(),valid:true});previous=JSON.parse(JSON.stringify(next));
 }
 expect(seen.filter(id=>id==='high')).toHaveLength(180);
 expect(new Set(seen.filter(id=>id!=='high')).size).toBe(50);
 expect(new Set(seen.filter(id=>id!=='high').slice(0,50)).size).toBe(50);
 expect(Object.keys(history.lastReviewed)).toHaveLength(51);
 expect(BigInt(history.lastReviewed.high)).toBeGreaterThan(90071992547409930n);
});

it('returns each borrowed review to the least served high-priority peer instead of restarting the first peer forever',()=>{
 const now=Date.parse('2026-10-11T14:30:00Z'),created_at=new Date(now-3600000).toISOString();
 const base={state:'open',decision_run_id:null,has_live_run:false,pending_dependencies:0,current_attempt:3,current_run_state:'failed',created_at};
 const cases=[{...base,id:'high-a',fresh_handoffs:1},{...base,id:'high-b',fresh_handoffs:1},
  {...base,id:'canonical'},{...base,id:'repair',work_kind:'repair'},{...base,id:'planning',work_kind:'planning'}];
 const reviews:any[]=[],seen:string[]=[];let previous:any=null,history:any=null;
 for(let i=0;i<48;i++){
  history=managerFocusServiceHistory(cases,history,reviews.slice(0,40));
  const next=selectManagerFocus(cases,previous,undefined,reviews.slice(0,40),now+i*120000,history)!;
  seen.push(next.caseId);reviews.unshift({caseId:next.caseId,sourceFingerprint:next.sourceFingerprint,eventId:String(i+1),valid:true});previous=next;
 }
 expect(seen.filter(id=>id==='high-a')).toHaveLength(18);
 expect(seen.filter(id=>id==='high-b')).toHaveLength(18);
 for(const id of ['canonical','repair','planning'])expect(seen.filter(seenId=>seenId===id)).toHaveLength(4);
 expect(seen.slice(0,8)).toEqual(['high-a','high-a','high-a','canonical','high-b','high-b','high-b','repair']);
});

it('prunes only closed cases and never advances service history from invalid or uncommitted review proposals',()=>{
 const cases=[{id:'open',state:'open'},{id:'omitted-open',state:'open'},{id:'closed',state:'delegated'}],fp='a'.repeat(64);
 const prior={version:1,lastReviewed:{open:'90071992547409931','omitted-open':'90071992547409932',closed:'90071992547409933'}};
 const history=managerFocusServiceHistory(cases,prior,[{caseId:'open',eventId:'90071992547409999',sourceFingerprint:fp,valid:false},
  {caseId:'open',sourceFingerprint:fp},{caseId:'open',eventId:'90071992547409998',sourceFingerprint:'malformed'},
  {caseId:'other-goal',eventId:'90071992547409999',sourceFingerprint:fp}]);
 expect(history).toEqual({version:1,lastReviewed:{open:'90071992547409931','omitted-open':'90071992547409932'}});
 expect(managerFocusServiceHistory(cases,history,[{caseId:'open',eventId:'90071992547409930',sourceFingerprint:fp}])).toEqual(history);
 expect(prior.lastReviewed.closed).toBe('90071992547409933');
});

it('reserves one stopped incomplete delivery alongside semantic focus and transports without granting retry',()=>{
 const focal={id:'focal',current_run_state:'submitted'},
 qa={id:'qa',state:'open',source_run_id:'qa-run',current_run_id:'qa-run',has_live_run:false,current_run_state:'failed',
 current_error_code:'INCOMPLETE_TURN',created_at:'2026-10-13T18:19:00Z',evidence:[{immutable:'original failure'}]},
 next={...qa,id:'next',current_error_code:'MODEL_OUTPUT_LIMIT',created_at:'2026-10-13T19:00:00Z'};
 const transports=Array.from({length:9},(_,i)=>({id:'transport-'+i,current_run_state:'failed',current_error_code:'MODEL_TIMEOUT'}));
 const cases=[focal,...transports,next,qa],before=structuredClone(cases),selected=selectManagerCaseWindow(cases,{caseId:'focal'});
 expect(selected.map(c=>c.id)).toEqual(['focal','qa',...transports.slice(0,4).map(c=>c.id)]);
 expect(selected[1]).toMatchObject({operationalRecovery:true,evidence:qa.evidence});
 expect(cases).toEqual(before);expect(selected[1]).not.toHaveProperty('retryAuthorized');
});
it('does not reserve delivery review for live, stale-source or already decided cases',()=>{
 const focal={id:'focal',current_run_state:'submitted'},qa={id:'qa',state:'open',source_run_id:'run',current_run_id:'run',
 has_live_run:false,current_run_state:'failed',current_error_code:'MODEL_OUTPUT_LIMIT',created_at:'2026-10-13T18:19:00Z'};
 for(const guard of [{has_live_run:true},{current_run_state:'running'},{source_run_id:'obsolete'},{state:'decided'}])
 expect(selectManagerCaseWindow([focal,{...qa,...guard}],{caseId:'focal'}).map(c=>c.id)).toEqual(['focal']);
});

it('services an unseen current interrupted delivery after three accepted reviews even when it is much newer than fifty waiting cases',()=>{
 const now=Date.parse('2026-10-13T21:00:00Z'),fp='a'.repeat(64);
 const high={id:'high',task_id:'high',state:'open',pending_dependencies:0,fresh_handoffs:1,current_run_state:'submitted'};
 const old=Array.from({length:50},(_,i)=>({id:'old-'+i,task_id:'old-'+i,state:'open',has_live_run:false,
 current_run_state:'failed',current_attempt:2,pending_dependencies:0,created_at:'2026-10-11T10:00:00Z',work_kind:'repair'}));
 const qa={id:'qa',task_id:'qa',state:'open',decision_run_id:null,has_live_run:false,pending_dependencies:0,
 current_task_state:'recovery_blocked',current_run_state:'failed',current_run_id:'qa-run',source_run_id:'qa-run',
 current_attempt:2,current_error_code:'INCOMPLETE_TURN',created_at:'2026-10-13T18:19:00Z',base_workspace:'project/qa'};
 const cases=[high,...old,qa],before=structuredClone(cases),prior=selectManagerFocus(cases,null,undefined,[],now);
 const reviews=Array.from({length:3},(_,i)=>({caseId:i===0?'different-high':'high',sourceFingerprint:fp,eventId:String(100-i),valid:true}));
 const selected=selectManagerFocus(cases,prior,undefined,reviews,now,{version:1,lastReviewed:{high:'99'}});
 expect(selected).toMatchObject({caseId:'qa',scope:'project/qa',selectionReason:'unseen_interrupted_delivery'});
 expect(cases).toEqual(before);
 expect(selectManagerFocus(cases,prior,undefined,reviews,now,{version:1,lastReviewed:{high:'99',qa:'80'}})?.caseId).not.toBe('qa');
});
it('never promotes interrupted delivery on stale ownership, an active actor, a decided case or incomplete review evidence',()=>{
 const now=Date.parse('2026-10-13T21:00:00Z'),fp='a'.repeat(64),high={id:'high',state:'open',pending_dependencies:0,fresh_handoffs:1,current_run_state:'submitted'},
 qa={id:'qa',state:'open',decision_run_id:null,has_live_run:false,current_task_state:'recovery_blocked',current_run_state:'failed',
 source_run_id:'qa-run',current_run_id:'qa-run',current_attempt:2,current_error_code:'MODEL_OUTPUT_LIMIT',created_at:'2026-10-13T18:19:00Z'};
 const reviews=Array.from({length:3},(_,i)=>({caseId:'high',sourceFingerprint:fp,eventId:String(100-i),valid:true}));
 const prior=selectManagerFocus([high,qa],null,undefined,[],now);
 for(const change of [{source_run_id:'old'},{has_live_run:true},{state:'decided'},{decision_run_id:'owned'},
 {current_run_state:'running'},{current_task_state:'accepted'},{current_attempt:0},{current_error_code:'AUTH_REQUIRED'},
 {created_at:new Date(now).toISOString()}]){
  const selected=selectManagerFocus([high,{...qa,...change}],prior,undefined,reviews,now);
  expect(selected?.selectionReason).not.toBe('unseen_interrupted_delivery');
 }
 for(const incomplete of [reviews.slice(0,2),reviews.map(r=>({...r,valid:false})),reviews.map(r=>({...r,sourceFingerprint:'invalid'}))])
  expect(selectManagerFocus([high,qa],prior,undefined,incomplete,now)?.selectionReason).not.toBe('unseen_interrupted_delivery');
});

it('gives the real context-too-large alias a focal review and delivery slot without granting a retry',()=>{
 const now=Date.parse('2026-10-16T13:50:00Z'),fp='a'.repeat(64);
 const high={id:'high',task_id:'high',state:'open',pending_dependencies:0,fresh_handoffs:1,current_run_state:'submitted'};
 const qa={id:'qa',task_id:'qa',state:'open',decision_run_id:null,has_live_run:false,pending_dependencies:0,
  current_task_state:'recovery_blocked',current_run_state:'failed',current_run_id:'qa-run',source_run_id:'qa-run',
  current_attempt:7,current_error_code:'CONTEXT_TOO_LARGE',created_at:'2026-10-16T03:07:00Z',
  evidence:[{kind:'INDEPENDENT_QA_PILOT_EVIDENCE',candidateDigest:'same-candidate'}]};
 const cases=[high,qa],before=structuredClone(cases),prior=selectManagerFocus(cases,null,undefined,[],now);
 const reviews=Array.from({length:3},(_,i)=>({caseId:i===0?'another-high':'high',sourceFingerprint:fp,eventId:String(100-i),valid:true}));
 const selected=selectManagerFocus(cases,prior,undefined,reviews,now,{version:1,lastReviewed:{high:'99'}});
 expect(selected).toMatchObject({caseId:'qa',selectionReason:'unseen_interrupted_delivery'});
 expect(selectManagerCaseWindow(cases,{caseId:'high'})).toMatchObject([{id:'high'},{id:'qa',operationalRecovery:true}]);
 expect(selected).not.toHaveProperty('retryAuthorized');expect(cases).toEqual(before);
 for(const change of [{source_run_id:'old-run'},{has_live_run:true},{current_run_state:'reconciling'},
  {state:'decided'},{decision_run_id:'existing-owner'}]){
  const guarded=[high,{...qa,...change}];
  expect(selectManagerFocus(guarded,prior,undefined,reviews,now)?.selectionReason).not.toBe('unseen_interrupted_delivery');
  expect(selectManagerCaseWindow(guarded,{caseId:'high'}).map(entry=>entry.id)).toEqual(['high']);
 }
});
