import {it,expect} from 'vitest';
import {createHash} from 'node:crypto';
import {planningInputContext} from '../server/planning-input-context.js';
const input=(id:string,content:string|Buffer,requirementIds=['req-4'])=>{
  const bytes=Buffer.from(content);
  return {id,name:id+'.txt',digest:createHash('sha256').update(bytes).digest('hex'),base64:bytes.toString('base64'),requirementIds};
};
it('includes whole relevant source guidance with byte identity and isolates unrelated requirements',()=>{
  const source=input('source','Actual callers are bridge/runtime-runner.mjs and server/planner.ts.');
  const result=planningInputContext([input('unrelated','Other task',['req-2']),source],['req-4']);
  expect(result).toMatchObject({permissionChanges:false,inventoryComplete:true,contentComplete:true});
  expect(result.inputs).toEqual([{id:source.id,name:source.name,digest:source.digest,size:Buffer.byteLength(Buffer.from(source.base64,'base64')),
    requirementIds:['req-4'],inputPath:'/home/agent/inputs/source/source.txt',content:Buffer.from(source.base64,'base64').toString()}]);
});
it('prioritizes newer complete inputs and accounts for every omission without slicing programs',()=>{
  const result=planningInputContext([input('old','x'.repeat(6000)),input('mid','y'.repeat(6000)),input('new','z'.repeat(6000)),input('large','q'.repeat(6001))],['req-4']);
  expect(result.contentCharacters).toBe(12000);expect(result.contentComplete).toBe(false);
  expect(result.inputs.map(i=>({id:i.id,omitted:'omitted' in i?i.omitted:undefined}))).toEqual([
    {id:'large',omitted:'per-input-budget'},{id:'new',omitted:undefined},{id:'mid',omitted:undefined},{id:'old',omitted:'total-budget'}]);
  for(const item of result.inputs.filter(i=>'omitted' in i))expect(item).not.toHaveProperty('content');
});
it('does not decode binary or invalid UTF8 into apparent source evidence',()=>{
  const result=planningInputContext([input('binary','abc\0def'),input('invalid',Buffer.from([0xff,0xfe]))],['req-4']);
  expect(result.inputs.map(i=>'omitted' in i?i.omitted:null)).toEqual(['non-utf8','binary']);
});
it('counts unicode as complete text and does not mutate the verified inputs',()=>{
  const inputs=[input('unicode','Coordenação e publicação ✓')],before=structuredClone(inputs);
  expect(planningInputContext(inputs,['req-4']).inputs[0]).toHaveProperty('content','Coordenação e publicação ✓');
  expect(inputs).toEqual(before);expect(planningInputContext(inputs,['req-9']).inputs).toEqual([]);
});
it('refuses modified bytes before they can be presented to the model',()=>{
  const forged={...input('source','original'),base64:Buffer.from('forged').toString('base64')};
  expect(()=>planningInputContext([forged],['req-4'])).toThrow('verified digest');
});

it('uses a complete operator summary for a large input without implying the original was read',()=>{
  const summary='A complete operator summary of this large profile, to be checked against the original evidence.';
  const source={...input('profile','p'.repeat(49000)),planningSummary:summary};
  const result=planningInputContext([source],['req-4']);
  expect(result).toMatchObject({contentComplete:false,contentCharacters:0,summaryCharacters:summary.length,contextCharacters:summary.length,permissionChanges:false});
  expect(result.inputs[0]).toMatchObject({id:source.id,digest:source.digest,omitted:'per-input-budget',summary,
    summaryDigest:createHash('sha256').update(summary).digest('hex'),summarySource:'authenticated-operator',originalContentRead:false});
  expect(result.inputs[0]).not.toHaveProperty('content');
});

it('prioritizes a new summary over older full content within the same total budget',()=>{
  const newest={...input('profile','x'.repeat(49000)),planningSummary:'s'.repeat(3000)};
  const result=planningInputContext([input('old','o'.repeat(6000)),input('mid','m'.repeat(6000)),newest],['req-4']);
  expect(result).toMatchObject({contentCharacters:6000,summaryCharacters:3000,contextCharacters:9000});
  expect(result.inputs[0]).toHaveProperty('summary',newest.planningSummary);
  expect(result.inputs[1]).toHaveProperty('content');
  expect(result.inputs[2]).toHaveProperty('omitted','total-budget');
});

it('marks a summary omitted when even the complete summary does not fit, without slicing it',()=>{
  const large={...input('profile','x'.repeat(49000)),planningSummary:'s'.repeat(3000)};
  const result=planningInputContext([large,input('mid','m'.repeat(4000)),input('new','n'.repeat(6000))],['req-4']);
  expect(result).toMatchObject({contentCharacters:10000,summaryCharacters:0,contextCharacters:10000});
  expect(result.inputs[2]).toMatchObject({omitted:'per-input-budget',summaryOmitted:'total-budget'});
  expect(result.inputs[2]).not.toHaveProperty('summary');
});

it('can describe a binary input with an explicit summary while keeping the binary unread',()=>{
  const source={...input('binary',Buffer.from([0xff,0xfe])),planningSummary:'This is an operator description of an opaque binary artifact.'};
  const entry=planningInputContext([source],['req-4']).inputs[0];
  expect(entry).toMatchObject({omitted:'non-utf8',summary:source.planningSummary,originalContentRead:false});
  expect(entry).not.toHaveProperty('content');
});

it('does not replace a fitting original with its summary or admit a forged byte identity',()=>{
  const source={...input('short','complete original text'),planningSummary:'A separate claim that is unnecessary when the original fits.'};
  expect(planningInputContext([source],['req-4']).inputs[0]).toHaveProperty('content','complete original text');
  expect(planningInputContext([source],['req-4']).inputs[0]).not.toHaveProperty('summary');
  expect(()=>planningInputContext([{...source,base64:Buffer.from('changed').toString('base64')}],['req-4'])).toThrow('verified digest');
});
