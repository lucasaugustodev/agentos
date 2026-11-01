// Actual SDK tool execution with a deterministic adapter, no provider/account.
import assert from 'node:assert/strict';
import {readFile,writeFile} from 'node:fs/promises';
import {LlmAdapter} from '/opt/deepseek-harness/app/node_modules/@deepseek-ai/dsh-llm/lib/index.js';

export const name='supervisor-tool-driver-fixture';
export const inject=['llm','tools'];
export function apply(ctx){
  const supervisor=process.env.AGENTOS_SUPERVISOR_TOOL_PROFILE==='supervisor-v1';
  const tool=ctx.tools.get('bash');
  assert.deepEqual(Object.keys(tool.parameters.properties),['command']);
  class Scripted extends LlmAdapter{
    step=0;
    async resolveModel(provider,id){return {provider,id,name:id,context:{contextWindow:128000},defaultMaxTokens:1024};}
    async *stream(options){
      options.signal.throwIfAborted();
      const step=this.step++;
      assert.ok(step<4,'unexpected additional fixture model call');
      let block;
      if(step===0){
        block={type:'tool-call',id:'fixture_cpu',name:'bash',arguments:JSON.stringify({command:'python3 /workspace/cpu-fixture.py'})};
      }else if(step===1){
        const messages=JSON.stringify(options.messages);
        if(supervisor){
          assert.match(messages,/timed out after 1 seconds/);
          assert.match(messages,/persistent bash shell was reset/);
        }else{
          assert.match(messages,/WORKER_COMPLETED_BEYOND_FIXTURE_SUPERVISOR_LIMIT/);
          assert.doesNotMatch(messages,/persistent bash shell was reset/);
        }
        block={type:'tool-call',id:'fixture_recovery',name:'bash',arguments:JSON.stringify({command:'python3 /workspace/check-fixture.py'})};
      }else{
        assert.match(JSON.stringify(options.messages),/SAME_SESSION_NEXT_COMMAND_SUCCEEDED/);
        const identity=JSON.parse(await readFile('/run/fixture-identity.json','utf8'));
        assert.equal(await readFile('/workspace/document.txt','utf8'),'immutable document');
        await writeFile('/workspace/evidence/sdk-timeout-proof.json',JSON.stringify({
          role:supervisor?'supervisor':'worker',modelSteps:this.step,
          runId:identity.runId,runFence:identity.runFence,sessionPreserved:true,
          supervisorFixtureTimeoutMs:1000,workerDefaultTimeoutMs:300000,
          foregroundStopped:true,documentPreserved:true,nextCommandSucceeded:true,
        }));
        block={type:'text',text:'SAME_SESSION_COMPLETED_'+step};
      }
      yield {type:'block-start',index:0,blockType:block.type};
      yield {type:'block-end',index:0,block};
      yield {type:'finish',reason:{kind:block.type==='tool-call'?'tool-calls':'stop'}};
    }
  }
  ctx.llm.registerAdapter(['supervisor-tool-scripted-fixture'],new Scripted());
}
