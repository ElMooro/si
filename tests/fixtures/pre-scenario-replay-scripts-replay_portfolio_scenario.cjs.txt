#!/usr/bin/env node
'use strict';
// Uses this reviewed local model only; never execute JavaScript from an export.
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto'),assert=require('node:assert/strict');
const modelPath=path.join(__dirname,'..','jh-portfolio-scenario.js');
const model=require(modelPath);
function verify(packet){
  assert.deepEqual(Object.keys(packet??{}).sort(),['contract','input','model','output']);
  assert.equal(packet?.contract,'portfolio-scenario-export.v1');
  const raw=fs.readFileSync(modelPath),sha=crypto.createHash('sha256').update(raw).digest('hex');
  assert.deepEqual(packet.model,{contract:model.CONTRACT,sha256:sha,bytes:raw.length});
  const output=model.calculate(packet.input);
  assert.deepEqual(packet.output,output,'Exported result differs from complete deterministic replay');
  return output;
}
if(require.main===module){
  try{
    if(process.argv.length!==3)throw Error('Usage: node scripts/replay_portfolio_scenario.cjs <export.json>');
    const stat=fs.statSync(process.argv[2]);if(stat.size>4*1024*1024)throw Error('Export exceeds bound');
    const out=verify(JSON.parse(fs.readFileSync(process.argv[2],'utf8')));
    process.stdout.write(JSON.stringify({verified:true,model:out.contract,positions:out.positions.length,total_pnl_pct_nav:out.total_pnl_pct_nav,ending_nav:out.ending_nav})+'\n');
  }catch(error){process.stderr.write('Scenario replay rejected: '+error.message+'\n');process.exitCode=1;}
}
module.exports={verify};
