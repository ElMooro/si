#!/usr/bin/env node
'use strict';
// Uses this reviewed local model only; never execute JavaScript from an export.
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto');
const modelPath=path.join(__dirname,'..','jh-portfolio-scenario.js');
const model=require(modelPath);
const io=require('../jh-portfolio-scenario-io.js');
function verify(packet){
  const raw=fs.readFileSync(modelPath),sha=crypto.createHash('sha256').update(raw).digest('hex');
  return io.verify(packet,{contract:model.CONTRACT,sha256:sha,bytes:raw.length},model);
}
function readExport(filename){
  const fd=fs.openSync(filename,'r');
  try{
    const before=fs.fstatSync(fd);if(!before.isFile()||before.size>io.LIMIT)throw Error('Export is not a regular file within the 4 MB bound');
    const chunks=[];let total=0;
    for(;;){const block=Buffer.alloc(Math.min(65536,io.LIMIT+1-total)),size=fs.readSync(fd,block,0,block.length,null);if(!size)break;total+=size;if(total>io.LIMIT)throw Error('Export exceeds bound');chunks.push(block.subarray(0,size));}
    const after=fs.fstatSync(fd);
    if(total!==before.size||after.size!==before.size||after.mtimeMs!==before.mtimeMs||after.ctimeMs!==before.ctimeMs)throw Error('Export changed while being read');
    return io.decode(Buffer.concat(chunks,total));
  }finally{fs.closeSync(fd);}
}
if(require.main===module){
  try{
    if(process.argv.length!==3)throw Error('Usage: node scripts/replay_portfolio_scenario.cjs <export.json>');
    const out=verify(readExport(process.argv[2]));
    process.stdout.write(JSON.stringify({verified:true,model:out.contract,positions:out.positions.length,total_pnl_pct_nav:out.total_pnl_pct_nav,ending_nav:out.ending_nav})+'\n');
  }catch(error){process.stderr.write('Scenario replay rejected: '+error.message+'\n');process.exitCode=1;}
}
module.exports={verify,readExport};
