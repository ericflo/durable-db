import { describe, expect, it } from "vitest";
import { registerStorageConformance } from "@earendil-works/pi-durable/testing";
import { DurableDb } from "./client.mjs";
import { openReferencePiStorage } from "./pi-reference.mjs";
import { readFileSync } from "node:fs";

const url=process.env.DURABLE_DB_TEST_URL;
const token=process.env.DURABLE_DB_TEST_TOKEN_FILE ? readFileSync(process.env.DURABLE_DB_TEST_TOKEN_FILE,"utf8").trim() : "";
const suite=url ? describe : describe.skip;
suite("pi-durable 1.5.0 against the HTTP database",()=>{
  registerStorageConformance({describe,expect,it},"durable-db reference adapter",async use=>{
    const database=`pi-${crypto.randomUUID()}`;
    const client=new DurableDb({url,database,token});
    // The development server deliberately limits even administrators. Keep this suite
    // sequential and retry bounded admission failures; every case uses a fresh database.
    for(let n=0;;n++) { try {await client.create();break;} catch(e) {if(e.status!==429||n>20)throw e;await new Promise(r=>setTimeout(r,250));} }
    const storage=await openReferencePiStorage(client);
    try {await use(storage);} finally {await storage.close();}
  });
});
