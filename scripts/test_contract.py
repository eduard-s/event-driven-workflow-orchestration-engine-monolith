"""Public contract tests for both isolated implementations."""
import asyncio
import json
from pathlib import Path
from uuid import uuid4

import httpx
import pytest

from compare import ready, start, submit, trigger, completed, request, audit

ROOT = Path(__file__).resolve().parents[1]

def n(identifier, handler="input", parents=(), **fields):
    return {"id": identifier, "handler": handler, "dependencies": list(parents), **fields}

def graph(*nodes):
    return {"name": "contract", "dag": {"nodes": list(nodes)}}

async def contract(kind):
    await start(kind)
    base = "http://localhost:29500" if kind == "go" else "http://localhost:29501"
    async with httpx.AsyncClient(base_url=base, timeout=20) as c:
        invalid = [
            graph(n("a", parents=["a"])),
            graph(n("a", parents=["b"]), n("b", parents=["a"])),
            graph(n("a", parents=["missing"])),
            graph(n("a"), n("a")),
            graph(n("a"), n("b", "llm", ["a", "a"], config={"prompt":"x"})),
            graph(n("a", "output"), n("b", parents=["a"])),
            graph(n("a", "unknown")),
            graph(n("a"), n("b", "llm", ["a"], config={"prompt":"{{ b }}"})),
            graph(n("a"), n("b", "llm", ["a"], config={"prompt":"{{ missing }}"})),
            graph(n("a", "llm", config={"prompt":3})),
            graph(n("a", "call_external_service", config={"url":"ftp://invalid"})),
            graph(n("bad id")),
        ]
        for definition in invalid:
            r = await c.post("/workflow", json=definition)
            assert r.status_code == 422, (definition,r.status_code,r.text)
        r = await c.post("/workflow",content="{} {}", headers={"Content-Type":"application/json"})
        assert r.status_code == 422
        r = await c.get("/workflows/"+str(uuid4()))
        assert r.status_code == 404
        r = await c.get("/workflows/not-a-uuid")
        assert r.status_code == 422
        definition = graph(n("a"), n("b","llm",["a"],config={"prompt":"Value {{ a[\"a.b\"][1] }}"}),n("out","output",["b"],data="{{ b }}"))
        identifier = await submit(c,definition)
        initial = await request(c,"GET",f"/workflows/{identifier}",200)
        assert initial["state"] == "PENDING" and all(x["starts"]==0 for x in initial["nodes"])
        r = await c.get(f"/workflows/{identifier}/results")
        assert r.status_code == 409 and r.json()["partial_outputs"] == {}
        for overrides in ({"unknown":{}},{"b":{"prompt":3}},{"b":{"prompt":"{{ b }}"}},{"b":{"url":"x"}}):
            r = await c.post(f"/workflow/trigger/{identifier}",json={"node_params":overrides})
            assert r.status_code == 422,(r.status_code,r.text)
        await request(c,"POST",f"/workflow/trigger/{identifier}",202,json={"input":{"a.b":[False,9007199254740993]}})
        value = await completed(c,identifier)
        assert value["outputs"] == {"out":"mock: Value 9007199254740993"},value
        r = await c.post(f"/workflow/trigger/{identifier}",json={})
        assert r.status_code == 409
        for query in ("limit=0","limit=101","offset=-1"):
            r = await c.get(f"/workflows/{identifier}?{query}")
            assert r.status_code == 422
        page = await request(c,"GET",f"/workflows/{identifier}?limit=1&offset=1",200)
        assert page["total_nodes"] == 3 and len(page["nodes"]) == 1
        for raw,expected in ((None,None),(False,False),([1,"{{ literal }}"],[1,"{{ literal }}"])):
            d=graph(n("a"),n("out","output",["a"],data="{{ a }}"))
            identifier=await submit(c,d)
            await request(c,"POST",f"/workflow/trigger/{identifier}",202,json={"input":raw})
            value=await completed(c,identifier)
            assert value["outputs"] == {"out":expected}
        d=graph(n("a"),n("b","llm",["a"],config={"prompt":"x"}),n("c","llm",["a"],config={"prompt":"y"}))
        identifier=await submit(c,d);await trigger(c,identifier)
        assert (await completed(c,identifier))["outputs"] == {"b":"mock: x","c":"mock: y"}
        d=graph(n("a"),*(n(f"b{i}","llm",["a"],config={"prompt":"{{ a.id }}"}) for i in range(24)),n("out","output",[f"b{i}" for i in range(24)]))
        identifier=await submit(c,d);await trigger(c,identifier)
        value=await completed(c,identifier)
        assert len(value["outputs"]["out"])==24
        assert audit(kind,[identifier])["tries"]==26
        key=str(uuid4());definition=graph(n("a"))
        ids=await asyncio.gather(*(submit(c,definition,key) for _ in range(12)))
        assert len(set(ids))==1
        repeated=await submit(c,graph(n("different")),key)
        assert repeated==ids[0]
        identifier=ids[0]
        async def compete():
            while True:
                r=await c.post(f"/workflow/trigger/{identifier}",json={})
                if r.status_code not in (429,503):return r.status_code
                await asyncio.sleep(.025)
        codes=await asyncio.gather(*(compete() for _ in range(12)))
        assert codes.count(202)==1 and codes.count(409)==11,codes
        await completed(c,identifier)
        assert audit(kind,[identifier])["tries"]==1

@pytest.mark.parametrize("kind",["go","python"])
def test_public_contract(kind):
    asyncio.run(contract(kind))
