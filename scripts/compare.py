#!/usr/bin/env python3
"""Compare isolated Go and Python deployments with the same public requests."""
from __future__ import annotations
import argparse
import asyncio
import json
import math
import statistics
import subprocess
import time
from pathlib import Path
from uuid import uuid4

import httpx
import psycopg

ROOT = Path(__file__).resolve().parents[1]
PROJECT = "workflow-go-compare"
FILE = ROOT / "scripts/compare.compose.yml"
URLS = {"go": "http://localhost:29500", "python": "http://localhost:29501"}
APP = {"go": ["go"], "python": ["api", "orchestrator", "worker"]}
ALL = {"go": ["go", "pg-go"], "python": ["api", "orchestrator", "worker", "pg-python", "redis"]}
COUNTS = {"503": 0, "429": 0}
DSNS = {"go": "postgresql://workflow:workflow@localhost:29532/workflow", "python": "postgresql://workflow:workflow@localhost:29533/workflow"}

def compose(*args):
    result = subprocess.run(["docker", "compose", "-p", PROJECT, "-f", str(FILE), *args], cwd=ROOT, capture_output=True, text=True, timeout=120)
    if result.returncode:
        raise RuntimeError(result.stdout + result.stderr)
    return result.stdout.strip()

def container(service):
    return compose("ps", "--all", "--quiet", service)

async def ready(kind, timeout=60):
    deadline = time.monotonic() + timeout
    async with httpx.AsyncClient(timeout=2) as client:
        while time.monotonic() < deadline:
            try:
                r = await client.get(URLS[kind] + "/health/ready")
                if r.status_code == 200:
                    return
            except httpx.HTTPError:
                pass
            await asyncio.sleep(.1)
    raise AssertionError(f"{kind} did not become ready")

async def start(kind):
    other = "python" if kind == "go" else "go"
    await asyncio.to_thread(compose, "stop", *APP[other])
    await asyncio.to_thread(compose, "up", "--detach", *APP[kind])
    await ready(kind)

async def request(client, method, path, expected, **kwargs):
    deadline = time.monotonic() + 60
    while True:
        r = await client.request(method, path, **kwargs)
        if r.status_code == expected:
            return r.json()
        if r.status_code not in (429, 503) or time.monotonic() >= deadline:
            raise AssertionError(f"{method} {path}: {r.status_code}: {r.text[:500]}")
        COUNTS[str(r.status_code)] += 1
        if path.startswith("/workflow/trigger/"):
            identifier = path.rsplit("/", 1)[1]
            status = await client.get(f"/workflows/{identifier}")
            if status.status_code == 200 and status.json()["state"] in ("RUNNING", "COMPLETED", "FAILED", "EXPIRED"):
                return {"execution_id": identifier}
        await asyncio.sleep(.025)

async def submit(client, definition, key=None):
    value = await request(client, "POST", "/workflow", 201, json=definition, headers={"Idempotency-Key": key or str(uuid4())})
    return value["execution_id"]

async def trigger(client, identifier):
    return await request(client, "POST", f"/workflow/trigger/{identifier}", 202, json={"input": {"id": 7}})

async def completed(client, identifier, timeout=90):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            r = await client.get(f"/workflows/{identifier}/results")
        except httpx.HTTPError:
            await asyncio.sleep(.05)
            continue
        if r.status_code == 200:
            value = r.json()
            if value["state"] != "COMPLETED":
                raise AssertionError(value)
            return value
        if r.status_code == 409:
            value = r.json()
            if value["state"] in ("FAILED", "EXPIRED"):
                raise AssertionError(value)
        elif r.status_code != 503:
            raise AssertionError(f"results: {r.status_code} {r.text[:500]}")
        await asyncio.sleep(.025)
    raise AssertionError(f"unfinished execution {identifier}")

async def running_branch(client, identifier):
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        r = await client.get(f"/workflows/{identifier}")
        if r.status_code == 200:
            for n in r.json()["nodes"]:
                if n["id"] == "get_user" and n["state"] == "RUNNING" and n["tries"]:
                    return
        await asyncio.sleep(.01)
    raise AssertionError("no active branch")

def audit(kind, identifiers):
    with psycopg.connect(DSNS[kind]) as c:
        rows = c.execute("SELECT state,count(*) FROM executions WHERE id=ANY(%s::uuid[]) GROUP BY state", (identifiers,)).fetchall()
        states = dict(rows)
        assert states == {"COMPLETED": len(identifiers)}, states
        attempts, unique_attempts = c.execute("SELECT count(*),count(DISTINCT (execution_id,node_id,try_number)) FROM tries WHERE execution_id=ANY(%s::uuid[])", (identifiers,)).fetchone()
        assert attempts == unique_attempts
        joins = c.execute("SELECT execution_id,node_id,try_number FROM nodes WHERE execution_id=ANY(%s::uuid[]) AND handler='output'", (identifiers,)).fetchall()
        assert all(n[2] == 1 for n in joins), joins
        return {"completed": len(identifiers), "tries": attempts, "unique_tries": unique_attempts, "output_nodes": len(joins)}

class Stats:
    def __init__(self):
        host = subprocess.check_output(["docker", "context", "inspect", "--format", "{{.Endpoints.docker.Host}}"], text=True).strip()
        if not host.startswith("unix://"):
            raise RuntimeError("This sampler needs a local Docker Unix socket")
        self.client = httpx.Client(transport=httpx.HTTPTransport(uds=host[7:]), base_url="http://docker", timeout=10)
    def sample(self, names):
        cpu = memory = 0
        for name in names:
            r = self.client.get(f"/v1.48/containers/{name}/stats", params={"stream": "false", "one-shot": "true"})
            r.raise_for_status()
            value = r.json()
            cpu += value["cpu_stats"]["cpu_usage"]["total_usage"] / 1e9
            m = value["memory_stats"]
            memory += max(0, m.get("usage", 0) - m.get("stats", {}).get("inactive_file", 0))
        return cpu, memory

async def load_case(kind, name, definition, count, concurrency, stats):
    COUNTS.update({"503": 0, "429": 0})
    names = [container(s) for s in ALL[kind]]
    before, _ = await asyncio.to_thread(stats.sample, names)
    peak = 0
    done = asyncio.Event()
    async def sample():
        nonlocal peak
        while not done.is_set():
            _, memory = await asyncio.to_thread(stats.sample, names)
            peak = max(peak, memory)
            try:
                await asyncio.wait_for(done.wait(), .5)
            except TimeoutError:
                pass
    sampler = asyncio.create_task(sample())
    ids, latencies, receipts = [], [], []
    semaphore = asyncio.Semaphore(concurrency)
    async with httpx.AsyncClient(base_url=URLS[kind], timeout=30, limits=httpx.Limits(max_connections=concurrency * 2)) as client:
        async def one(index):
            async with semaphore:
                started = time.perf_counter()
                identifier = await submit(client, definition)
                receipts.append(time.perf_counter() - started)
                ids.append(identifier)
                await trigger(client, identifier)
                value = await completed(client, identifier)
                assert value["outputs"]
                if name == "linear":
                    assert value["outputs"] == {"c": "mock: ID 7"}, value
                elif name == "parallel":
                    assert set(value["outputs"]["output"]) == {"get_user", "get_posts", "get_comments"}, value
                elif name == "chain50":
                    assert value["outputs"] == {"out":"mock: hello"}
                elif name == "large192KiB":
                    assert value["outputs"] == {"out":"x"*(192*1024)}
                latencies.append(time.perf_counter() - started)
        started = time.perf_counter()
        try:
            await asyncio.gather(*(one(i) for i in range(count)))
            elapsed = time.perf_counter() - started
        finally:
            done.set()
            await sampler
    after, memory = await asyncio.to_thread(stats.sample, names)
    peak = max(peak, memory)
    ordered = sorted(latencies)
    result = {"engine": kind, "case": name, "workflows": count, "concurrency": concurrency, "elapsed_seconds": elapsed, "workflows_per_second": count / elapsed, "p50_ms": statistics.median(latencies) * 1000, "p95_ms": ordered[math.ceil(len(ordered) * .95) - 1] * 1000, "receipt_p95_ms": sorted(receipts)[math.ceil(len(receipts) * .95) - 1] * 1000, "cpu_seconds": after - before, "peak_sampled_mib": peak / 2**20, "overload_responses": dict(COUNTS), "audit": audit(kind, ids)}
    print(json.dumps(result), flush=True)
    return result

async def benchmarks(rounds, extra=False):
    definitions = {name: json.loads((ROOT / "examples" / f"{name}.json").read_text()) for name in ("linear", "parallel")}
    if extra:
        chain = [{"id":"n0","handler":"input","dependencies":[]}]
        chain += [{"id":f"n{i}","handler":"llm","dependencies":[f"n{i-1}"],"config":{"prompt":"hello"}} for i in range(1,49)]
        chain += [{"id":"out","handler":"output","dependencies":["n48"],"data":"{{ n48 }}"}]
        definitions["chain50"] = {"name":"50-node chain","dag":{"nodes":chain}}
        definitions["large192KiB"] = {"name":"large output","dag":{"nodes":[{"id":"out","handler":"output","dependencies":[],"data":"x"*(192*1024)}]}}
    cases = (("chain50", 32, 8), ("large192KiB",64,8)) if extra else (("linear",128,16),("parallel",24,12))
    stats = Stats()
    results = []
    for round_number in range(rounds):
        for kind in (("go", "python") if round_number % 2 == 0 else ("python", "go")):
            await start(kind)
            async with httpx.AsyncClient(base_url=URLS[kind], timeout=30) as client:
                for _ in range(4):
                    identifier = await submit(client, definitions["linear"])
                    await trigger(client, identifier)
                    await completed(client, identifier)
            for name, count, concurrency in cases:
                result = await load_case(kind, name, definitions[name], count, concurrency, stats)
                result["round"] = round_number + 1
                results.append(result)
    output = {"method": {"rounds": rounds, "total_cpu_limit": 4, "total_memory_mib": 2048, "postgres_cpu_limit": 1.5, "postgres_memory_mib": 768, "handler_concurrency": 4, "handler_timeout_seconds": 10, "execution_timeout_seconds": 300, "rate_limit": 10000, "poll_seconds": .025, "durability": "PostgreSQL synchronous_commit=on, Redis appendfsync=always", "scope": "full deployment, including broker cost", "overload_policy": "retry 429/503 after 25ms with the same submission key; receipt and workflow latency include retries", "memory": "largest sampled sum of container usage minus inactive file cache", "cpu": "sum of Docker cumulative CPU counters over each case"}, "runs": results}
    output["method"]["cases"] = [{"name":name,"count":count,"concurrency":concurrency} for name,count,concurrency in cases]
    destination = "comparison_extra.json" if extra else "comparison.json"
    (ROOT / "results" / destination).write_text(json.dumps(output, indent=2) + "\n")
    stats.client.close()
    return output

async def faults():
    parallel = json.loads((ROOT / "examples/parallel.json").read_text())
    results = []
    for kind in ("go", "python"):
        await start(kind)
        async with httpx.AsyncClient(base_url=URLS[kind], timeout=10) as client:
            # Save a receipt, then stop the process before trigger acceptance.
            key = str(uuid4())
            identifier = await submit(client, parallel, key)
            service = "go" if kind == "go" else "api"
            began = time.monotonic()
            await asyncio.to_thread(compose, "kill", "--signal", "SIGKILL", service)
            await asyncio.to_thread(compose, "up", "--detach", service)
            await ready(kind)
            repeated = await submit(client, parallel, key)
            assert repeated == identifier
            await trigger(client, identifier)
            await completed(client, identifier)
            results.append({"engine": kind, "case": "api_sigkill_preserves_receipt", "pass": True, "recovery_seconds": time.monotonic() - began, "audit": audit(kind, [identifier])})
            for case, service in (("control_sigkill_during_handler", "go" if kind == "go" else "orchestrator"), ("handler_process_sigkill", "go" if kind == "go" else "worker"), ("postgres_sigkill_during_handler", "pg-go" if kind == "go" else "pg-python")):
                identifier = await submit(client, parallel)
                await trigger(client, identifier)
                await running_branch(client, identifier)
                began = time.monotonic()
                await asyncio.to_thread(compose, "kill", "--signal", "SIGKILL", service)
                await asyncio.to_thread(compose, "up", "--detach", service)
                await ready(kind)
                value = await completed(client, identifier, timeout=75)
                assert len(value["outputs"]["output"]) == 3
                results.append({"engine": kind, "case": case, "pass": True, "recovery_seconds": time.monotonic() - began, "audit": audit(kind, [identifier])})
            # A database outage must not create an undurable successful receipt.
            service = "pg-go" if kind == "go" else "pg-python"
            await asyncio.to_thread(compose, "stop", service)
            began = time.monotonic()
            try:
                r = await client.post("/workflow", json=parallel)
                assert r.status_code >= 500, (r.status_code, r.text)
                response = str(r.status_code)
            except httpx.HTTPError:
                response = "connection unavailable"
            await asyncio.to_thread(compose, "up", "--detach", service)
            await ready(kind)
            identifier = await submit(client, parallel)
            await trigger(client, identifier)
            await completed(client, identifier)
            results.append({"engine": kind, "case": "database_outage_rejects_and_recovers", "pass": True, "response": response, "recovery_seconds": time.monotonic() - began, "audit": audit(kind, [identifier])})
        print(json.dumps(results[-5:]), flush=True)
    (ROOT / "results/reliability.json").write_text(json.dumps({"cases": results}, indent=2) + "\n")
    return results

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("bench", "extra", "faults", "all"))
    parser.add_argument("--rounds", type=int, default=3)
    args = parser.parse_args()
    compose("up", "--detach", "--wait", "pg-go", "pg-python", "redis")
    if args.mode in ("faults", "all"):
        await faults()
    if args.mode in ("bench", "all"):
        await benchmarks(args.rounds)
    if args.mode in ("extra", "all"):
        await benchmarks(args.rounds, extra=True)

if __name__ == "__main__":
    asyncio.run(main())
