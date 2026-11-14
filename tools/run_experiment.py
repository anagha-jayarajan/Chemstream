import argparse, json, os, shutil, signal, subprocess, sys, threading, time
from datetime import datetime, timezone
from confluent_kafka.admin import AdminClient, NewTopic
from deltalake import DeltaTable

ROOT = "/workspace"
KAFKA = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")

def iso(t=None):
    return datetime.fromtimestamp(t or time.time(), timezone.utc).isoformat(timespec="seconds")

def ev(events, text):
    events.append({"t": iso(), "event": text})
    print(iso(), text, flush=True)

def silver_rows(exp):
    try:
        return DeltaTable(f"{ROOT}/exp/{exp}/lake/silver").to_pyarrow_table(columns=["record_id"]).num_rows
    except Exception:
        return 0

def ledger_rows(topic):
    path = f"{ROOT}/ledger/{topic}_sent.csv"
    if not os.path.exists(path):
        return 0
    with open(path) as f:
        return sum(1 for _ in f)

class Job:
    def __init__(self, module, env, logfile):
        self.module, self.env, self.logfile = module, env, logfile
        self.proc, self.expected_down, self.started_at = None, False, 0
    def start(self):
        self.started_at = time.time()
        self.proc = subprocess.Popen([sys.executable, "-m", self.module], cwd=ROOT, env=self.env,
                                     stdout=open(self.logfile, "a"), stderr=subprocess.STDOUT,
                                     start_new_session=True)
    def alive(self):
        return self.proc is not None and self.proc.poll() is None
    def kill(self):
        if self.alive():
            os.killpg(self.proc.pid, signal.SIGKILL)
            self.proc.wait()

def supervise(jobs, events):
    """Restart a job that died on its own (like a production supervisor), at most every 15 s."""
    for name, j in jobs.items():
        if not j.alive() and not j.expected_down and time.time() - j.started_at > 15:
            ev(events, f"{name} crashed, auto-restarting")
            j.start()

def fault_worker(args, jobs, events, topic, shared):
    time.sleep(args.fault_at)
    if args.fault.startswith("kill"):
        names = {"kill_silver": ["silver"], "kill_bronze": ["bronze"],
                 "kill_both": ["bronze", "silver"]}[args.fault]
        for n in names:
            jobs[n].expected_down = True
            jobs[n].kill()
        ev(events, "KILLED " + ", ".join(names))
        time.sleep(args.downtime)
        for n in names:
            jobs[n].start()
            jobs[n].expected_down = False
        shared["t_restart"] = time.time()
        ev(events, "restarted " + ", ".join(names))
    elif args.fault == "bad_and_dup":
        subprocess.run([sys.executable, "-m", "tools.inject_faults", topic, "50", "50"], cwd=ROOT)
        ev(events, "injected 50 corrupt + 50 duplicate messages")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--exp", required=True)
    ap.add_argument("--cores", default="2")
    ap.add_argument("--rate", type=int, default=300)
    ap.add_argument("--seconds", type=int, default=30)
    ap.add_argument("--partitions", type=int, default=8)
    ap.add_argument("--warmup", type=int, default=45)
    ap.add_argument("--timeout", type=int, default=600)
    ap.add_argument("--trigger", type=int, default=0, help="seconds for both triggers; 0 = defaults (5 and 10)")
    ap.add_argument("--fault", default="none",
                    choices=["none", "kill_silver", "kill_bronze", "kill_both", "bad_and_dup"])
    ap.add_argument("--fault_at", type=int, default=15)
    ap.add_argument("--downtime", type=int, default=20)
    args = ap.parse_args()

    exp, topic = args.exp, f"chem.{args.exp}.raw"
    expdir = f"{ROOT}/exp/{exp}"
    admin = AdminClient({"bootstrap.servers": KAFKA})
    if topic in admin.list_topics(timeout=15).topics:
        sys.exit(f"Topic {topic} already exists. Use a new --exp name.")
    shutil.rmtree(expdir, ignore_errors=True)
    for suffix in ("_sent.csv", "_summary.json"):
        p = f"{ROOT}/ledger/{topic}{suffix}"
        if os.path.exists(p):
            os.remove(p)
    os.makedirs(expdir)
    admin.create_topics([NewTopic(topic, num_partitions=args.partitions, replication_factor=1)])[topic].result()

    env = {**os.environ, "EXP": exp, "CORES": str(args.cores)}
    if args.trigger:
        env["BRONZE_TRIGGER"] = env["SILVER_TRIGGER"] = f"{args.trigger} seconds"
    jobs = {"bronze": Job("spark_jobs.bronze", env, f"{expdir}/bronze.log"),
            "silver": Job("spark_jobs.silver", env, f"{expdir}/silver.log")}
    events, shared = [], {}

    jobs["bronze"].start(); time.sleep(8); jobs["silver"].start()   # stagger to avoid package-cache clashes
    ev(events, f"jobs started, warm-up {args.warmup}s")
    time.sleep(args.warmup)

    total = args.rate * args.seconds
    prod = subprocess.Popen([sys.executable, "-m", "producers.replay_producer",
                             topic, str(args.rate), str(total)], cwd=ROOT)
    ev(events, f"LOAD START: {args.rate}/s for {args.seconds}s = {total} messages")
    if args.fault != "none":
        threading.Thread(target=fault_worker, args=(args, jobs, events, topic, shared), daemon=True).start()
    while prod.poll() is None:
        supervise(jobs, events)
        time.sleep(1)
    t_prod_end = time.time()
    ev(events, "load finished, waiting for the pipeline to catch up")

    drained = False
    while time.time() - t_prod_end < args.timeout:
        supervise(jobs, events)
        n = ledger_rows(topic)
        if n > 0 and silver_rows(exp) >= n:
            drained = True
            break
        time.sleep(2)
    t_drained = time.time()
    ev(events, "CAUGHT UP" if drained else "TIMEOUT: not caught up")
    time.sleep(20)                                   # let the last batches and quarantine writes finish
    for j in jobs.values():
        j.expected_down = True
        j.kill()

    meta = {"exp": exp, "cores": args.cores, "rate": args.rate, "seconds": args.seconds,
            "trigger": f"{args.trigger}s" if args.trigger else "default(5s/10s)",
            "fault": args.fault, "drained": drained, "drain_seconds": t_drained - t_prod_end,
            "recovery_s": round(t_drained - shared["t_restart"], 1) if shared.get("t_restart") else None,
            "events": events}
    json.dump(meta, open(f"{expdir}/meta.json", "w"), indent=2)
    subprocess.run([sys.executable, "-m", "tools.measure"], cwd=ROOT,
                   env={**os.environ, "EXP": exp, "CORES": "2"})

if __name__ == "__main__":
    main()