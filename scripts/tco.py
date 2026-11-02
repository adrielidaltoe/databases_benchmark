"""TCO model: 3-year cost of staying on Postgres against migrating to ClickHouse, both
self-managed on AWS at production scale (SF 200). See README section 10.

Inputs are in tco/assumptions.toml: AWS and labour prices with their sources, and the
benchmark figures the sizing is based on. Per option:
  infrastructure  primary + HA replica (EC2 on-demand), gp3 volumes, snapshots
  operations      DBA time at a loaded market rate (salary / wage share of compensation)
  migration       ClickHouse only, one-off: porting and validating reports, the extra work
                  on the reports that needed a fix in the benchmark, the correction pipeline
                  redesign, and a parallel run with Postgres still in production
Sizing: Postgres keeps its node (the status quo meets its window by definition).
ClickHouse gets the smallest node whose memory holds the concurrent reports' peak, scaled
from SF 10, since memory, not CPU, was what made it fail (section 9).

Prints the base case, the sensitivity table, and the yearly licence the current
warehouse would need to cost for the vendor's 60% saving to hold.

    python scripts/tco.py
    python scripts/tco.py --assumptions tco/assumptions.toml
"""
import argparse
import copy
import tomllib

from common import ROOT

CLAIMED_SAVING = 0.60


def loaded(salary: float, a: dict) -> float:
    return salary / a["labour"]["wage_share"]


def instance(a: dict, name: str) -> dict:
    vcpu, mem, od, ri, mbps = a["aws"]["instances"][name]
    return {"name": name, "vcpu": vcpu, "mem": mem, "price": od, "reserved": ri, "mbps": mbps}


def ch_memory_gb(a: dict) -> float:
    b, s = a["benchmark"], a["sizing"]
    scale = a["production"]["scale_factor"] / a["production"]["benchmark_sf"]
    per_query = b["ch_query_peak_gb"] * (1 + (scale - 1) * s["ch_query_memory_growth"])
    need = a["production"]["streams"] * per_query + b["ch_server_overhead_gb"] * scale
    return need / s["ch_ram_ratio"]


def ch_instance(a: dict) -> dict:
    need = ch_memory_gb(a)
    fits = [instance(a, n) for n in a["aws"]["instances"]]
    fits = [i for i in fits if i["mem"] >= need and i["vcpu"] >= a["sizing"]["ch_min_vcpu"]]
    if not fits:
        raise SystemExit(f"no instance in the list has {need:.0f} GB of memory")
    return min(fits, key=lambda i: i["price"])


def storage_year(a: dict, data_gb: float, iops: int, mbps: int) -> float:
    """gp3 volume per node plus snapshots of the data, $ per year."""
    e, s = a["aws"]["ebs"], a["sizing"]
    volume = (data_gb * s["storage_headroom"] * e["gp3_gb_month"]
              + max(0, iops - 3000) * e["gp3_iops_month"]
              + max(0, mbps - 125) * e["gp3_mbps_month"])
    snapshots = data_gb * e["snapshot_retention_factor"] * e["snapshot_gb_month"]
    return 12 * (volume * s["nodes"] + snapshots)


def option(a: dict, engine: str, reserved: bool = False) -> dict:
    b, s, lab = a["benchmark"], a["sizing"], a["labour"]
    scale = a["production"]["scale_factor"] / a["production"]["benchmark_sf"]
    if engine == "postgres":
        node = instance(a, s["pg_instance"])
        data_gb = b["pg_disk_gb"] * scale * (1 + b["pg_write_growth"])
        storage = storage_year(a, data_gb, s["pg_iops"], s["pg_mbps"])
        ops = lab["ops_fte_pg"] * loaded(lab["dba_salary"], a)
    else:
        node = ch_instance(a)
        data_gb = b["ch_disk_gb"] * scale
        storage = storage_year(a, data_gb, s["ch_iops"], s["ch_mbps"])
        ops = lab["ops_fte_ch"] * loaded(lab["dba_salary"], a) * (1 + lab["clickhouse_premium"])
    compute = s["nodes"] * node["reserved" if reserved else "price"] * a["hours_per_year"]
    return {"engine": engine, "node": node, "data_gb": data_gb, "compute": compute,
            "storage": storage, "ops": ops, "infra": compute + storage,
            "yearly": compute + storage + ops}


def migration(a: dict, pg: dict) -> dict:
    m, b, lab = a["migration"], a["benchmark"], a["labour"]
    fix_share = b["reports_needing_fix"] / b["reports_total"]
    report_days = m["reports"] * (m["days_per_report"] + fix_share * m["days_per_fix"])
    pipeline_days = m["pipeline_weeks"] * 5
    day_rate = loaded(lab["architect_salary"], a) / lab["working_days_per_year"]
    engineering = (report_days + pipeline_days) * day_rate
    # Postgres stays in production during the parallel run, with someone reconciling results
    months = m["parallel_run_months"] / 12
    parallel = months * (pg["yearly"] + m["parallel_run_fte"] * loaded(lab["dba_salary"], a))
    return {"report_days": report_days, "pipeline_days": pipeline_days,
            "engineering": engineering, "parallel": parallel, "total": engineering + parallel}


def tco(a: dict, reserved: bool = False) -> dict:
    years = a["horizon_years"]
    pg, ch = option(a, "postgres", reserved), option(a, "clickhouse", reserved)
    mig = migration(a, pg)
    pg_total = pg["yearly"] * years
    ch_total = ch["yearly"] * years + mig["total"]
    return {"pg": pg, "ch": ch, "mig": mig, "pg_total": pg_total, "ch_total": ch_total,
            "change": ch_total / pg_total - 1,
            "infra_change": ch["infra"] / pg["infra"] - 1,
            # yearly licence of the current warehouse that would make ClickHouse
            # break even, or reach the claimed saving, over the horizon
            "licence_even": (ch_total - pg_total) / years,
            "licence_claim": (ch_total / (1 - CLAIMED_SAVING) - pg_total) / years,
            "ch_memory": ch_memory_gb(a)}


def k(x: float) -> str:
    return f"${x / 1000:,.0f}k"


def print_base(a: dict, r: dict) -> None:
    pg, ch, mig, years = r["pg"], r["ch"], r["mig"], a["horizon_years"]
    print(f"ClickHouse memory needed: {r['ch_memory']:.0f} GB "
          f"({a['production']['streams']} concurrent reports)\n")
    print("| | Postgres | ClickHouse |\n|---|--:|--:|")
    rows = [
        ("Node (x2: primary + replica)",
         *(f"{o['node']['name']} ({o['node']['vcpu']} vCPU, {o['node']['mem']} GB)"
           for o in (pg, ch))),
        ("Data on disk", f"{pg['data_gb']:.0f} GB", f"{ch['data_gb']:.0f} GB"),
        ("Compute per year", k(pg["compute"]), k(ch["compute"])),
        ("Storage + snapshots per year", k(pg["storage"]), k(ch["storage"])),
        ("Operations per year", k(pg["ops"]), k(ch["ops"])),
        ("**Running cost per year**", f"**{k(pg['yearly'])}**", f"**{k(ch['yearly'])}**"),
        (f"Running cost, {years} years", k(pg["yearly"] * years), k(ch["yearly"] * years)),
        ("Migration: reports + pipeline", "—",
         f"{k(mig['engineering'])} ({mig['report_days'] + mig['pipeline_days']:.0f} days)"),
        ("Migration: parallel run", "—", k(mig["parallel"])),
        (f"**{years}-year TCO**", f"**{k(r['pg_total'])}**", f"**{k(r['ch_total'])}**"),
    ]
    for row in rows:
        print("| " + " | ".join(row) + " |")
    print(f"\nClickHouse vs Postgres: {r['change']:+.0%} over {years} years, "
          f"infrastructure {r['infra_change']:+.0%} per year")
    print(f"Licence the current warehouse would need to cost per year: "
          f"{k(r['licence_even'])} to break even, {k(r['licence_claim'])} for a 60% saving")


def variants(a: dict):
    """(label, assumptions, reserved) for each sensitivity row: one change at a time."""
    def with_(path: str, value):
        v = copy.deepcopy(a)
        *keys, last = path.split(".")
        d = v
        for key in keys:
            d = d[key]
        d[last] = value
        return v
    yield "Base case", a, False
    for s in (1, 2, 8):
        yield f"{s} concurrent report{'s' if s > 1 else ''} (base {a['production']['streams']})", \
            with_("production.streams", s), False
    yield "Per-query memory grows 25% as fast as the data (more spilling, untested)", \
        with_("sizing.ch_query_memory_growth", 0.25), False
    yield "3-year reserved instances, both engines", a, True
    yield "Postgres needs r7i.16xlarge for I/O", with_("sizing.pg_instance", "r7i.16xlarge"), False
    for n in (50, 200):
        yield f"{n} reports to migrate (base {a['migration']['reports']})", \
            with_("migration.reports", n), False
    yield "ClickHouse operators cost 25% more (scarcity)", \
        with_("labour.clickhouse_premium", 0.25), False
    yield "ClickHouse needs 0.75 FTE to operate (base 0.5)", with_("labour.ops_fte_ch", 0.75), False


def print_sensitivity(a: dict) -> None:
    years = a["horizon_years"]
    print(f"\n| Case | ClickHouse node | Postgres, {years} y | ClickHouse, {years} y | "
          f"ClickHouse vs Postgres | Infra per year | Licence for a 60% saving |")
    print("|---|---|--:|--:|--:|--:|--:|")
    for label, v, reserved in variants(a):
        r = tco(v, reserved)
        print(f"| {label} | {r['ch']['node']['name']} | {k(r['pg_total'])} | "
              f"{k(r['ch_total'])} | {r['change']:+.0%} | {r['infra_change']:+.0%} | "
              f"{k(r['licence_claim'])}/y |")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--assumptions", default=str(ROOT / "tco" / "assumptions.toml"))
    args = ap.parse_args()
    with open(args.assumptions, "rb") as f:
        a = tomllib.load(f)
    print_base(a, tco(a))
    print_sensitivity(a)


if __name__ == "__main__":
    main()
