"""Monitoring checks for silent failures, data anomalies and BigQuery cost.

Each check takes plain query results and returns Findings, so the logic is unit-testable
without BigQuery. Thresholds live here so they are easy to explain and tune.
"""
from dataclasses import dataclass
from datetime import datetime, timedelta

STALE_AFTER_HOURS = 26  # daily DAG + 2h slack
VOLUME_LOW_RATIO = 0.5  # latest batch < 50% of recent average
VOLUME_HIGH_RATIO = 2.0  # latest batch > 200% of recent average
VOLUME_MIN_HISTORY = 3  # batches needed before judging volume
KPI_MAX_CHANGE = 0.5  # CPL / EPC moved more than 50% vs the trailing average
KPI_MIN_LEADS = 20  # too few leads makes CPL noisy
KPI_MIN_HISTORY = 3
USD_PER_TIB = 6.25  # BigQuery on-demand price; ignores the free tier, so the estimate is conservative
COST_WARN_USD = 0.50
COST_CRITICAL_USD = 1.00

CRITICAL = "CRITICAL"
WARNING = "WARNING"


@dataclass(frozen=True)
class Finding:
    severity: str
    key: str  # stable id used to throttle repeats
    message: str


def check_freshness(latest_ingested_by_table: dict, now: datetime) -> list[Finding]:
    """Silent stop: a bronze table received nothing recently (DAG not running, or every load failing)."""
    findings = []
    for table, latest in sorted(latest_ingested_by_table.items()):
        if latest is None:
            findings.append(Finding(CRITICAL, f"stale:{table}", f"{table} has never received data."))
        elif now - latest > timedelta(hours=STALE_AFTER_HOURS):
            hours = (now - latest).total_seconds() / 3600
            findings.append(Finding(CRITICAL, f"stale:{table}",
                                    f"{table}: no new data for {hours:.0f}h (last load {latest:%Y-%m-%d %H:%M} UTC)."))
    return findings


def check_silver_lag(watermarks: dict, silver_models: dict) -> list[Finding]:
    """Silent partial run: bronze has a newer batch than silver, so dbt did not process it.

    watermarks: {bronze_table: (bronze_max_ingested_at, silver_max_ingested_at)}
    silver_models: {bronze_table: silver model name}
    """
    findings = []
    for table, (bronze_max, silver_max) in sorted(watermarks.items()):
        if bronze_max is not None and (silver_max is None or silver_max < bronze_max):
            silver = f"silver.{silver_models[table]}"
            findings.append(Finding(
                CRITICAL, f"silver_behind:{table}",
                f"{silver} is behind bronze.{table} (bronze latest {bronze_max:%Y-%m-%d %H:%M}, "
                f"silver latest {silver_max:%Y-%m-%d %H:%M} UTC) - dbt_build did not run or failed."
                if silver_max else f"{silver} is empty while bronze.{table} has data.",
            ))
    return findings


def check_rejects(rejects_by_table: dict) -> list[Finding]:
    """Quarantined lines in the last 24h. rejects_by_table: {table: (count, [sample errors])}"""
    return [
        Finding(WARNING, f"rejects:{table}",
                f"{table}: {count} line(s) quarantined in the last 24h. Examples: " + "; ".join(samples[:3]))
        for table, (count, samples) in sorted(rejects_by_table.items())
        if count > 0
    ]


def check_volume(batch_sizes_by_table: dict) -> list[Finding]:
    """Latest batch unusually small or large. batch_sizes_by_table: {table: [row counts, newest first]}"""
    findings = []
    for table, sizes in sorted(batch_sizes_by_table.items()):
        latest, history = (sizes[0], sizes[1:]) if sizes else (None, [])
        if latest is None or len(history) < VOLUME_MIN_HISTORY:
            continue
        average = sum(history) / len(history)
        ratio = latest / average if average else float("inf")
        if ratio < VOLUME_LOW_RATIO or ratio > VOLUME_HIGH_RATIO:
            direction = "low" if ratio < VOLUME_LOW_RATIO else "high"
            findings.append(Finding(WARNING, f"volume_{direction}:{table}",
                                    f"{table}: latest batch has {latest} rows, {ratio:.0%} of the recent "
                                    f"average ({average:.0f}) - unusually {direction}."))
    return findings


def check_kpis(days: list[dict]) -> list[Finding]:
    """CPL / EPC spike or drop. days: [{report_date, cpl, epc, leads}], newest complete day first."""
    if len(days) < 1 + KPI_MIN_HISTORY or (days[0]["leads"] or 0) < KPI_MIN_LEADS:
        return []
    latest, history = days[0], days[1:]
    findings = []
    for metric in ("cpl", "epc"):
        values = [d[metric] for d in history if d[metric] is not None]
        if latest[metric] is None or len(values) < KPI_MIN_HISTORY:
            continue
        average = sum(values) / len(values)
        if average and abs(latest[metric] - average) / average > KPI_MAX_CHANGE:
            findings.append(Finding(WARNING, f"kpi:{metric}:{latest['report_date']}",
                                    f"{metric.upper()} on {latest['report_date']} is {latest[metric]:.2f} vs a "
                                    f"{len(values)}-day average of {average:.2f} ({(latest[metric] / average - 1):+.0%})."))
    return findings


def estimate_cost_usd(bytes_billed: int) -> float:
    return (bytes_billed or 0) / 2**40 * USD_PER_TIB


def check_cost(bytes_billed_month_to_date: int) -> list[Finding]:
    cost = estimate_cost_usd(bytes_billed_month_to_date)
    if cost >= COST_CRITICAL_USD:
        return [Finding(CRITICAL, "cost:critical",
                        f"BigQuery month-to-date query cost is about ${cost:.2f} (limit ${COST_CRITICAL_USD:.2f}).")]
    if cost >= COST_WARN_USD:
        return [Finding(WARNING, "cost:warning",
                        f"BigQuery month-to-date query cost is about ${cost:.2f} "
                        f"(warning at ${COST_WARN_USD:.2f}, limit ${COST_CRITICAL_USD:.2f}).")]
    return []


def run_check(name: str, check) -> list[Finding]:
    """Run one check; if it crashes (e.g. missing permission), report that instead of hiding the other checks."""
    try:
        return check()
    except Exception as e:  # noqa: BLE001 - any failure must become a visible finding
        return [Finding(CRITICAL, f"check_error:{name}", f"monitor check '{name}' could not run: {str(e)[:300]}")]


def format_findings(findings: list[Finding]) -> tuple[str, str, str]:
    """One email for all findings of a monitor run: (severity, subject, body)."""
    severity = CRITICAL if any(f.severity == CRITICAL for f in findings) else WARNING
    counts = {s: sum(f.severity == s for f in findings) for s in (CRITICAL, WARNING)}
    subject = f"monitor found {counts[CRITICAL]} critical, {counts[WARNING]} warning"
    ordered = sorted(findings, key=lambda f: (f.severity != CRITICAL, f.key))
    body = "\n".join(f"[{f.severity}] {f.message}" for f in ordered)
    return severity, subject, body
