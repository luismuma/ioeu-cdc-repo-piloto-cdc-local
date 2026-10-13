#!/usr/bin/env python3
"""
test_local.py — Local render test for ogg_report.py.

Builds realistic fake API results (no OGG servers needed) and runs the full
data-processing + Jinja2 pipeline, writing a sample HTML you can open in a
browser to verify the report looks correct.

Usage:
    python3 test_local.py [--out sample_report.html] [--open]
"""

import argparse
import subprocess
import sys
from pathlib import Path

# Import the functions we want to test from ogg_report
sys.path.insert(0, str(Path(__file__).parent))
from ogg_report import build_report_data, render_html
import json

SCRIPT_DIR = Path(__file__).parent


# ──────────────────────────────────────────────────────────────────────────────
# FAKE RESULTS — one dict per deployment, same shape as collect_deployment()
# ──────────────────────────────────────────────────────────────────────────────

FAKE_RESULTS = [

    # ── PRE Extractor (Oracle CDC) — healthy ──────────────────────────────────
    {
        "dep_name":   "OGG_CDC_ODB_ORA_E01_AWS_PRE",
        "host":       "ogg-cdc-odb-ora-e01-aws-pre.es.mapfre.net",
        "grupo":      "Online",
        "env":        "PRE",
        "role_base":  "Extractor",
        "unreachable": False,
        "overall":    "OK",
        "s3_bucket":  "s3://mapfre-ogg-pre/trails",
        "errors_24h": 0,
        "services": [
            {"name": "adminsrvr",  "estado": "RUNNING"},
            {"name": "distsrvr",   "estado": "RUNNING"},
            {"name": "pmsrvr",     "estado": "RUNNING"},
            {"name": "recvsrvr",   "estado": "RUNNING"},
            {"name": "pluginsrvr", "estado": "RUNNING"},
        ],
        "processes": [
            {"name": "ECDC01", "type": "EXTRACT", "status": "RUNNING",
             "lag": "00:00:04", "last_started": "2026-09-14T08:00:00Z", "desc": "CDC Oracle → Kafka"},
            {"name": "ECDC02", "type": "EXTRACT", "status": "RUNNING",
             "lag": "00:00:07", "last_started": "2026-09-14T08:01:00Z", "desc": "CDC Oracle → Kafka (schema 2)"},
            {"name": "DP_PRE_01", "type": "DIST_PATH", "status": "RUNNING",
             "lag": "", "last_started": "", "desc": ""},
        ],
        "stats": [
            {"tabla": "SCHEMA1.CLIENTES",   "bbdd": "ORACLE", "proceso": "ECDC01",
             "proc_type": "EXTRACT", "proc_status": "RUNNING",
             "since": "2026-09-13T08:00:00Z", "elapsed": "1d 02:00:00",
             "inserts": 1520, "updates": 830, "deletes": 45, "upserts": 0, "discards": 0},
            {"tabla": "SCHEMA1.POLIZAS",    "bbdd": "ORACLE", "proceso": "ECDC01",
             "proc_type": "EXTRACT", "proc_status": "RUNNING",
             "since": "2026-09-13T10:00:00Z", "elapsed": "1d 00:00:00",
             "inserts": 320, "updates": 150, "deletes": 8, "upserts": 0, "discards": 0},
            {"tabla": "SCHEMA1.SINIESTROS", "bbdd": "ORACLE", "proceso": "ECDC01",
             "proc_type": "EXTRACT", "proc_status": "RUNNING",
             "since": "2026-09-14T06:00:00Z", "elapsed": "02:00:00",
             "inserts": 0, "updates": 12, "deletes": 0, "upserts": 0, "discards": 0},
            {"tabla": "SCHEMA2.PRODUCTOS",  "bbdd": "ORACLE", "proceso": "ECDC02",
             "proc_type": "EXTRACT", "proc_status": "RUNNING",
             "since": "2026-09-14T07:00:00Z", "elapsed": "01:00:00",
             "inserts": 88, "updates": 34, "deletes": 3, "upserts": 0, "discards": 2},
        ],
    },

    # ── PRE Replicator (Oracle CDC) — one service down ────────────────────────
    {
        "dep_name":   "OGG_CDC_ODB_DAA_R01_AWS_PRE",
        "host":       "ogg-cdc-odb-daa-r01-aws-pre.es.mapfre.net",
        "grupo":      "Online",
        "env":        "PRE",
        "role_base":  "Replicator",
        "unreachable": False,
        "overall":    "WARNING",
        "s3_bucket":  "",
        "errors_24h": 3,
        "services": [
            {"name": "adminsrvr",  "estado": "RUNNING"},
            {"name": "distsrvr",   "estado": "STOPPED"},
            {"name": "pmsrvr",     "estado": "RUNNING"},
            {"name": "recvsrvr",   "estado": "RUNNING"},
            {"name": "pluginsrvr", "estado": "RUNNING"},
        ],
        "processes": [
            {"name": "RCDC01", "type": "REPLICAT", "status": "RUNNING",
             "lag": "00:01:15", "last_started": "2026-09-14T08:05:00Z", "desc": "Apply Oracle CDC"},
            {"name": "RCDC02", "type": "REPLICAT", "status": "ABENDED",
             "lag": "",         "last_started": "2026-09-13T22:00:00Z", "desc": "Apply Oracle CDC (schema 2)"},
            {"name": "RP_PRE_01", "type": "RCVR_PATH", "status": "RUNNING",
             "lag": "", "last_started": "", "desc": ""},
        ],
        "stats": [
            {"tabla": "DAA.CLIENTES", "bbdd": "ORACLE", "proceso": "RCDC01",
             "proc_type": "REPLICAT", "proc_status": "RUNNING",
             "since": "2026-09-13T08:00:00Z", "elapsed": "1d 02:00:00",
             "inserts": 1515, "updates": 828, "deletes": 45, "upserts": 0, "discards": 0,
             "team": "TeamDataOps", "cost_center": "CC001",
             "size_mb": 2048, "num_rows": 4200000},
            {"tabla": "DAA.POLIZAS",  "bbdd": "ORACLE", "proceso": "RCDC01",
             "proc_type": "REPLICAT", "proc_status": "RUNNING",
             "since": "2026-09-13T10:00:00Z", "elapsed": "1d 00:00:00",
             "inserts": 318, "updates": 149, "deletes": 8, "upserts": 0, "discards": 0,
             "team": "TeamRisk", "cost_center": "CC002",
             "size_mb": 512, "num_rows": 980000},
            {"tabla": "DAA.SINIESTROS", "bbdd": "ORACLE", "proceso": "RCDC02",
             "proc_type": "REPLICAT", "proc_status": "ABENDED",
             "since": "2026-09-13T22:00:00Z", "elapsed": "10:00:00",
             "inserts": 0, "updates": 0, "deletes": 0, "upserts": 0, "discards": 0,
             "size_mb": 760, "num_rows": 1500000},
        ],
    },

    # ── PRE Initial Load Extractor ────────────────────────────────────────────
    {
        "dep_name":   "OGG_INI_ODB_ORA_E01_AWS_PRE",
        "host":       "ogg-ini-odb-ora-e01-aws-pre.es.mapfre.net",
        "grupo":      "InitialLoad",
        "env":        "PRE",
        "role_base":  "Extractor",
        "unreachable": False,
        "overall":    "OK",
        "s3_bucket":  "",
        "errors_24h": 0,
        "services": [
            {"name": "adminsrvr", "estado": "RUNNING"},
            {"name": "pmsrvr",    "estado": "RUNNING"},
            {"name": "distsrvr",  "estado": "RUNNING"},
        ],
        "processes": [
            {"name": "EINIT01", "type": "EXTRACT", "status": "RUNNING",
             "lag": "00:00:00", "last_started": "2026-09-14T06:00:00Z",
             "desc": "Extract-Initial_load-ACNPDB-00-DATAIA-PRE for table SCHEMA1.HISTORICO part 1/3 (created by cdc-controller date: 2026-09-14 06:00:00)"},
            {"name": "EINIT02", "type": "EXTRACT", "status": "RUNNING",
             "lag": "00:00:00", "last_started": "2026-09-14T06:05:00Z",
             "desc": "Extract-Initial_load-ACNPDB-00-DATAIA-PRE for table SCHEMA1.HISTORICO part 2/3 (created by cdc-controller date: 2026-09-14 06:05:00)"},
            {"name": "EINIT03", "type": "EXTRACT", "status": "STOPPED",
             "lag": "", "last_started": "2026-09-14T06:10:00Z",
             "desc": "Extract-Initial_load-ACNPDB-00-DATAIA-PRE for table SCHEMA1.HISTORICO part 3/3 (created by cdc-controller date: 2026-09-14 06:10:00)"},
        ],
        "stats": [
            {"tabla": "SCHEMA1.HISTORICO", "bbdd": "ORACLE", "proceso": "EINIT01",
             "proc_type": "EXTRACT", "proc_status": "RUNNING",
             "proc_desc": "Extract-Initial_load-ACNPDB-00-DATAIA-PRE for table SCHEMA1.HISTORICO part 1/3 (created by cdc-controller date: 2026-09-14 06:00:00)",
             "proc_last_started": "2026-09-14T06:00:00Z",
             "inserts": 0, "updates": 0, "deletes": 0, "upserts": 0, "discards": 0,
             "tot_ins": 4500000, "tot_upd": 0, "tot_del": 0,
             "num_rows": 12000000, "size_mb": 9600},
            {"tabla": "SCHEMA1.HISTORICO", "bbdd": "ORACLE", "proceso": "EINIT02",
             "proc_type": "EXTRACT", "proc_status": "RUNNING",
             "proc_desc": "Extract-Initial_load-ACNPDB-00-DATAIA-PRE for table SCHEMA1.HISTORICO part 2/3 (created by cdc-controller date: 2026-09-14 06:05:00)",
             "proc_last_started": "2026-09-14T06:05:00Z",
             "inserts": 0, "updates": 0, "deletes": 0, "upserts": 0, "discards": 0,
             "tot_ins": 3200000, "tot_upd": 0, "tot_del": 0,
             "num_rows": 12000000, "size_mb": 9600},
            {"tabla": "SCHEMA1.HISTORICO", "bbdd": "ORACLE", "proceso": "EINIT03",
             "proc_type": "EXTRACT", "proc_status": "STOPPED",
             "proc_desc": "Extract-Initial_load-ACNPDB-00-DATAIA-PRE for table SCHEMA1.HISTORICO part 3/3 (created by cdc-controller date: 2026-09-14 06:10:00)",
             "proc_last_started": "2026-09-14T06:10:00Z",
             "inserts": 0, "updates": 0, "deletes": 0, "upserts": 0, "discards": 0,
             "tot_ins": 12000000, "tot_upd": 0, "tot_del": 0,
             "num_rows": 12000000, "size_mb": 9600},
        ],
    },

    # ── PRE Initial Load Replicator — part 3 done, parts 1 and 2 pending ─────
    {
        "dep_name":   "OGG_INI_ODB_DAA_R01_AWS_PRE",
        "host":       "ogg-ini-odb-daa-r01-aws-pre.es.mapfre.net",
        "grupo":      "InitialLoad",
        "env":        "PRE",
        "role_base":  "Replicator",
        "unreachable": False,
        "overall":    "OK",
        "errors_24h": 0,
        "s3_bucket":  "",
        "services": [
            {"name": "adminsrvr", "estado": "RUNNING"},
            {"name": "recvsrvr",  "estado": "RUNNING"},
        ],
        "processes": [
            {"name": "RINIT03", "type": "REPLICAT", "status": "STOPPED",
             "lag": "", "last_started": "2026-09-14T07:00:00Z",
             "desc": "Replicat-Initial_load-ACNPDB-00-DATAIA-PRE for table SCHEMA1.HISTORICO part 3/3 (created by cdc-controller date: 2026-09-14 07:00:00)"},
        ],
        "stats": [
            {"tabla": "SCHEMA1.HISTORICO", "bbdd": "ORACLE", "proceso": "RINIT03",
             "proc_type": "REPLICAT", "proc_status": "STOPPED",
             "proc_desc": "Replicat-Initial_load-ACNPDB-00-DATAIA-PRE for table SCHEMA1.HISTORICO part 3/3 (created by cdc-controller date: 2026-09-14 07:00:00)",
             "proc_last_started": "2026-09-14T07:00:00Z",
             "inserts": 0, "updates": 0, "deletes": 0, "upserts": 0, "discards": 0,
             "tot_ins": 12000000, "tot_upd": 0, "tot_del": 0,
             "num_rows": 12000000, "size_mb": 9600},
        ],
    },

    # ── PRO Extractor — with significant lag ──────────────────────────────────
    {
        "dep_name":   "OGG_DR_ODB_ORA_E01_AWS_PRO",
        "host":       "ogg-dr-odb-ora-e01-aws-pro.es.mapfre.net",
        "grupo":      "Online",
        "env":        "PRO",
        "role_base":  "Extractor",
        "unreachable": False,
        "overall":    "OK",
        "s3_bucket":  "s3://mapfre-ogg-pro/trails",
        "errors_24h": 1,
        "services": [
            {"name": "adminsrvr",  "estado": "RUNNING"},
            {"name": "distsrvr",   "estado": "RUNNING"},
            {"name": "pmsrvr",     "estado": "RUNNING"},
            {"name": "recvsrvr",   "estado": "RUNNING"},
            {"name": "pluginsrvr", "estado": "RUNNING"},
        ],
        "processes": [
            {"name": "ECDC01", "type": "EXTRACT", "status": "RUNNING",
             "lag": "4d:02:15:33", "last_started": "2026-09-10T06:00:00Z",
             "desc": "CDC Oracle PRO"},
            {"name": "DP_PRO_01", "type": "DIST_PATH", "status": "RUNNING",
             "lag": "", "last_started": "", "desc": ""},
        ],
        "stats": [
            {"tabla": "SCHEMA1.CLIENTES", "bbdd": "ORACLE", "proceso": "ECDC01",
             "proc_type": "EXTRACT", "proc_status": "RUNNING",
             "since": "2026-09-10T06:00:00Z", "elapsed": "4d 02:15:33",
             "inserts": 98200, "updates": 54100, "deletes": 3200, "upserts": 0, "discards": 0},
        ],
    },

    # ── PRO Replicator — healthy ──────────────────────────────────────────────
    {
        "dep_name":   "OGG_DR_ODB_DAA_R01_AWS_PRO",
        "host":       "ogg-dr-odb-daa-r01-aws-pro.es.mapfre.net",
        "grupo":      "Online",
        "env":        "PRO",
        "role_base":  "Replicator",
        "unreachable": False,
        "overall":    "OK",
        "errors_24h": 0,
        "s3_bucket":  "",
        "services": [
            {"name": "adminsrvr", "estado": "RUNNING"},
            {"name": "recvsrvr",  "estado": "RUNNING"},
            {"name": "pmsrvr",    "estado": "RUNNING"},
        ],
        "processes": [
            {"name": "RCDC01", "type": "REPLICAT", "status": "RUNNING",
             "lag": "00:00:08", "last_started": "2026-09-14T08:10:00Z", "desc": "Apply Oracle CDC PRO"},
            {"name": "RCDC02", "type": "REPLICAT", "status": "RUNNING",
             "lag": "00:00:05", "last_started": "2026-09-14T08:11:00Z", "desc": "Apply Oracle CDC PRO (schema 2)"},
            {"name": "RP_PRO_01", "type": "RCVR_PATH", "status": "RUNNING",
             "lag": "", "last_started": "", "desc": ""},
        ],
        "stats": [
            {"tabla": "DAA.CLIENTES", "bbdd": "ORACLE", "proceso": "RCDC01",
             "proc_type": "REPLICAT", "proc_status": "RUNNING",
             "since": "2026-09-14T08:10:00Z", "elapsed": "02:00:00",
             "inserts": 98100, "updates": 54050, "deletes": 3190, "upserts": 0, "discards": 0,
             "team": "TeamDataOps", "cost_center": "CC001",
             "size_mb": 4096, "num_rows": 42000000},
            {"tabla": "DAA.POLIZAS", "bbdd": "ORACLE", "proceso": "RCDC01",
             "proc_type": "REPLICAT", "proc_status": "RUNNING",
             "since": "2026-09-14T08:10:00Z", "elapsed": "02:00:00",
             "inserts": 12400, "updates": 6800, "deletes": 420, "upserts": 0, "discards": 0,
             "team": "TeamRisk", "cost_center": "CC002",
             "size_mb": 1024, "num_rows": 9800000},
            {"tabla": "DAA.CONTRATOS", "bbdd": "ORACLE", "proceso": "RCDC02",
             "proc_type": "REPLICAT", "proc_status": "RUNNING",
             "since": "2026-09-14T08:11:00Z", "elapsed": "02:00:00",
             "inserts": 5500, "updates": 2100, "deletes": 180, "upserts": 0, "discards": 0,
             "team": "TeamRisk", "cost_center": "CC002",
             "size_mb": 640, "num_rows": 3200000},
            {"tabla": "DAA.PRODUCTOS", "bbdd": "ORACLE", "proceso": "RCDC02",
             "proc_type": "REPLICAT", "proc_status": "RUNNING",
             "since": "2026-09-14T08:11:00Z", "elapsed": "02:00:00",
             "inserts": 210, "updates": 90, "deletes": 5, "upserts": 0, "discards": 0,
             "size_mb": 128, "num_rows": 450000},
        ],
    },
]


# ──────────────────────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Render a sample OGG report with fake data")
    parser.add_argument("--out",  default="sample_report.html", help="Output HTML path")
    parser.add_argument("--open", action="store_true", help="Open the report in the default browser")
    args = parser.parse_args()

    print("Building report data from fake results...")
    data = build_report_data(FAKE_RESULTS)

    total_procs  = len(data["all_procs"])
    total_tables = len(data["stats_rows"])
    print(f"  Hosts:       {len(data['hosts'])}")
    print(f"  Deployments: {len(data['deployments'])}")
    print(f"  Processes:   {total_procs}")
    print(f"  Tables:      {total_tables}")

    ctx = {
        **data,
        "date_str":     "14/09/2026 11:47",
        "total_procs":  total_procs,
        "total_tables": total_tables,
        "counts_js":    json.dumps(data["counts_by_filter"]),
    }

    print(f"Rendering template...")
    templates_dir = str(SCRIPT_DIR / "templates")
    html = render_html(ctx, templates_dir)

    out_path = Path(args.out)
    out_path.write_text(html, encoding="utf-8")
    print(f"\n✓ Report written: {out_path.resolve()}  ({len(html) // 1024} KB)")

    if args.open:
        import webbrowser
        webbrowser.open(out_path.resolve().as_uri())
        print("  Opened in browser.")


if __name__ == "__main__":
    main()
