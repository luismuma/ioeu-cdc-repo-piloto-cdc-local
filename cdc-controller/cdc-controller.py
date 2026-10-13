import sys
import time
from datetime import datetime
from utils import *
from validate import validate
from apply import apply, plan
from config_trandata import config_trandata

version      = 1.6
current_date = datetime.now().strftime("%d-%b-%Y %H:%M:%S").upper()

USAGE = f"""
Usage: python cdc-controller.py <command> [options]

Commands:
  -help                        Show this help message.

  -validate   <file.json>      Run all required validations for the replication defined in <file.json>:
                                - source/target database prerequisites (driver, version, settings)
                                - replication software (Oracle GoldenGate or IBM IIDR)
                                - target connectivity (Kafka or AWS S3)

  -apply      <file.json>      Configure and start every process needed to run the replication
                               requested by the user in <file.json>

  -plan       <file.json>      Same logic as -apply but READ-ONLY: report exactly what would be
                               created / modified / started, without executing any change on OGG
                               deployments, processes, or PRM files.

  -config_trandata <file.json> Configure Oracle supplemental logging (TRANDATA) for the tables
                               listed in <file.json> via OGG Microservices REST API.
                               Only these fields are required: env, request_type, bbdd_engine,
                               bbdd_name, tables.
                               Supported request_type values:
                                 CREATE — enable TRANDATA (all columns) + verify with INFO
                                 DELETE — disable TRANDATA + verify with INFO
                                 STATUS — show current TRANDATA configuration (read-only)

Notes:
  - The action performed is driven by the JSON field 'request_type' (CREATE | UPDATE | DELETE | STATUS).
  - 'request_type': STATUS  works with -validate, -plan and -apply alike; in all three cases
    cdc-controller produces a read-only report of the OGG processes that handle the requested
    replication. No CREATE / DELETE side-effects are performed.
"""

####### MAIN CDC-CONTROLLER #######
def main():
    start_time = time.time()

    # print Begin banner
    banner = f"cdc-controller version {version} on {current_date}"
    print(f"\n{banner}\n")

    # init log file
    setup_log_file(banner)

    if len(sys.argv) < 2 or sys.argv[1] == "-help":
        print(USAGE)
        sys.exit(0)

    # get input file 
    command = sys.argv[1]
    # define principal functions 
    commands_with_file = {
        "-validate":          validate,
        "-apply":             apply,
        "-plan":              plan,
        "-config_trandata":   config_trandata,
    }

    if command not in commands_with_file:
        print_log("error", f"unknown command '{command}'.")
        print(USAGE)
        sys.exit(1)

    if len(sys.argv) < 3:
        print_log("error", f"command '{command}' requires a JSON file as argument.")
        print(USAGE)
        sys.exit(1)

    rc = commands_with_file[command](sys.argv[2])

    # elapsed time — format as 'Xd Yh Zm W.WWs' omitting leading zero units so short
    # runs stay readable (e.g. "12.34s") and long ones (initial_load FULL) show days
    # without a wall of zeros (e.g. "1d 4h 12m 7.50s").
    elapsed   = time.time() - start_time
    secs_rem  = elapsed
    days      = int(secs_rem // 86400);  secs_rem -= days  * 86400
    hours     = int(secs_rem // 3600);   secs_rem -= hours * 3600
    minutes   = int(secs_rem // 60);     secs_rem -= minutes * 60
    if days:
        elapsed_str = f"{days}d {hours}h {minutes}m {secs_rem:.2f}s"
    elif hours:
        elapsed_str = f"{hours}h {minutes}m {secs_rem:.2f}s"
    elif minutes:
        elapsed_str = f"{minutes}m {secs_rem:.2f}s"
    else:
        elapsed_str = f"{secs_rem:.2f}s"
    # print End
    print(f"\nelapsed time: {elapsed_str}")
    sys.exit(rc if rc is not None else 0)


if __name__ == "__main__":
    main()
