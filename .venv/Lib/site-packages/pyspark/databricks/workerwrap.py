#
# DATABRICKS CONFIDENTIAL & PROPRIETARY
# __________________
#
# Copyright 2023-present Databricks, Inc.
# All Rights Reserved.
#
# NOTICE:  All information contained herein is, and remains the property of Databricks, Inc.
# and its suppliers, if any.  The intellectual and technical concepts contained herein are
# proprietary to Databricks, Inc. and its suppliers and may be covered by U.S. and foreign Patents,
# patents in process, and are protected by trade secret and/or copyright law. Dissemination, use,
# or reproduction of this information is strictly forbidden unless prior written permission is
# obtained from Databricks, Inc.
#
# If you view or obtain a copy of this information and believe Databricks, Inc. may not have
# intended it to be made available, please promptly report it to Databricks Legal Department
# @ legal@databricks.com.
#

import numbers
import os
import socket
import sys
import textwrap

from threading import Thread, Lock

from pyspark.databricks.utils.memory import clear_memory, get_used_memory
from pyspark.worker import main as worker_main

worker_main_is_running = False
worker_main_is_running_lock = Lock()


def run_worker_main(conn, addr):
    global worker_main_is_running, worker_main_is_running_lock
    """
    Runs the worker_main function in a separate thread. This is necessary because the
    SafeSpark api server makes connection attempts to check liveliness of the worker,
    however disconnects immediately. This function will only allow one connection to
    proceed past the initial connection attempt.
    """

    # Wait for some initial bytes to be sent, so we don't start worker_main if it's just
    # a connection attempt.
    if not conn.recv(4, socket.MSG_PEEK):
        # We do not log here, as liveness checks would only clutter the logs.
        conn.close()
        return

    with worker_main_is_running_lock:
        # Beyond simple connection attempts, only one connection should proceed past this point.
        if worker_main_is_running:
            print("worker_main is already running, this is unexpected.")
            sys.exit(os.EX_SOFTWARE)
        worker_main_is_running = True
    print(f"Received bytes, handing over to worker ({conn} {addr})")

    conn.settimeout(None)  # disable timeout, as worker_main does not expect this to be set

    # The following code is very similar to PySpark's daemon.py - we keep running worker_main
    # until we get a non-zero exit code, or SPARK_REUSE_WORKER is not set.
    reuse = os.environ.get("SPARK_REUSE_WORKER", "0") == "1"
    buffer_size = int(os.environ.get("SPARK_BUFFER_SIZE", 65536))

    infile = os.fdopen(os.dup(conn.fileno()), "rb", buffer_size)
    outfile = os.fdopen(os.dup(conn.fileno()), "wb", buffer_size)

    exit_code = 0
    while True:
        try:
            worker_main(infile, outfile)
        except SystemExit as exc:
            if isinstance(exc.code, numbers.Integral):
                exit_code = exc.code
            else:
                exit_code = 1
        finally:
            try:
                outfile.flush()
            except Exception:
                pass
        if reuse and exit_code == 0:
            # To avoid excessive memory usage we clear up memory after each run.
            mem_before = get_used_memory()
            clear_memory()
            mem_after = get_used_memory()
            print(f"worker_main finished with rc=0, reusing worker ({mem_before}->{mem_after})")
            continue
        else:
            print(f"worker_main finished with {exit_code}")
            break

    conn.close()
    print("run_worker_main finished")
    # When we reach this point, no reuse of the worker is expected by PySpark -
    # we can safely stop the whole process & container.
    sys.exit(0)


def main(port: int):
    """
    Wait for connections - the API server makes connection attempts to
    check liveliness of the worker, however disconnects immediately.
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("0.0.0.0", port))
    s.listen()
    while True:
        conn, addr = s.accept()
        conn.settimeout(30)  # expect some initial bytes to be sent quickly
        Thread(target=run_worker_main, args=(conn, addr)).start()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: -m pyspark.databricks.workerwrap <port>")
        sys.exit(os.EX_USAGE)

    port_arg = sys.argv[-1]
    port = int(port_arg.split(":")[-1])
    startup_info = f"""\
       port: {port}
       sys.argv: {sys.argv}
       sys.version_info: {sys.version_info}
       sys.executable: {sys.executable}
       sys.path: {sys.path}
       os.environ: {dict(os.environ)}
       uid: {os.getuid()}
       groups: {os.getgroups()}"""
    print(textwrap.dedent(startup_info))

    main(port)
