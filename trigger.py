#!/usr/bin/env python3

import argparse
from config_reader import AppConfig, PoolConfig
from contextlib import redirect_stdout, redirect_stderr
from email.message import EmailMessage
import email.policy
import enum
import io
import logging
import logging.handlers
import os
from pep3143daemon import DaemonContext, PidFile  # type: ignore [import-untyped]
import pyudev  # type: ignore [import-untyped]
import queue
import signal
import subprocess
import sys
import traceback
from typing import Optional

# Kludge: zfs-autobackup captures sys.argv[0] at import time for use in log output
_real_argv0 = sys.argv[0]
try:
    sys.argv[0] = 'zfs-autobackup'
    from zfs_autobackup.ZfsAutobackup import ZfsAutobackup  # type: ignore [import-untyped]
finally:
    sys.argv[0] = _real_argv0
    del _real_argv0

APP_NAME = 'trigger-zfs-autobackup'
PID_FILE_PATH = f'/var/run/{APP_NAME}.pid'
FINISHED_BEEP_INTERVAL = 10 # seconds

logger = logging.getLogger(APP_NAME)


class UdevAutobackupMonitor:
    def __init__(self, config: AppConfig):
        self.device_events: queue.Queue[tuple[str, str]] = queue.Queue()
        self.config = config

    def test(self) -> None:
        def is_device_connected(device_label: str) -> bool:
            return os.path.islink(os.path.join('/dev/disk/by-label', device_label))

        print(f"Testing with config: {self.config}")
        for device_label, pool_config in self.config.pools.items():
            if is_device_connected(device_label):
                print(f"Starting manual backup on pool {device_label}...")
                _, msg = import_decrypt_backup_export(pool_config)
                print(msg)

    def run(self, *, daemon: bool = False) -> None:
        logger.info(f"Waiting for devices: {', '.join(self.config.pools.keys())}")
        logger.debug('Using pyudev version: {0}'.format(pyudev.__version__))

        monitor = pyudev.Monitor.from_netlink(pyudev.Context())
        monitor.filter_by('block')

        if daemon:
            daemon_ctx = DaemonContext(pidfile=PidFile(PID_FILE_PATH), files_preserve=[monitor])
            daemon_ctx.open()

        self._wait_for_udev_triggers(monitor)

    # Callback for device events, runs in a separate thread
    def _device_callback(self, device: pyudev.Device) -> None:
        fs_type = device.get('ID_FS_TYPE')
        fs_label = device.get('ID_FS_LABEL')
        #fs_uuid = device.get('ID_FS_UUID')
        if fs_type == "zfs_member" and fs_label and fs_label in self.config.pools and device.action in ("add", "remove"):
            logger.debug(f"udev observed {device.action} of pool {fs_label}")
            self.device_events.put((device.action, fs_label))

    def _wait_for_udev_triggers(self, monitor: pyudev.Monitor) -> None:
        observer = pyudev.MonitorObserver(monitor, callback=self._device_callback)
        observer.start()
        finished_devices: set[str] = set()

        try:
            while True:
                # block indefinitely for an event, unless we're waiting for removal of finished devices
                try:
                    action, device_label = self.device_events.get(block=True, timeout=(FINISHED_BEEP_INTERVAL if finished_devices else None))
                except queue.Empty:
                    # Continuously beep for finished devices if they are still connected
                    self.beep()
                    continue

                if action == "add":
                    self.beep()
                    self._do_backup(device_label)
                    finished_devices.add(device_label)  # Add to finished devices set
                elif action == "remove":
                    finished_devices.discard(device_label)

        except KeyboardInterrupt:
            logger.info("Received KeyboardInterrupt...")
        except Exception as e:
            logger.exception(f"An unexpected error occurred: {e}")
        finally:
            logger.info("Shutting down")
            observer.stop()

    def _do_backup(self, device_label: str) -> None:
        try:
            pool_config = self.config.pools[device_label]
        except KeyError:
            logger.info(f"Unrecognized disk {device_label}")
            self._send_email(
                "Unrecognized disk",
                f"Plugged in disk {device_label} that is not matching any configuration. You can unplug it again safely.")
            return

        assert pool_config.name == device_label
        logger.info(f"Pool {device_label} has been added to queue. Starting backup...")

        try:
            success, msg = import_decrypt_backup_export(pool_config)
        except Exception as e:
            logger.exception("Unexpected error, backup has failed")
            self._send_email(
                "Unexpected error",
                f"An unexpected error occurred. Backup has probably failed. Please investigate.\nError: {e}\n{traceback.format_exc()}")
            return

        if success:
            logger.info(f"Backup of {device_label} completed")
            self._send_email(
                f"Backup of {device_label} completed", 
                f"Backup finished. You can safely unplug the disk {device_label} now.\n\n" + msg)
        else:
            self._send_email(f"Error backing up {device_label}", msg)

    def _send_email(self, subject: str, body: str) -> None:
        if not self.config.email.recipients:
            return

        # Create the plain-text email
        message = EmailMessage(policy=email.policy.default)
        message['From'] = self.config.email.fromaddr
        message['To'] = self.config.email.recipients
        message['Subject'] = f"[{APP_NAME}] {subject}"
        message.set_content(body, cte='8bit')

        # On TrueNAS, sendmail is a python script, so we must not leak our private venv to it.
        child_env = os.environ
        if 'VIRTUAL_ENV' in os.environ:
            child_env['PATH'] = '/usr/local/bin:/usr/bin:/bin:/usr/games'

        # Send the email
        try:
            subprocess.run(["/usr/sbin/sendmail", "-t", "-i"], env=child_env, input=message.as_bytes(), check=True)
            logger.debug(f"Email about {subject} sent to {self.config.email.recipients}")
        except subprocess.CalledProcessError as e:
            logger.exception(f"Error sending email to {self.config.email.recipients}")

    def beep(self) -> None:
        if self.config.beep:
            with open('/dev/tty5','w') as f:
                f.write('\a')


def import_decrypt_backup_export(pool_config: PoolConfig) -> tuple[bool, str]:
    logger.debug(f"Importing pool {pool_config.name}")
    result = run_command(["zpool", "import", pool_config.name, "-N"])
    if result.returncode != 0:
        return False, f"Failed to import pool. Backup not yet run.\n" + result.stderr

    if pool_config.passphrase:
        logger.debug(f"Decrypting pool {pool_config.name}")
        result = run_command(["zfs", "load-key", pool_config.name], input=pool_config.passphrase)
        if result.returncode != 0:
            return False, f"Failed to decrypt pool. Backup not yet run.\n" + result.stderr

    logger.info(f"Starting ZFS-Autobackup for pool {pool_config.name} with parameters: " + " ".join(pool_config.autobackup_parameters))
    success, stdout, stderr = run_zfs_autobackup(pool_config.autobackup_parameters)
    if not success or stderr: # something went wrong
        logger.error("ZFS autobackup failed")
        return False, "ZFS autobackup error! Disk will not be exported automatically.\n" + stderr + stdout
    elif stdout:
        logger.info(stdout)

    logger.debug(f"Setting pool {pool_config.name} to read-only")
    result = run_command(["zfs", "set", "readonly=on", pool_config.name])
    if result.returncode != 0:
        return False, f"Failed to set pool readonly. Backup succeeded, but disk will not be exported automatically.\n" + result.stderr

    logger.debug(f"Exporting {pool_config.name}")
    result = run_command(["zpool", "export", pool_config.name])
    if result.returncode != 0:
        return False, f"Failed to export pool. Backup succeeded, but disk will not be exported automatically.\n" + result.stderr

    return True, stdout


def run_zfs_autobackup(args: list[str]) -> tuple[bool, str, str]:
    """
    Runs the ZfsAutobackup CLI with given arguments and captures its stdout output.

    :param args: List of command-line arguments to pass to ZfsAutobackup CLI.
    :return: 3-tuple of a boolean success flag, the captured stdout, and captured stderr.
    """

    success = False
    stdout_capture = io.StringIO()
    stderr_capture = io.StringIO()

    try:
        with redirect_stdout(stdout_capture), redirect_stderr(stderr_capture):
            failed_datasets = ZfsAutobackup(args, print_arguments=False).run()
        success = (failed_datasets == 0)
    except SystemExit as e:
        logger.error(f"zfs-autobackup exited with code {e.code}")

    return success, stdout_capture.getvalue(), stderr_capture.getvalue()


def run_command(command: list[str], input: Optional[str] = None) -> subprocess.CompletedProcess:
    result = subprocess.run(command, input=input, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if result.returncode != 0:
        logger.error(f"Error: command '{command}' returned exit status {result.returncode}")
    return result


def init_logging(run_as_daemon: bool) -> None:
    # Always log to syslog at INFO level
    syslog_handler = logging.handlers.SysLogHandler(address="/dev/log")
    syslog_handler.setLevel(logging.INFO)
    logging.basicConfig(
        format=APP_NAME + ' %(levelname)s: %(message)s',
        handlers=(syslog_handler,),
        level=syslog_handler.level)

    # If running on a terminal, assume the user wants debug output
    if not run_as_daemon and sys.stdout.isatty():
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        root_logger = logging.getLogger()
        root_logger.addHandler(console_handler)
        root_logger.setLevel(console_handler.level)


if __name__ == "__main__":
     # Set up command-line argument parsing
    parser = argparse.ArgumentParser(description="Triggers zfs-autobackup jobs on disk hotplug events")
    parser.add_argument('config_file', type=argparse.FileType("rb"), help='Path to the config file', nargs='?')

    Action = enum.Enum('Action', ['TEST', 'START', 'STOP', 'RESTART'])
    g = parser.add_mutually_exclusive_group()
    g.add_argument("--start", help="Start as a daemon process", action="store_const", dest='action', const=Action.START)
    g.add_argument("--stop", help="Stop a running daemon process", action="store_const", dest='action', const=Action.STOP)
    g.add_argument("--restart", help="Restart a running daemon process", action="store_const", dest='action', const=Action.RESTART)
    g.add_argument("--test", help="Run a one-time backup for all configured pools, then exit", action="store_const", dest='action', const=Action.TEST)

    # Parse command-line arguments
    args = parser.parse_args()

    # Parse config file
    if args.config_file is not None:
        try:
            config = AppConfig.parse_and_validate_file(args.config_file)
        except (ValueError, TypeError) as e:
            sys.stderr.write(f"Error in configuration file: {e}\n")
            sys.exit(1)
        args.config_file.close()
    elif args.action != Action.STOP:
        parser.error('config file argument is required')

    run_as_daemon = args.action in {Action.START, Action.RESTART}
    init_logging(run_as_daemon)

    # Stop an existing daemon
    if args.action in {Action.STOP, Action.RESTART}:
        oldpid = None
        try:
            with open(PID_FILE_PATH, 'r') as fh:
                oldpid = int(fh.read())
        except OSError:
            pass

        if oldpid is not None:
            try:
                os.kill(oldpid, signal.SIGTERM)
                print(f"Sent SIGTERM to PID {oldpid}")
            except ProcessLookupError:
                pass

    if args.action != Action.STOP:
        app = UdevAutobackupMonitor(config)
        if args.action == Action.TEST:
            app.test()
        else:
            app.run(daemon=run_as_daemon)
