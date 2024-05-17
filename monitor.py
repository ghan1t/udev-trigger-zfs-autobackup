#!/usr/bin/env python3

import argparse
from contextlib import redirect_stdout, redirect_stderr
import dataclasses
from email.message import EmailMessage
import io
import logging
import logging.handlers
import os
import pyudev  # type: ignore [import-untyped]
import queue
import shlex
import subprocess
import sys
import traceback
from typing import Dict, List, Optional, TextIO
import yaml
from zfs_autobackup.ZfsAutobackup import ZfsAutobackup  # type: ignore [import-untyped]

FINISHED_BEEP_INTERVAL = 10 # seconds

logger = logging.getLogger('ZfsAutobackupTrigger')


class SecretStr(str):
    """String class that hides its value."""

    def __str__(self) -> str:
        return '*****'

    def __repr__(self) -> str:
        return self.__str__()


@dataclasses.dataclass
class EmailConfig:
    fromaddr: str = dataclasses.field(default='admin')
    recipients: List[str] = dataclasses.field(default_factory=list)


@dataclasses.dataclass
class PoolConfig:
    pool_name: str
    autobackup_parameters: List[str] = dataclasses.field(default_factory=list)
    passphrase: Optional[SecretStr] = dataclasses.field(default=None)


@dataclasses.dataclass
class AppConfig:
    pools: Dict[str, PoolConfig] = dataclasses.field(default_factory=dict)
    email: EmailConfig = dataclasses.field(default_factory=EmailConfig)


# Load and validate the YAML config
def read_validate_config(config_stream: TextIO) -> AppConfig:
    config = yaml.safe_load(config_stream)

    # Check if 'email' key is present in the config
    email_conf = None
    if 'email' in config:
        recipients_str = config['email'].get('recipients')
        if not isinstance(recipients_str, str) or not recipients_str:
            raise ValueError("The 'recipients' key must be a non-empty string.")
        
        # Add validated and stripped recipients back to the config
        config['email']['recipients'] = [email.strip() for email in recipients_str.split(',')]
        email_conf = EmailConfig(**config['email'])
    else:
        email_conf = EmailConfig()

    # Initialize pool configurations if they're present
    pool_confs = {}
    if 'pools' in config:
        for pool_key, pool_values in config['pools'].items():
            if 'autobackup_parameters' not in pool_values:
                raise ValueError(f"Pool '{pool_key}' is missing mandatory parameter 'autobackup_parameters'.")
            if pool_values.get('split_parameters', True):
                # split parameters using shell-like word splitting, and flatten back into a single-level list
                pool_values['autobackup_parameters'] = [p2 for p1 in pool_values['autobackup_parameters'] for p2 in shlex.split(p1)]
            if 'passphrase' in pool_values:
                pool_values['passphrase'] = SecretStr(pool_values['passphrase'])

            pool_confs[pool_key] = PoolConfig(pool_key, **pool_values)
    else:
        raise ValueError("The 'pools' field is missing or empty.")

    return AppConfig(email=email_conf, pools=pool_confs)


class UdevAutobackupMonitor:
    def __init__(self, config: AppConfig):
        self.device_events: queue.Queue[tuple[str, str]] = queue.Queue()
        self.config = config

    def run(self, test: bool = False):
        logger.info(f"started with config: {self.config}")
        if test:
            for device_label, pool_config in self.config.pools.items():
                if is_device_connected(device_label):
                    logger.info(f"Starting manual backup on Pool {device_label}...")
                    import_decrypt_backup_export(pool_config)
        else:
            logger.debug('Using pyudev version: {0}'.format(pyudev.__version__))
            monitor = pyudev.Monitor.from_netlink(pyudev.Context())
            monitor.filter_by('block')
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
                    beep()
                    continue

                if action == "add":
                    beep()
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

        assert pool_config.pool_name == device_label
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
        message = EmailMessage()
        message.set_content(body)  # Set email body
        message['Subject'] = 'ZFS-Autobackup Trigger: ' + subject  # Set email subject
        message['From'] = self.config.email.fromaddr  # Set email from
        message['To'] = self.config.email.recipients # All recipients

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


def import_decrypt_backup_export(pool_config: PoolConfig) -> tuple[bool, str]:
    logger.debug(f"Importing pool {pool_config.pool_name}")
    result = run_command(["zpool", "import", pool_config.pool_name, "-N"])
    if result.returncode != 0:
        return False, f"Failed to import pool. Backup not yet run.\n" + result.stderr

    if pool_config.passphrase:
        logger.debug(f"Decrypting pool {pool_config.pool_name}")
        result = run_command(["zfs", "load-key", pool_config.pool_name], input=pool_config.passphrase)
        if result.returncode != 0:
            return False, f"Failed to decrypt pool. Backup not yet run.\n" + result.stderr

    logger.info(f"Starting ZFS-Autobackup for pool {pool_config.pool_name} with parameters: " + " ".join(pool_config.autobackup_parameters))
    success, stdout, stderr = run_zfs_autobackup(pool_config.autobackup_parameters)
    if not success or stderr: # something went wrong
        logger.error("ZFS autobackup failed")
        return False, "ZFS autobackup error! Disk will not be exported automatically.\n" + stderr
    elif stdout:
        logger.info(stdout)

    logger.debug(f"Setting pool {pool_config.pool_name} to read-only")
    result = run_command(["zfs", "set", "readonly=on", pool_config.pool_name])
    if result.returncode != 0:
        return False, f"Failed to set pool readonly. Backup succeeded, but disk will not be exported automatically.\n" + result.stderr

    logger.debug(f"Exporting {pool_config.pool_name}")
    result = run_command(["zpool", "export", pool_config.pool_name])
    if result.returncode != 0:
        return False, f"Failed to export pool. Backup succeeded, but disk will not be exported automatically.\n" + result.stderr

    return True, stdout


def run_zfs_autobackup(args: list[str]) -> tuple[bool, str, str]:
    """
    Runs the ZfsAutobackup CLI with given arguments and captures its stdout output.

    :param args: List of command-line arguments to pass to ZfsAutobackup CLI.
    :return: 3-tuple of a boolean success flag, the captured stdout, and captured stderr.
    """
    # Capture both standard output and standard error
    stdout_capture = io.StringIO()
    stderr_capture = io.StringIO()
    with redirect_stdout(stdout_capture), redirect_stderr(stderr_capture):
        real_argv0 = sys.argv[0]
        try:
            sys.argv[0] = 'zfs-autobackup'
            failed_datasets = ZfsAutobackup(args, print_arguments=False).run()
        finally:
            sys.argv[0] = real_argv0
    
    return failed_datasets == 0, stdout_capture.getvalue(), stderr_capture.getvalue()


def run_command(command: list[str], input: Optional[str] = None) -> subprocess.CompletedProcess:
    result = subprocess.run(command, input=input, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if result.returncode != 0:
        logger.error(f"Error: command '{command}' returned exit status {result.returncode}")
    return result


def is_device_connected(device_label: str) -> bool:
    return os.path.islink(os.path.join('/dev/disk/by-label', device_label))


def beep() -> None:
    with open('/dev/tty5','w') as f:
        f.write('\a')


def init_logging() -> None:
    # Always log to syslog at INFO level
    syslog_handler = logging.handlers.SysLogHandler(address="/dev/log")
    syslog_handler.setLevel(logging.INFO)
    logging.basicConfig(
        format='%(levelname)s: %(message)s',
        handlers=(syslog_handler,),
        level=syslog_handler.level)

    # If running on a terminal, assume the user wants debug output
    if sys.stdout.isatty():
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        root_logger = logging.getLogger()
        root_logger.addHandler(console_handler)
        root_logger.setLevel(console_handler.level)


if __name__ == "__main__":
     # Set up command-line argument parsing
    parser = argparse.ArgumentParser(description="Triggers zfs-autobackup jobs on disk hotplug events")
    parser.add_argument('config_file', type=argparse.FileType("r"), help='Path to the YAML config file')
    parser.add_argument("--test", help="test zfs-backup with the given config file", action="store_true")

    # Parse command-line arguments
    args = parser.parse_args()

    try:
        config = read_validate_config(args.config_file)
    except yaml.YAMLError as e:
        sys.stderr.write(f"Error parsing YAML file: {e}\n")
        sys.exit(1)
    except ValueError as e:
        sys.stderr.write(f"Configuration validation error: {e}\n")
        sys.exit(1)
    
    init_logging()
    app = UdevAutobackupMonitor(config)
    app.run(test=args.test)
