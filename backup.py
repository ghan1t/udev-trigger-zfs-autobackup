import io
from contextlib import redirect_stdout, redirect_stderr
from zfs_autobackup.ZfsAutobackup import ZfsAutobackup  # type: ignore [import-untyped]
from typing import Optional
from log_util import Logging
from mail_util import mail, mail_error
from config_reader import AppConfig, PoolConfig
import subprocess
import traceback

# Backup function
def import_decrypt_backup_export(device_label: str, config: AppConfig, logger: Logging) -> None:
    pool_config = config.pools.get(device_label, None)
    if pool_config is None:
        mail(f"Plugged in disk {device_label} that is not matching any configuration. You can unplug it again safely.",
             config.email, logger)
        return

    try:
        logger.log(f"Importing pool {device_label}")
        result = import_pool(device_label, logger)
        if result.returncode != 0:
            mail_error(f"Failed to import pool. Backup not yet run.\n\nError:\n{result.stderr}", config.email, logger)
            return

        captured_output = decrypt_and_backup(device_label, pool_config, config, logger)
        if captured_output is None:
            return # backup unsuccessful
        
        logger.log(f"Exporting {device_label}")
        result = export_pool(device_label, logger)
        if result.returncode != 0:
            mail_error(f"Failed to export pool.\n\nError:\n{result.stderr}", config.email, logger)
            return

        msg = f"Backup finished. You can safely unplug the disk {device_label} now."
        if config.email is not None and config.email.send_autobackup_output:
            msg += f"\n\nZFS-Autobackup output:\n{captured_output}"
        mail(msg, config.email, logger)

    except Exception as e:
        mail_error(f"An unexpected error occurred. Backup may have failed. Please investigate.\n\nError:\n{e}\n{traceback.format_exc()}", config.email, logger)


def decrypt_and_backup(device_label: str, pool_config: PoolConfig, config: AppConfig, logger: Logging) -> Optional[str]:
    if pool_config.passphrase:
        logger.log(f"Decrypting pool {device_label}")
        result = decrypt_pool(device_label, pool_config.passphrase, logger)
        if result.returncode != 0:
            mail_error(f"Failed to decrypt pool. Backup not yet run.\n\nError:\n{result.stderr}", config.email, logger)
            return None

    logger.log(f"Starting ZFS-Autobackup for pool {device_label} with parameters:\nzfs-autobackup " + " ".join(pool_config.autobackup_parameters))
    success, stdout, stderr = run_zfs_autobackup(pool_config.autobackup_parameters, logger)
    if not success or stderr:
        msg = "ZFS autobackup error! Disk will not be exported automatically."
        if config.email is not None and config.email.send_autobackup_output:
            msg += f"\n\n{stderr if stderr else stdout}"
        else:
            msg += " Check logs for details."
        mail_error(msg, config.email, logger)
    elif stdout:
        logger.log(stdout)

    logger.log(f"Setting pool {device_label} to read-only")
    result = set_pool_readonly(device_label, logger)
    if result.returncode != 0:
        mail_error(f"Failed to set pool readonly. Disk will not be exported automatically.\n\nError:\n{result.stderr}\n\nBackup was successful:\n{stdout}\n\n{stderr}", config.email, logger)
        return None
    return stdout


def run_zfs_autobackup(args: list[str], logger: Logging) -> tuple[bool, str, str]:
    """
    Runs the ZfsAutobackup CLI with given arguments and captures its stdout output.

    :param args: List of command-line arguments to pass to ZfsAutobackup CLI.
    :return: 3-tuple of a boolean success flag, the captured stdout, and captured stderr.
    """
    # Capture both standard output and standard error
    stdout_capture = io.StringIO()
    stderr_capture = io.StringIO()
    with redirect_stdout(stdout_capture), redirect_stderr(stderr_capture):
        failed_datasets = ZfsAutobackup(args, print_arguments=False).run()
    
    return failed_datasets == 0, stdout_capture.getvalue(), stderr_capture.getvalue()


def import_pool(pool: str, logger: Logging) -> subprocess.CompletedProcess:
    return run_command(logger, ["zpool", "import", pool, "-N"])


def export_pool(pool: str, logger: Logging) -> subprocess.CompletedProcess:
    return run_command(logger, ["zpool", "export", pool])


def decrypt_pool(pool: str, passphrase: str, logger: Logging) -> subprocess.CompletedProcess:
    return run_command(logger, ["zfs", "load-key", pool], input_data=passphrase)


def set_pool_readonly(pool: str, logger: Logging) -> subprocess.CompletedProcess:
    return run_command(logger, ["zfs", "set", "readonly=on", pool])


def run_command(logger: Logging, command: list[str], input_data: Optional[str] = None) -> subprocess.CompletedProcess:
    result = subprocess.run(command, input=input_data, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if result.returncode != 0:
        logger.error(f"Error: command '{command}' returned exit status {result.returncode}")
    return result
