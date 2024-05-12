#!/usr/bin/env python3

# To be called by trigger.sh
import os
import pyudev  # type: ignore [import-untyped]

import argparse
from config_reader import read_validate_config
import queue
from log_util import Logging
from backup import decrypt_and_backup, import_decrypt_backup_export

FINISHED_BEEP_INTERVAL = 10 # seconds

class UdevAutobackupMonitor:
    def __init__(self, config_file: str):
        self.device_events: queue.Queue[tuple[str, str]] = queue.Queue()
        self.config = read_validate_config(config_file)

        # Set up logging based on configuration
        self.logger = Logging(self.config.logging)

    def run(self, test: bool = False):
        self.logger.log(f"started monitor.py with config:\n{self.config}")
        if test:
            for device_label, pool_config in self.config.pools.items():
                if is_device_connected(device_label):
                    beep()
                    self.logger.log(f"Starting manual backup on Pool {device_label}...")
                    decrypt_and_backup(device_label, pool_config, self.config, self.logger)
        else:
            self.logger.log('Using pyudev version: {0}'.format(pyudev.__version__))
            monitor = pyudev.Monitor.from_netlink(pyudev.Context())
            monitor.filter_by('block')
            self._wait_for_udev_triggers(monitor)

    # Callback for device events, runs in a separate thread
    def _device_callback(self, device: pyudev.Device) -> None:
        fs_type = device.get('ID_FS_TYPE')
        fs_label = device.get('ID_FS_LABEL')
        #fs_uuid = device.get('ID_FS_UUID')
        if fs_type == "zfs_member" and fs_label and fs_label in self.config.pools and device.action in ("add", "remove"):
            self.logger.log(f"udev observed {device.action} of pool {fs_label}")
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
                    self.logger.log(f"Pool {device_label} has been added to queue. Starting backup...")
                    import_decrypt_backup_export(device_label, self.config, self.logger)
                    finished_devices.add(device_label)  # Add to finished devices set
                elif action == "remove":
                    finished_devices.discard(device_label)

        except KeyboardInterrupt:
            self.logger.log("Received KeyboardInterrupt...")
        except Exception as e:
            self.logger.exception(f"An unexpected error occurred: {e}")
        finally:
            self.logger.log("Stopping PYUDEV and Shutting down...")
            observer.stop()
            # sys.exit(0)


def is_device_connected(device_label: str) -> bool:
    return os.path.islink(os.path.join('/dev/disk/by-label', device_label))


def beep() -> None:
    with open('/dev/tty5','w') as f:
        f.write('\a')


if __name__ == "__main__":
     # Set up command-line argument parsing
    parser = argparse.ArgumentParser(description="UDEV monitor to start zfs-autobackup jobs")
    parser.add_argument('config_file', type=str, help='Path to the YAML config file')
    parser.add_argument("--test", help="test zfs-backup with the given config file", action="store_true")

    # Parse command-line arguments
    args = parser.parse_args()

    app = UdevAutobackupMonitor(args.config_file)
    app.run(test=args.test)
