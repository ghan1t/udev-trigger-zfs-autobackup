from dataclasses import dataclass, field, asdict
import json
import shlex
import sys
from typing import List, Dict, Optional
import yaml


@dataclass
class EmailConfig:
    fromaddr: str
    recipients: List[str]
    send_autobackup_output: bool

    def __str__(self):
        return json.dumps(asdict(self), indent=2)


@dataclass
class LoggingConfig:
    level: str
    logfile_path: str
    def __str__(self):
        return json.dumps(asdict(self), indent=2)


@dataclass
class PoolConfig:
    pool_name: str
    autobackup_parameters: List[str] = field(default_factory=list)
    passphrase: Optional[str] = ""  # Optional, default is empty string
    def __str__(self):
        data = asdict(self)
        data['passphrase'] = '*****' if self.passphrase else self.passphrase  # Replace the passphrase when logging
        return json.dumps(data, indent=2)


# Application configuration including logging and pools
@dataclass
class AppConfig:
    logging: Optional[LoggingConfig] = None
    pools: Dict[str, PoolConfig] = field(default_factory=dict)
    email: Optional[EmailConfig] = field(default=None)
    def __str__(self):
        data = asdict(self)
        for pool_key, pool in data.get('pools', {}).items():
            if 'passphrase' in pool and pool['passphrase']:
                data['pools'][pool_key]['passphrase'] = '***'
        return json.dumps(data, indent=2)


# Load and validate the YAML config
def read_validate_config(config_path: str) -> AppConfig:
    with open(config_path, 'r') as stream:
        try:
            config = yaml.safe_load(stream)

            # Check for mandatory fields
            if config.get('logging') is None:
                raise ValueError("The 'logging' field is missing or not set.")
            elif config['logging'].get('logfile_path') is None:
                raise ValueError("The 'logfile_path' field is missing or not set.")
            if not config['pools']:
                raise ValueError("The 'pools' field is missing or empty.")

            # Check if 'email' key is present in the config
            email_conf = None
            if 'email' in config:
                required_keys = ['fromaddr', 'recipients']
                missing_keys = [key for key in required_keys if key not in config['email']]
                if missing_keys:
                    raise ValueError(f"Missing required email config keys: {', '.join(missing_keys)}")
                # Validate each email address in the comma-separated recipients list
                recipients_str = config['email']['recipients']
                if not isinstance(recipients_str, str) or not recipients_str:
                    raise ValueError("The 'recipients' key must be a non-empty string.")
                recipients = [email.strip() for email in recipients_str.split(',')]
                
                # Add validated and stripped recipients back to the config
                config['email']['recipients'] = recipients
                email_conf = EmailConfig(**config['email'])
            
            # Initialize logging configuration if it's present
            logging_conf = None
            if 'logging' in config:
                logging_conf = LoggingConfig(**config['logging'])

            # Initialize pool configurations if they're present
            pool_confs = {}
            if 'pools' in config:
                for pool_key, pool_values in config['pools'].items():
                    if 'pool_name' not in pool_values or 'autobackup_parameters' not in pool_values:
                        raise ValueError(f"Pool '{pool_key}' is missing mandatory parameters 'pool_name', 'autobackup_parameters'.")
                    if pool_values.get('split_parameters', True):
                        # split parameters using shell-like word splitting, and flatten back into a single-level list
                        pool_values['autobackup_parameters'] = [p2 for p1 in pool_values['autobackup_parameters'] for p2 in shlex.split(p1)]

                    pool_confs[pool_values['pool_name']] = PoolConfig(**pool_values)
            else:
                raise ValueError(f"missing parameter pools'.")

            # Return an AppConfig instance with logging and pool configurations
            return AppConfig(email=email_conf, logging=logging_conf, pools=pool_confs)

        except yaml.YAMLError as exc:
            sys.exit(f"Error parsing YAML file: {exc}")
        except ValueError as ve:
            sys.exit(f"Configuration validation error: {ve}")
