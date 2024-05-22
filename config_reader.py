import dataclasses
import tomllib
from typing import Any, BinaryIO, Optional, Self


class SecretStr(str):
    """String class that hides its value."""

    def __str__(self) -> str:
        return '*****'

    def __repr__(self) -> str:
        return self.__str__()


@dataclasses.dataclass
class EmailConfig:
    fromaddr: str = 'admin'
    recipients: list[str] = dataclasses.field(default_factory=list)

    @classmethod
    def parse(cls, d: dict[str, Any]) -> Self:
        # rename 'from' (which is a reserved word) to 'fromaddr'
        if 'from' in d:
            d['fromaddr'] = d.pop('from')

        return cls(**d)

    def validate(self) -> None:
        if not isinstance(self.fromaddr, str):
            raise TypeError(f"Email fromaddr must be a string, not {self.fromaddr}")
        if not (isinstance(self.recipients, list) and all(isinstance(e, str) for e in self.recipients)):
            raise TypeError(f"Email recipients must be a list of strings, not {self.recipients}")


@dataclasses.dataclass
class PoolConfig:
    name: str
    autobackup_parameters: list[str] = dataclasses.field(default_factory=list)
    passphrase: Optional[SecretStr] = None

    @classmethod
    def parse(cls, d: dict[str, Any]) -> Self:
        if (passphrase := d.get('passphrase')): # wrap passphrase in a SecretStr
            d['passphrase'] = SecretStr(passphrase)

        return cls(**d)

    def validate(self) -> None:
        if not isinstance(self.name, str):
            raise TypeError(f"Pool name must be a string, not {self.name}")
        if not (isinstance(self.autobackup_parameters, list) and all(isinstance(e, str) for e in self.autobackup_parameters)):
            raise TypeError(f"autobackup_parameters must be a list of strings, not {self.autobackup_parameters}")


@dataclasses.dataclass
class AppConfig:
    pools: dict[str, PoolConfig] = dataclasses.field(default_factory=dict)
    email: EmailConfig = dataclasses.field(default_factory=EmailConfig)
    beep: bool = True

    @classmethod
    def parse(cls, d: dict[str, Any]) -> Self:
        unknown_keys = d.keys() - {'pools', 'email', 'general'}
        if unknown_keys:
            raise ValueError(f"Unsupported configuration keys: {', '.join(unknown_keys)}")

        pools = {}
        for pd in d.get('pools', []):
            pool = PoolConfig.parse(pd)
            if pool.name in pools:
                raise ValueError(f"Pool {pool.name} has multiple definitions")
            pools[pool.name] = pool

        if not pools:
            raise ValueError("No pools are configured")

        email = EmailConfig.parse(d.get('email', {}))

        return cls(pools, email, **d.get('general', {}))

    def validate(self) -> None:
        for pool in self.pools.values():
            pool.validate()
        self.email.validate()
        if not isinstance(self.beep, bool):
            raise TypeError(f"beep must be a boolean, not {self.beep}")

    @classmethod
    def parse_and_validate_file(cls, stream: BinaryIO) -> Self:
        try:
            d = tomllib.load(stream)
        except tomllib.TOMLDecodeError as e:
            raise ValueError("Failed to parse configuration file") from e

        config = cls.parse(d)
        config.validate()
        return config
