import secrets
from datetime import datetime, timezone


class ULID:
    """https://github.com/ulid/spec"""

    def __init__(self):
        self.crockford_base32_characters = '0123456789ABCDEFGHJKMNPQRSTVWXYZ'

    def new(self):
        current_timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)
        epoch_bits = '{0:050b}'.format(current_timestamp)
        random_bits = '{0:080b}'.format(secrets.randbits(80))
        bits = epoch_bits + random_bits
        return self._generate(bits)

    def _generate(self, bits: str) -> str:
        return ''.join(
            self.crockford_base32_characters[int(bits[i: i + 5], base=2)]
            for i in range(0, 130, 5)
        )
