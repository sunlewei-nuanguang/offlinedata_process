from __future__ import (absolute_import, division,
                        print_function, unicode_literals)

import random as _random

from .hotp import HOTP  # noqa
from .otp import OTP  # noqa
from .totp import TOTP  # noqa
from . import utils  # noqa


def random_base32(length=16, random=_random.SystemRandom(),
                  chars=list('ABCDEFGHIJKLMNOPQRSTUVWXYZ234567')):
    return ''.join(
        random.choice(chars)
        for _ in range(length)
    )
