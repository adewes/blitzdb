from .backends.file import Backend as FileBackend
from .document import Document

try:
    from .backends.mongo import Backend as MongoBackend
except ImportError:
    pass

try:
    from .backends.sql import Backend as SqlBackend
except ImportError:
    pass

__version__ = '0.2.12'
