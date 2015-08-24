from .document import Document
from .backends.file import Backend as FileBackend
try:
    from .backends.mongo import Backend as MongoBackend
except ImportError:
    pass

try:
    from .backends.sql import Backend as SqlBackend
except ImportError:
    pass
    
__version__ = '0.2.12'
