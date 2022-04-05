from .client import S3Client
from .version import __author__, __version__, version_info
from .xml import AwsObjectMeta


__all__ = (
    "S3Client", "version_info", "__author__", "__version__", "AwsObjectMeta"
)
