import os
import hashlib
import logging


logger = logging.getLogger(__name__)

def ensure_dir(directory):
    os.makedirs(directory, exist_ok=True)

def safe_filename(url):
    """Create a safe and shorter filename from a URL."""
    url_hash = hashlib.md5(url.encode('utf-8')).hexdigest()
    return url_hash + '.json'

