from __future__ import annotations

import os
import tempfile
from pathlib import Path

import certifi
import requests
from requests import Response
from requests.exceptions import SSLError

# Missing intermediate for novaapi.kmdnova.dk
# Served by DigiCert over plain HTTP in the certificate's AIA URL
DIGICERT_INTERMEDIATE_URL = (
    "http://cacerts.digicert.com/"
    "DigiCertGlobalG2TLSRSASHA2562020CA1-1.crt"
)

# Cache folder usable across runs on the same machine
CACHE_DIR = Path(os.getenv("PROGRAMDATA") or tempfile.gettempdir()) / "nova_tls_cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

INTERMEDIATE_DER = CACHE_DIR / "DigiCertGlobalG2TLSRSASHA2562020CA1-1.crt"
INTERMEDIATE_PEM = CACHE_DIR / "DigiCertGlobalG2TLSRSASHA2562020CA1-1.pem"
COMBINED_BUNDLE = CACHE_DIR / "nova_certifi_plus_intermediate.pem"


def _download_intermediate_der() -> Path:
    if INTERMEDIATE_DER.exists() and INTERMEDIATE_DER.stat().st_size > 0:
        return INTERMEDIATE_DER

    r = requests.get(DIGICERT_INTERMEDIATE_URL, timeout=30)
    r.raise_for_status()
    INTERMEDIATE_DER.write_bytes(r.content)
    return INTERMEDIATE_DER


def _convert_der_to_pem(der_bytes: bytes) -> bytes:
    """
    Convert DER certificate bytes to PEM using cryptography.
    Install once:
        pip install cryptography
    """
    from cryptography import x509
    from cryptography.hazmat.primitives import serialization

    cert = x509.load_der_x509_certificate(der_bytes)
    return cert.public_bytes(serialization.Encoding.PEM)


def _ensure_intermediate_pem() -> Path:
    if INTERMEDIATE_PEM.exists() and INTERMEDIATE_PEM.stat().st_size > 0:
        return INTERMEDIATE_PEM

    der_path = _download_intermediate_der()
    pem_bytes = _convert_der_to_pem(der_path.read_bytes())
    INTERMEDIATE_PEM.write_bytes(pem_bytes)
    return INTERMEDIATE_PEM


def ensure_nova_verify_bundle() -> str:
    """
    Returns path to a CA bundle containing:
      - normal certifi roots
      - missing DigiCert intermediate for Nova
    """
    if COMBINED_BUNDLE.exists() and COMBINED_BUNDLE.stat().st_size > 0:
        return str(COMBINED_BUNDLE)

    base_bundle = Path(certifi.where()).read_bytes()
    intermediate_pem = _ensure_intermediate_pem().read_bytes()

    # Avoid duplicate append if recreated
    if intermediate_pem in base_bundle:
        COMBINED_BUNDLE.write_bytes(base_bundle)
    else:
        COMBINED_BUNDLE.write_bytes(base_bundle + b"\n" + intermediate_pem)

    return str(COMBINED_BUNDLE)


def nova_request(
    method: str,
    url: str,
    *,
    headers: dict | None = None,
    json: dict | None = None,
    params: dict | None = None,
    timeout: int = 60,
    session: requests.Session | None = None,
    **kwargs,
) -> Response:
    """
    Tries normal TLS first.
    If SSL verification fails, retries with an auto-patched CA bundle.
    """
    client = session or requests.Session()

    try:
        return client.request(
            method=method,
            url=url,
            headers=headers,
            json=json,
            params=params,
            timeout=timeout,
            verify=certifi.where(),
            **kwargs,
        )
    except SSLError as e:
        msg = str(e)
        ssl_chain_error = any(
            text in msg for text in (
                "CERTIFICATE_VERIFY_FAILED",
                "unable to get local issuer certificate",
                "unable to verify the first certificate",
            )
        )
        if not ssl_chain_error:
            raise

        verify_bundle = ensure_nova_verify_bundle()
        return client.request(
            method=method,
            url=url,
            headers=headers,
            json=json,
            params=params,
            timeout=timeout,
            verify=verify_bundle,
            **kwargs,
        )