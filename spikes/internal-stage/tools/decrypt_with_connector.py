"""Decrypt a fixture written by `spike-internal-stage encrypt-fixture <dir>` with the
official Python connector's routine and compare it to the plaintext.

Run with an interpreter that has snowflake-connector-python installed:

    spike-internal-stage encrypt-fixture /tmp/fixture
    python tools/decrypt_with_connector.py /tmp/fixture
"""

import json
import pathlib
import sys
import tempfile

from snowflake.connector.constants import EncryptionMetadata
from snowflake.connector.encryption_util import SnowflakeEncryptionUtil
from snowflake.connector.storage_client import SnowflakeFileEncryptionMaterial


def main(directory: str) -> None:
    d = pathlib.Path(directory)
    meta = json.loads((d / "meta.json").read_text())
    material = SnowflakeFileEncryptionMaterial(
        query_stage_master_key=meta["query_stage_master_key"],
        query_id=meta["query_id"],
        smk_id=meta["smk_id"],
    )
    metadata = EncryptionMetadata(key=meta["key"], iv=meta["iv"], matdesc=meta["matdesc"])
    with tempfile.TemporaryDirectory() as tmp:
        out = SnowflakeEncryptionUtil.decrypt_file(
            metadata, material, str(d / "encrypted.bin"), tmp_dir=tmp
        )
        decrypted = pathlib.Path(out).read_bytes()
    plain = (d / "plain.bin").read_bytes()
    if decrypted != plain:
        sys.exit(f"MISMATCH: decrypted {len(decrypted)} bytes, expected {len(plain)}")
    matdesc = json.loads(meta["matdesc"])
    assert matdesc == {
        "queryId": meta["query_id"],
        "smkId": str(meta["smk_id"]),
        "keySize": "256",
    }, matdesc
    print(f"OK: connector decrypted {len(plain)} bytes identically")


if __name__ == "__main__":
    main(sys.argv[1])
