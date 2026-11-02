# Spike: internal-stage uploads without a Snowflake driver

Uploads gzip NDJSON files to a Snowflake **internal** (S3-backed) stage using only HTTPS,
then loads them with `COPY INTO`. It keeps one pooled HTTPS client for Snowflake and one
for S3, and caches the stage credentials, so a run opens a handful of TCP connections
however many files it uploads. Drivers that build a new S3 client per `PUT` open at least
one connection per file.

## Running

```sh
export SQLLOAD_SNOWFLAKE_ACCOUNT=myorg-myaccount SQLLOAD_SNOWFLAKE_USER=... \
       SQLLOAD_SNOWFLAKE_PAT=... SQLLOAD_SNOWFLAKE_ROLE=... SQLLOAD_SNOWFLAKE_WAREHOUSE=... \
       SQLLOAD_SNOWFLAKE_DATABASE=MY_DB SQLLOAD_SNOWFLAKE_SCHEMA=MY_SCHEMA \
       SQLLOAD_STAGE=ORDERS_STAGE SQLLOAD_TABLE=ORDERS_RAW
# CREATE STAGE ORDERS_STAGE;  CREATE TABLE ORDERS_RAW (v VARIANT);
cargo run -- run --files 50 --rows 100 --renew-session
```

The run prints a JSON summary with `snowflake_connections` and `s3_connections`, counted
by a tower layer on each reqwest connector, so every new TCP and TLS dial is counted. It
also prints the host-wide `/proc/net/snmp` `ActiveOpens` delta, which other processes
inflate on a busy host. `--fresh-s3-client` builds a new client for each file, for
comparison.

Measured: 50 files of 100 rows each, from a cloud VM to a stage in a nearby region:

| mode                                   | Snowflake conns | S3 conns | stage `PUT`s | upload time |
|----------------------------------------|----------------:|---------:|-------------:|------------:|
| pooled, sequential                     | 1               | 1        | 1            | 2.2 s       |
| pooled, `--concurrency 4`              | 1               | 4        | 1            | 0.7 s       |
| `--fresh-s3-client`                    | 1               | 50       | 1            | 3.7 s       |
| `--fresh-s3-client --stage-creds-ttl 0`| 1               | 50       | 50           | 7.2 s       |

The last row is roughly what a driver does when it runs one `PUT` statement per file.
The load balancer occasionally closed the idle Snowflake connection, which gave 2
Snowflake connections in some runs.

## Protocol notes

1. **Login**: `POST /session/v1/login-request?request_id=…&databaseName=…&schemaName=…&warehouse=…&roleName=…`
   with `{"data": {"CLIENT_APP_ID", "CLIENT_APP_VERSION", "ACCOUNT_NAME", "LOGIN_NAME",
   "AUTHENTICATOR": "PROGRAMMATIC_ACCESS_TOKEN", "TOKEN": <PAT>, "CLIENT_ENVIRONMENT"}}`.
   The response `data` has `token` (session token, `validityInSeconds` ≈ 1 h) and `masterToken`
   (`masterValidityInSeconds` ≈ 4 h). A custom `CLIENT_APP_ID` is accepted, and results come
   back as JSON `rowset`s with no Arrow decoding needed.
2. **Queries**: `POST /queries/v1/query-request?requestId=…` with
   `Authorization: Snowflake Token="<session token>"` and body
   `{sqlText, asyncExec: false, sequenceId, querySubmissionTime}`. Codes `333333`/`333334` mean
   poll `data.getResultUrl` with GET. Code `390112` means the session expired: renew and retry.
3. **Stage info**: `PUT 'file:///<anything>' @stage AUTO_COMPRESS=FALSE SOURCE_COMPRESSION=GZIP`
   must be sent with `Accept: application/json`. `application/snowflake` fails with
   `Unsupported feature 'unsupported_requested_format:snowflake'`. The local path is never read.
   Fields used:
   - `stageInfo.locationType` (`S3`), `location` (`<bucket>/<prefix>/`), `region`, `endPoint`;
   - `stageInfo.creds.{AWS_KEY_ID, AWS_SECRET_KEY, AWS_TOKEN}` (STS credentials);
   - `stageInfo.credExpiryTime` (Unix ms; about 2 h after the `PUT`);
   - `stageInfo.ciphers` (`AES_CBC`), `isClientSideEncrypted` (`true`);
   - `encryptionMaterial.{queryStageMasterKey, queryId, smkId}` (an object for uploads).
   - Also present: `threshold` (200 MiB, the server's multipart hint), `parallel`, `uploadInfo`,
     `presignedUrl` (unused for S3), `useS3RegionalUrl`, `useVirtualUrl`.
4. **Encryption**, matching `encryption_util.py` / `encrypt_util.go`: a random file key the
   size of the master key (32 bytes, AES-256) and a random 16-byte IV. The data is
   AES-CBC encrypted with PKCS#7 padding (always padded). The file key is PKCS#7-padded to
   48 bytes and AES-ECB encrypted with the master key. One master key from one `PUT` works
   for every file uploaded with those credentials.
   `tools/decrypt_with_connector.py` checks a fixture against the official connector's
   `SnowflakeEncryptionUtil.decrypt_file`. Encrypting with the wrong master key makes
   `COPY` fail with `100109 Failed to decrypt. Check file key and master key.`
5. **S3 upload**: a single `PUT https://<bucket>.s3.<region>.amazonaws.com/<prefix><key>`,
   signed with SigV4 and `x-amz-content-sha256: UNSIGNED-PAYLOAD`, the same as the drivers.
   The signed headers are `host`, `x-amz-content-sha256`, `x-amz-date`,
   `x-amz-security-token`, `x-amz-meta-sfc-digest` (base64 SHA-256 of the gzip bytes before
   encryption), `x-amz-meta-x-amz-iv`, `x-amz-meta-x-amz-key` and
   `x-amz-meta-x-amz-matdesc` (`{"queryId":"…","smkId":"…","keySize":"256"}`). The request
   also sends `Content-Type: application/octet-stream`. The drivers use the global
   endpoint unless `useS3RegionalUrl` is set. This spike always uses the regional
   endpoint, which is valid everywhere and avoids redirects.
6. **Load**: `COPY INTO <table> FROM @stage/<prefix>/ FILE_FORMAT = (TYPE = JSON)`. Gzip is
   detected automatically. The row counts are then checked.

## Expiry

- **Stage credentials** are cached until 5 minutes before `credExpiryTime`, or until S3
  returns `ExpiredToken`; the next upload then runs a new `PUT`.
  `--stage-creds-ttl` forces refreshes, for testing.
- **Session token** is renewed with `POST /session/token-request?requestId=…`, using
  `Authorization: Snowflake Token="<master token>"` and body
  `{"oldSessionToken": …, "requestType": "RENEW"}`. That returns a new `sessionToken` and
  `masterToken`. Renewal runs proactively at 90 % of the session lifetime, and on `390112`.
  If renewal fails, for example because the master token expired after about 4 h, the
  client logs in again with the PAT. `--renew-session` exercises the renewal path live.
  `POST /session?delete=true` logs out at the end.

## Snowpipe `insertFiles`

`POST /v1/data/pipes/<db>.<schema>.<pipe>/insertFiles` **does not accept a PAT**. With
`Authorization: Bearer <PAT>`, with or without
`X-Snowflake-Authorization-Token-Type: PROGRAMMATIC_ACCESS_TOKEN`, the response is
`401 394313 … sub (subject) claim in your JWT is empty`. A session token is rejected too.
The endpoint wants a key-pair JWT. The SQL API (`/api/v2/statements`) does accept the same
PAT bearer. This spike therefore loads with `COPY`, and only tries `insertFiles` when
`SQLLOAD_PIPE` and `SQLLOAD_PIPE_TABLE` are set.

## Large files

A single S3 `PUT` accepts up to 5 GB, so files under 100 MB need no multipart upload.
The drivers switch to multipart above their own threshold (the Python connector uses
64 MiB; the server suggests 200 MiB), but only for throughput.
The whole encrypted object is buffered in memory here. A streaming encryptor is
straightforward, since CBC works block by block. A streamed body would need
`UNSIGNED-PAYLOAD` (as here) and a known `Content-Length`, which is plaintext length
rounded up to the next 16-byte block.

## Not covered

Azure and GCS stages (`locationType` `AZURE`/`GCS`), `AES_GCM` (`ciphers`), retries with
backoff, and stages without client-side encryption (`SNOWFLAKE_SSE`).
