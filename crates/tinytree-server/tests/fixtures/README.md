# Public TLS test fixtures

These synthetic certificates and the **public test-only private key** are for
loopback tests only. Never deploy them or trust this CA outside tests.

P-256/SHA-256 certificates are valid from 2020-01-01 through 2120-01-01. The server
certificate is valid only for the DNS name `localhost`. `wrong-ca.pem` is an
independent CA. No private CA keys or real identities are included. Tests consume
these declared fixtures directly; they do not need certificate tools at runtime.
