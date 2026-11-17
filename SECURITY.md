# Security Policy

## Supported versions

Security fixes are made on the latest release. Until v1.0.0, only the most recent minor version is supported.

## Reporting a vulnerability

Please report vulnerabilities privately through
[GitHub security advisories](https://github.com/ShawineWu/go-xxljob/security/advisories/new) rather than in a
public issue. Include the affected version, the admin version, and steps or a test that reproduce the problem.

You can expect an acknowledgement within a week. Once a fix is available, it will be released and the advisory
published, crediting you unless you prefer otherwise.

## Deployment notes

- Set `AccessToken`. Without it, anything that can reach the executor port can trigger or kill jobs, and the
  executor logs a warning at startup.
- Do not expose the executor port publicly; only the admin needs to reach it.
- The executor only runs BEAN jobs compiled into your binary. GLUE scripts sent by the admin are rejected.
