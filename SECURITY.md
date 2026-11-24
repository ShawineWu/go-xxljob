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

- Set `AccessToken`. Without it, anything that can reach the executor port can trigger and kill jobs and read their
  execution logs (which include job parameters and error messages); the executor logs a warning at startup.
- Do not expose the executor port publicly; only the admin needs to reach it.
- The executor only runs BEAN jobs compiled into your binary. GLUE scripts sent by the admin are rejected.
- `CallbackDir` holds run results (log IDs and result messages); the executor creates its files readable by its own
  user only. Keep the directory private like any application data, and use one directory per executor process:
  whoever can write to it can report results for any run. The executor reads only regular files from it, never
  blocks on special files, and caps what it loads (512 MiB of files, lines of 1 MiB, messages of 50000 characters).
- `FileLogStore` removes only its own log files (`{logId}.log` in date directories) when cleaning up; give it a
  directory of its own anyway.
