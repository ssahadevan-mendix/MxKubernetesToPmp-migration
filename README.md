# mx2pmp — Mendix on Kubernetes (connected) → Private Mendix Platform

`mx2pmp.py` migrates Mendix applications from **Mendix on Kubernetes, connected architecture**
(Mendix Operator in your cluster, environments managed from the Mendix Portal) to
**Private Mendix Platform (PMP) 2.8**. It is a single Python 3.9+ file with no third-party
packages. External tools: `kubectl`, `git`, `pg_dump`/`pg_restore`, `aws` CLI.

The tool reads the source cluster directly (MendixApp custom resources, StorageInstances,
Secrets), so it does not depend on the Portal or the Private Cloud API. It drives PMP through
its REST API (`deployservice/v1`, `buildservice/v1`) with HTTP Basic auth (PMP user + PAT).

## What migrates, and how

| Item | Source | Mechanism | Target |
|---|---|---|---|
| Model / source code | Team Server Git (`git.api.mendix.com/<projectId>.git`) | `mirror-repo` (`git clone --mirror` + `push --mirror`) | Your Git provider registered in PMP |
| Runtime settings, constants, scheduled events, env vars, replicas, app URL, JVM opts | `MendixApp` CR `spec.runtime.*` + rendered Deployment env | `export` → `plan` → `pmp apply-config` | PMP environment configuration |
| Environment (cluster, namespace, plans, purpose, license) | CR + `defaults`/`plan_map` in config | `plan` → `pmp create-env` | New PMP environment |
| Deployment package | Rebuilt by PMP from Git (source image is **not** reused) | `pmp build` → `pmp deploy` | PMP package |
| Database | PostgreSQL referenced by the environment's StorageInstance secret | `migrate-db` (`pg_dump -Fc` → `pg_restore --clean`) | PMP environment database |
| Files | S3-compatible bucket from the storage secret | `migrate-files` (`aws s3 sync` or re-keyed copy) | PMP environment bucket |
| DNS / TLS | Ingress of source cluster | manual at cut-over | PMP ingress |

Not migrated automatically: non-PostgreSQL databases, Azure Blob storage (use `azcopy`),
Portal-side settings such as team members, alerts, backups schedule, and custom domains/certs.

## Credentials (environment variables only)

```
KUBECONFIG / kube context      read access to the SOURCE cluster
TEAMSERVER_PAT                 Mendix PAT with Team Server read scope
TARGET_GIT_USER / TARGET_GIT_TOKEN   push rights on the target Git provider
PMP_USER / PMP_PAT             PMP service user + Personal Access Token
SOURCE_DB_URL / TARGET_DB_URL  postgres://user:pass@host:5432/db  (see `db-url`)
```

Never put credentials in `mx2pmp.json`. `export --include-secrets` writes secret values
to `mx2pmp-work/export/...` with mode 0600; delete those files after the migration.

## Walkthrough (per application)

```bash
python3 mx2pmp.py init                       # writes mx2pmp.json template
kubectl config use-context <source-cluster>
python3 mx2pmp.py inventory --add-to-config  # discovers MendixApp CRs, fills apps[]
#   edit mx2pmp.json: pmp.base_url, defaults.*, plan_map, mx_project_id per app

python3 mx2pmp.py export orders              # CR + StorageInstances + secret key names
python3 mx2pmp.py plan   orders              # PMP payloads in mx2pmp-work/plan/orders/*.json
#   review plan files; WARN lines list secrets, runtime version, memory, appUrl

python3 mx2pmp.py mirror-repo orders --dry-run && python3 mx2pmp.py mirror-repo orders
#   in PMP: create the app from the mirrored repo, copy its id into pmp_app_id

python3 mx2pmp.py pmp create-env   orders --mode Test --dry-run
python3 mx2pmp.py pmp create-env   orders --mode Test
python3 mx2pmp.py pmp apply-config orders --mode Test
python3 mx2pmp.py pmp build        orders --branch main --version 1.4.0
python3 mx2pmp.py pmp deploy       orders --mode Test          # deploy package
python3 mx2pmp.py pmp deploy       orders --mode Test --action Stop

# cut-over: stop the source environment in the Mendix Portal (or scale the CR to 0)
eval "$(python3 mx2pmp.py db-url source -n mx-apps --secret orders-test-database)"
eval "$(python3 mx2pmp.py db-url target -n mendix-apps --secret <pmp-env-db-secret> --context <pmp-cluster>)"
python3 mx2pmp.py migrate-db    orders --mode Test --execute --yes
python3 mx2pmp.py migrate-files orders --mode Test             # dry-run
python3 mx2pmp.py migrate-files orders --mode Test --execute
python3 mx2pmp.py pmp deploy    orders --mode Test --action Start
python3 mx2pmp.py validate      orders --mode Test
python3 mx2pmp.py report
```

Repeat for Acceptance and Production. State is kept in `mx2pmp-work/state.json`; every
command is idempotent or guarded (`--dry-run`, `--execute`, `--yes`, `--force`).

## Configuration notes

* `defaults.constants_mode`: `env` emits one `MX_<Module>_<Constant>` variable per constant
  (Operator convention); `runtime` emits a single `MicroflowConstants` JSON runtime setting.
* `defaults.plan_map` maps source Operator plan names to PMP storage/database plan names.
* `defaults.file_key_layout`: `flat` when both storage plans store objects under the same key;
  `nested` re-keys to `aa/bb/<uuid>` and writes a manifest.
* Per-environment overrides: `app_url`, `replicas`, `display_name`, `cluster_name`,
  `namespace`, `storage_plan`, `database_plan`, `core_resource_size`, `licenseId`, `licenseKey`.
* `plan --overrides file.json` replaces constant values (e.g. new endpoints for the PMP side).

## Caveats to verify in your environment

* MendixApp CR field names differ slightly across Mendix Operator versions. The exporter reads
  several aliases (`appURL`/`appUrl`, `runtime.microflowConstants` as JSON string or object).
  Check `mx2pmp-work/export/<app>/<Mode>.json` once and adjust `build_plan()` if a field is missed.
* PMP REST responses are untyped. Id extraction and status polling use tolerant matching; if
  `pmp build`/`deploy` time out while the portal shows success, adjust the regexes in
  `_status_of` / `_env_status_from_dashboard`.
* The PMP build must use a base image that supports the source runtime version (shown as a
  WARN in `plan`).
* Stop the source environment before `migrate-db` for a consistent copy. The Mendix runtime
  performs schema synchronisation on the first Start in PMP; keep the source until validated.
* `migrate-db` is PostgreSQL only. For managed databases (RDS) run the tool from a host that can
  reach both databases, or inside a `kubectl run` pod with the postgres client image.
