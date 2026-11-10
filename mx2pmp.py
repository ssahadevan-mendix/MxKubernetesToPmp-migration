#!/usr/bin/env python3
"""
mx2pmp - migrate Mendix applications from Mendix on Kubernetes (connected architecture,
Mendix Operator managed from the Mendix Portal) to Private Mendix Platform (PMP).

Standard library only (Python 3.9+). External tools: kubectl, git, pg_dump/pg_restore, aws CLI.

Source of truth on the source side is the cluster, not the Portal:
  * MendixApp custom resources (privatecloud.mendix.com) hold runtime config, constants,
    scheduled events, replicas, app URL, runtime version and the deployed image.
  * StorageInstance resources + Secrets hold database and file-storage connection details.

Credentials come from environment variables only, never from the config file:
  KUBECONFIG / current context     access to the SOURCE cluster (kubectl)
  TEAMSERVER_PAT                   Mendix Team Server personal access token (git clone)
  TARGET_GIT_USER, TARGET_GIT_TOKEN  credentials for the organisation Git provider (git push)
  PMP_USER, PMP_PAT                PMP service user + Personal Access Token (HTTP Basic)
  SOURCE_DB_URL, TARGET_DB_URL     postgres://user:pass@host:5432/db (see 'db-url' helper)

Workflow (per app):
  init -> inventory --add-to-config -> export -> plan -> mirror-repo ->
  pmp create-env -> pmp apply-config -> pmp build -> pmp deploy (stopped) ->
  migrate-db -> migrate-files -> pmp deploy --action Start -> validate -> report
"""
import argparse
import base64
import csv
import datetime as dt
import json
import os
import re
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

VERSION = "0.2.0"
STATE_FILE = "state.json"
MXAPP_KINDS = ("mendixapps.privatecloud.mendix.com", "mendixapps")

# --------------------------------------------------------------------------- utilities


def log(msg, level="INFO"):
    ts = dt.datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {level:<5} {msg}", file=sys.stderr if level in ("WARN", "ERROR") else sys.stdout, flush=True)


def die(msg, code=2):
    log(msg, "ERROR")
    sys.exit(code)


def env(name, required=True):
    v = os.environ.get(name)
    if required and not v:
        die(f"environment variable {name} is required")
    return v


def read_json(path, default=None):
    if not os.path.exists(path):
        if default is not None:
            return default
        die(f"file not found: {path}")
    with open(path) as f:
        return json.load(f)


def write_json(path, data, mode=None):
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    if mode is not None:
        os.chmod(path, mode)
    log(f"wrote {path}")


def which(tool):
    if not shutil.which(tool):
        die(f"required tool not found on PATH: {tool}")


def run(cmd, check=True, capture=False, cwd=None, redact=(), stdin=None, stdout=None):
    shown = " ".join(cmd)
    for r in redact:
        if r:
            shown = shown.replace(r, "***")
    log(f"$ {shown}")
    res = subprocess.run(cmd, check=False, cwd=cwd, text=True, stdin=stdin,
                         stdout=stdout if stdout else (subprocess.PIPE if capture else None),
                         stderr=subprocess.PIPE if capture else None)
    if check and res.returncode != 0:
        if capture and res.stderr:
            log(res.stderr[-3000:], "ERROR")
        die(f"command failed ({res.returncode}): {shown}")
    return res


def kubectl_json(args, context=None):
    which("kubectl")
    cmd = ["kubectl"] + (["--context", context] if context else []) + args + ["-o", "json"]
    res = run(cmd, capture=True)
    return json.loads(res.stdout) if res.stdout.strip() else {}


def dig(obj, *paths, default=None):
    """Return first value found at any of the dotted paths."""
    for p in paths:
        cur = obj
        ok = True
        for part in p.split("."):
            if isinstance(cur, dict) and part in cur:
                cur = cur[part]
            else:
                ok = False
                break
        if ok and cur is not None:
            return cur
    return default


def b64d(s):
    return base64.b64decode(s).decode(errors="replace")


# --------------------------------------------------------------------------- HTTP client (PMP)


class HttpError(Exception):
    def __init__(self, status, method, url, text):
        super().__init__(f"HTTP {status} {method} {url}: {text[:500]}")
        self.status = status
        self.text = text


class Http:
    def __init__(self, base_url, headers=None, verify_tls=True, timeout=120):
        self.base_url = base_url.rstrip("/")
        self.headers = headers or {}
        self.timeout = timeout
        self.ctx = None
        if not verify_tls:
            import ssl
            self.ctx = ssl._create_unverified_context()

    def _req(self, method, path, body=None, raw=False, absolute=False):
        url = path if absolute else self.base_url + path
        data = None
        hdrs = dict(self.headers)
        hdrs["Accept"] = "application/json, */*"
        if body is not None:
            data = json.dumps(body).encode()
            hdrs["Content-Type"] = "application/json"
        req = urllib.request.Request(url, data=data, method=method, headers=hdrs)
        try:
            with urllib.request.urlopen(req, timeout=self.timeout, context=self.ctx) as resp:
                content = resp.read()
                if raw or not content:
                    return resp.status, content, dict(resp.headers)
                if "json" in resp.headers.get("Content-Type", "") or content[:1] in (b"{", b"["):
                    try:
                        return resp.status, json.loads(content), dict(resp.headers)
                    except json.JSONDecodeError:
                        pass
                return resp.status, content.decode(errors="replace"), dict(resp.headers)
        except urllib.error.HTTPError as e:
            raise HttpError(e.code, method, url, e.read().decode(errors="replace")) from None

    def get(self, path, **kw):
        return self._req("GET", path, **kw)[1]

    def post(self, path, body=None, **kw):
        return self._req("POST", path, body, **kw)[1]

    def patch(self, path, body=None, **kw):
        return self._req("PATCH", path, body, **kw)[1]


# --------------------------------------------------------------------------- config / state

CONFIG_TEMPLATE = {
    "workdir": "./mx2pmp-work",
    "source": {
        "kube_context": "",
        "_comment": "kube_context empty = current kubectl context of the SOURCE cluster (Mendix on Kubernetes, connected mode)",
        "teamserver_base_url": "https://git.api.mendix.com"
    },
    "pmp": {
        "base_url": "https://<pmp-host>",
        "verify_tls": True,
        "paths": {
            "environments": "/deployservice/v1/apps/{appId}/environments",
            "configurations": "/deployservice/v1/apps/{appId}/environments/{envId}/configurations",
            "env_request": "/deployservice/v1/apps/{appId}/environments/{envId}/request",
            "dashboard": "/deployservice/v1/apps/{appId}/dashboard",
            "packages": "/buildservice/v1/apps/{appId}/packages"
        }
    },
    "git": {"target_base_url": "https://gitlab.example.com/mendix-apps"},
    "defaults": {
        "cluster_name": "<pmp-cluster-name>",
        "namespace": "<pmp-namespace>",
        "storage_plan": "<pmp-storage-plan>",
        "database_plan": "<pmp-database-plan>",
        "plan_map": {"_comment": "optional: source Operator plan name -> PMP plan name", "<source-plan>": "<pmp-plan>"},
        "core_resource_size": "Small",
        "constants_mode": "env",
        "_constants_mode": "env = constants as MX_<Module>_<Constant> environment variables; runtime = one MicroflowConstants runtime setting",
        "file_key_layout": "flat",
        "_file_key_layout": "flat = objects copied with the same key; nested = <uuid[0:2]>/<uuid[2:4]>/<uuid> (match the target Operator storage layout)",
        "target_file_prefix": ""
    },
    "apps": [
        {
            "name": "example-app",
            "mx_project_id": "<team-server-project-id>",
            "pmp_app_id": "",
            "target_repo": "https://gitlab.example.com/mendix-apps/example-app.git",
            "environments": {
                "Test": {"source_namespace": "<ns>", "source_cr": "<mendixapp-cr-name>", "purpose": "Test",
                         "pmp_env_id": "", "display_name": "", "app_url": "", "replicas": None,
                         "source_bucket": "", "target_bucket": ""}
            }
        }
    ]
}


class Ctx:
    def __init__(self, args):
        self.args = args
        self.config_path = args.config
        self.config = read_json(self.config_path) if os.path.exists(self.config_path) else None
        self.workdir = (self.config or {}).get("workdir", "./mx2pmp-work")
        self.state_path = os.path.join(self.workdir, STATE_FILE)
        self.state = read_json(self.state_path, default={})

    def require_config(self):
        if not self.config:
            die(f"config not found: {self.config_path} (run 'init' first)")
        return self.config

    @property
    def kctx(self):
        return (self.require_config().get("source") or {}).get("kube_context") or None

    def app(self, name):
        for a in self.require_config().get("apps", []):
            if a.get("name") == name:
                return a
        die(f"app '{name}' not in config; run 'inventory --add-to-config' or add it manually")

    def app_env(self, app, mode):
        envs = app.get("environments", {})
        if mode not in envs:
            die(f"environment '{mode}' not defined for app {app['name']} in config (have: {', '.join(envs)})")
        return envs[mode]

    def save_config(self):
        write_json(self.config_path, self.config)

    def mark(self, app_name, step, **data):
        s = self.state.setdefault(app_name, {})
        s[step] = {"at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"), **data}
        write_json(self.state_path, self.state)

    def path(self, *parts):
        p = os.path.join(self.workdir, *parts)
        os.makedirs(os.path.dirname(p) if "." in parts[-1] else p, exist_ok=True)
        return p

    def pmp(self):
        cfg = self.require_config()["pmp"]
        token = base64.b64encode(f"{env('PMP_USER')}:{env('PMP_PAT')}".encode()).decode()
        return Http(cfg["base_url"], {"Authorization": f"Basic {token}"}, verify_tls=cfg.get("verify_tls", True))


def cmd_init(ctx):
    if os.path.exists(ctx.config_path) and not ctx.args.force:
        die(f"{ctx.config_path} exists; use --force to overwrite")
    write_json(ctx.config_path, CONFIG_TEMPLATE)
    log("next: point kubectl at the source cluster, then run 'inventory --add-to-config' and edit the PMP defaults")


# --------------------------------------------------------------------------- source: Kubernetes


def get_mendixapps(ctx, namespace=None):
    last = None
    for kind in MXAPP_KINDS:
        try:
            args = ["get", kind] + (["-n", namespace] if namespace else ["-A"])
            return kubectl_json(args, ctx.kctx).get("items", [])
        except SystemExit as e:  # run() died
            last = e
    raise last


def summarize_cr(cr):
    spec, status, meta = cr.get("spec", {}), cr.get("status", {}), cr.get("metadata", {})
    return {
        "namespace": meta.get("namespace"), "cr": meta.get("name"),
        "appName": spec.get("appName") or meta.get("name"),
        "appURL": spec.get("appURL") or spec.get("appUrl"),
        "replicas": spec.get("replicas"),
        "mendixRuntimeVersion": spec.get("mendixRuntimeVersion") or dig(status, "runtimeVersion"),
        "sourceURL": spec.get("sourceURL") or spec.get("sourceUrl"),
        "sourceVersion": spec.get("sourceVersion"),
        "dtapMode": dig(spec, "runtime.dtapMode"),
        "databasePlan": dig(spec, "database.plan", "database.storagePlan", "databasePlan"),
        "storagePlan": dig(spec, "storage.plan", "storage.storagePlan", "storagePlan"),
        "status": dig(status, "status", "phase", "runtimeStatus") or "",
        "portalEnvironmentId": dig(meta, "labels.privatecloud.mendix.com/environment-id", "labels.privatecloud\\.mendix\\.com/environment-id")
        or next((v for k, v in (meta.get("labels") or {}).items() if "environment" in k and "id" in k), None),
        "portalAppId": next((v for k, v in (meta.get("labels") or {}).items() if k.endswith("app-id") or k.endswith("appid")), None),
    }


def cmd_inventory(ctx):
    """Discover MendixApp CRs in the source cluster and write inventory.json + inventory.csv."""
    items = get_mendixapps(ctx, ctx.args.namespace)
    rows = [summarize_cr(cr) for cr in items]
    write_json(ctx.path("inventory.json"), rows)
    csv_path = ctx.path("inventory.csv")
    with open(csv_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()) if rows else ["namespace"])
        w.writeheader()
        w.writerows(rows)
    log(f"{len(rows)} Mendix environments in {len({r['namespace'] for r in rows})} namespaces -> {csv_path}")
    for r in rows:
        print(f"  {r['namespace']:<24} {r['cr']:<32} {str(r['dtapMode'] or ''):<3} v{r['mendixRuntimeVersion'] or '?':<10} {r['status']}")
    if not ctx.args.add_to_config:
        return
    cfg = ctx.require_config()
    apps = [a for a in cfg.get("apps", []) if a.get("name") != "example-app"]
    known = {(e.get("source_namespace"), e.get("source_cr")) for a in apps for e in a.get("environments", {}).values()}
    mode_name = {"D": "Development", "T": "Test", "A": "Acceptance", "P": "Production"}
    added = 0
    for r in rows:
        if (r["namespace"], r["cr"]) in known:
            continue
        slug = re.sub(r"[^a-z0-9-]+", "-", (r["appName"] or r["cr"]).lower()).strip("-")
        app = next((a for a in apps if a["name"] == slug), None)
        if not app:
            app = {"name": slug, "mx_project_id": "", "pmp_app_id": "",
                   "target_repo": f"{cfg['git']['target_base_url'].rstrip('/')}/{slug}.git", "environments": {}}
            apps.append(app)
        purpose = mode_name.get(str(r["dtapMode"] or "").upper(), "Test")
        key = purpose if purpose not in app["environments"] else f"{purpose}-{r['cr']}"
        app["environments"][key] = {"source_namespace": r["namespace"], "source_cr": r["cr"], "purpose": purpose,
                                    "pmp_env_id": "", "display_name": "", "app_url": "", "replicas": r["replicas"],
                                    "source_bucket": "", "target_bucket": ""}
        added += 1
    cfg["apps"] = apps
    ctx.save_config()
    log(f"added {added} environments across {len(apps)} apps to {ctx.config_path}; fill mx_project_id (Team Server project) per app")


def _referenced_secret_names(spec):
    names = set()
    txt = json.dumps(spec)
    for m in re.finditer(r'"(?:secretName|secretRef|name)"\s*:\s*"([^"]+)"', txt):
        names.add(m.group(1))
    return names


def cmd_export(ctx):
    """Export MendixApp CR, StorageInstances, env-var sources and (optionally) secrets for each environment."""
    app = ctx.app(ctx.args.app)
    modes = [ctx.args.mode] if ctx.args.mode else list(app.get("environments", {}).keys())
    for mode in modes:
        e = ctx.app_env(app, mode)
        ns, cr_name = e.get("source_namespace"), e.get("source_cr")
        if not ns or not cr_name or ns.startswith("<"):
            die(f"{mode}: source_namespace/source_cr not set in config")
        cr = None
        for kind in MXAPP_KINDS:
            try:
                cr = kubectl_json(["get", kind, cr_name, "-n", ns], ctx.kctx)
                break
            except SystemExit:
                continue
        if not cr:
            die(f"MendixApp {ns}/{cr_name} not found")
        out = {"exported_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"), "mode": mode,
               "namespace": ns, "cr": cr, "summary": summarize_cr(cr), "storage_instances": [], "secrets": {}}
        try:
            si = kubectl_json(["get", "storageinstances.privatecloud.mendix.com", "-n", ns], ctx.kctx).get("items", [])
            out["storage_instances"] = [s for s in si if cr_name in s["metadata"]["name"]
                                        or dig(s, "metadata.labels.privatecloud\\.mendix\\.com/app") == cr_name]
        except SystemExit:
            log("could not list StorageInstances (CRD name may differ); continuing", "WARN")
        # secrets: referenced by spec + named after the CR
        try:
            all_secrets = kubectl_json(["get", "secrets", "-n", ns], ctx.kctx).get("items", [])
        except SystemExit:
            all_secrets = []
        wanted = _referenced_secret_names(cr.get("spec", {}))
        for s in all_secrets:
            n = s["metadata"]["name"]
            if n in wanted or n.startswith(cr_name):
                out["secrets"][n] = {"keys": sorted((s.get("data") or {}).keys()), "type": s.get("type")}
                if ctx.args.include_secrets:
                    out["secrets"][n]["data"] = {k: b64d(v) for k, v in (s.get("data") or {}).items()}
        # deployment env (operator-rendered) for completeness
        try:
            deps = kubectl_json(["get", "deployments", "-n", ns, "-l", f"privatecloud.mendix.com/app={cr_name}"], ctx.kctx).get("items", [])
            if not deps:
                deps = [d for d in kubectl_json(["get", "deployments", "-n", ns], ctx.kctx).get("items", []) if cr_name in d["metadata"]["name"]]
            out["rendered_env"] = [{"container": c["name"], "env": c.get("env", []), "resources": c.get("resources", {})}
                                   for d in deps for c in dig(d, "spec.template.spec.containers", default=[])]
        except SystemExit:
            out["rendered_env"] = []
        path = ctx.path("export", app["name"], f"{mode}.json")
        write_json(path, out, mode=0o600 if ctx.args.include_secrets else None)
        ctx.mark(app["name"], f"export:{mode}", file=path, runtime=out["summary"]["mendixRuntimeVersion"], image=out["summary"]["sourceURL"])
    if ctx.args.include_secrets:
        log("export files contain secret values (chmod 600). Do not share them.", "WARN")


# --------------------------------------------------------------------------- plan (CR -> PMP payloads)


def _const_env_name(name):
    return "MX_" + re.sub(r"[^A-Za-z0-9_]", "_", name)


def _as_dict(v):
    if isinstance(v, dict):
        return v
    if isinstance(v, str) and v.strip():
        try:
            d = json.loads(v)
            return d if isinstance(d, dict) else {}
        except json.JSONDecodeError:
            return {}
    return {}


def _env_list(v):
    if isinstance(v, list):
        return [(x.get("name"), x.get("value")) for x in v if isinstance(x, dict) and x.get("name") and "valueFrom" not in x]
    if isinstance(v, dict):
        return list(v.items())
    return []


def build_plan(export, app, env_cfg, defaults, overrides=None):
    """Translate a MendixApp CR export into PMP request payloads. Pure function (testable)."""
    overrides = overrides or {}
    cr = export.get("cr", {})
    spec = cr.get("spec", {})
    rt = spec.get("runtime", {}) or {}
    summary = export.get("summary") or summarize_cr(cr)
    purpose = env_cfg.get("purpose") or {"D": "Development", "T": "Test", "A": "Acceptance", "P": "Production"}.get(str(rt.get("dtapMode", "T")).upper(), "Test")
    display = env_cfg.get("display_name") or f"{app['name']}-{purpose.lower()}"
    cmode = defaults.get("constants_mode", "env")
    notes = []

    # constants
    constants = _as_dict(rt.get("microflowConstants"))
    constants.update({k: v for k, v in overrides.items() if k in constants or "." in k})
    env_vars, runtime_settings = [], []
    if cmode == "env":
        env_vars += [{"name": _const_env_name(k), "newValue": "" if v is None else str(v)} for k, v in constants.items()]
    elif constants:
        runtime_settings.append({"name": "MicroflowConstants", "newValue": json.dumps(constants)})

    # runtime custom configuration (Mendix runtime settings)
    for k, v in _as_dict(rt.get("customConfiguration")).items():
        runtime_settings.append({"name": k, "newValue": v if isinstance(v, str) else json.dumps(v)})

    # scheduled events
    see = rt.get("scheduledEventExecution")
    if see:
        runtime_settings.append({"name": "ScheduledEventExecution", "newValue": str(see).upper()})
        mse = rt.get("myScheduledEvents")
        if mse:
            runtime_settings.append({"name": "MyScheduledEvents", "newValue": ",".join(mse) if isinstance(mse, list) else str(mse)})

    # dedupe runtime settings: explicit CR fields win over customConfiguration entries
    dedup = {}
    for r in runtime_settings:
        dedup[r["name"]] = r["newValue"]
    runtime_settings = [{"name": k, "newValue": v} for k, v in dedup.items()]

    # log levels
    for k, v in _as_dict(rt.get("logLevels")).items():
        notes.append(f"log level {k}={v} set in source; configure via PMP portal if required")

    # plain environment variables (operator spec or rendered deployment, excluding operator-managed ones)
    skip = re.compile(r"^(MX_|MXRUNTIME_|M2EE_|JAVA_|K8S_|KUBERNETES_|CF_|MENDIX_|POD_|HOSTNAME|PATH)", re.I)
    seen = {e["name"] for e in env_vars}
    for name, value in _env_list(spec.get("environmentVariables") or rt.get("environmentVariables")):
        if name not in seen and value is not None:
            env_vars.append({"name": name, "newValue": str(value)})
            seen.add(name)
    for c in export.get("rendered_env") or []:
        for name, value in _env_list(c.get("env")):
            if name not in seen and value is not None and not skip.match(name):
                env_vars.append({"name": name, "newValue": str(value)})
                seen.add(name)

    # jvm options
    jvm = rt.get("customJvmOptions") or rt.get("jvmOptions")
    jvm = " ".join(jvm) if isinstance(jvm, list) else jvm

    secretish = [c for c in constants if re.search(r"(pass|secret|token|key|credential)", c, re.I)]
    if secretish:
        notes.append(f"constants that look like secrets (store via PMP secret handling): {', '.join(secretish)}")
    if summary.get("mendixRuntimeVersion"):
        notes.append(f"source runtime {summary['mendixRuntimeVersion']}; PMP build base images must support it")
    if summary.get("sourceURL"):
        notes.append(f"source image {summary['sourceURL']} (PMP rebuilds from Git; image is not reused)")
    if spec.get("runtimeLicense") or rt.get("licenseId"):
        notes.append("source has a runtime license; assign a license in PMP (licenseId/licenseKey in environment_request)")
    plan_map = defaults.get("plan_map") or {}
    db_plan = plan_map.get(summary.get("databasePlan") or "", env_cfg.get("database_plan") or defaults.get("database_plan"))
    st_plan = plan_map.get(summary.get("storagePlan") or "", env_cfg.get("storage_plan") or defaults.get("storage_plan"))

    # resource size hint
    res = {}
    for c in export.get("rendered_env") or []:
        res = c.get("resources") or res
    mem = dig(res, "limits.memory", "requests.memory", default="")
    size = env_cfg.get("core_resource_size") or defaults.get("core_resource_size", "Small")
    if mem:
        notes.append(f"source memory {mem}; PMP coreResourceSize set to {size} (adjust)")

    env_request = {
        "displayName": display,
        "clusterName": env_cfg.get("cluster_name") or defaults.get("cluster_name"),
        "namespace": env_cfg.get("namespace") or defaults.get("namespace"),
        "storagePlanName": st_plan,
        "databasePlanName": db_plan,
        "deploymentPurpose": purpose,
        "coreResourceSize": size,
    }
    for k in ("licenseId", "licenseKey", "coreResourcePlan"):
        if env_cfg.get(k):
            env_request[k] = env_cfg[k]
    configuration = {
        "displayName": display,
        "dtapMode": purpose[0],
        "replicas": int(env_cfg.get("replicas") or spec.get("replicas") or 1),
        "CustomEnvironmentVariables": env_vars,
        "CustomRuntimeSettings": runtime_settings,
    }
    app_url = env_cfg.get("app_url") or summary.get("appURL")
    if app_url:
        configuration["appUrl"] = app_url
        if not env_cfg.get("app_url"):
            notes.append(f"appUrl copied from source ({app_url}); DNS must be re-pointed to the PMP ingress at cut-over")
    if jvm:
        configuration["CustomJVMOptions"] = {"newValue": jvm}
    return {"source": {"namespace": export.get("namespace"), "cr": summary.get("cr"), "appURL": summary.get("appURL"),
                       "runtime": summary.get("mendixRuntimeVersion"), "image": summary.get("sourceURL"),
                       "databasePlan": summary.get("databasePlan"), "storagePlan": summary.get("storagePlan")},
            "environment_request": env_request, "configuration": configuration, "notes": notes}


def cmd_plan(ctx):
    app = ctx.app(ctx.args.app)
    cfg = ctx.require_config()
    modes = [ctx.args.mode] if ctx.args.mode else list(app.get("environments", {}).keys())
    overrides = read_json(ctx.args.overrides) if ctx.args.overrides else {}
    for mode in modes:
        exp_path = ctx.path("export", app["name"], f"{mode}.json")
        if not os.path.exists(exp_path):
            log(f"no export for {mode}; run export first", "WARN")
            continue
        plan = build_plan(read_json(exp_path), app, ctx.app_env(app, mode), cfg.get("defaults", {}), overrides.get(mode) or overrides)
        write_json(ctx.path("plan", app["name"], f"{mode}.json"), plan)
        for n in plan["notes"]:
            log(f"{mode}: {n}", "WARN")
        log(f"{mode}: {len(plan['configuration']['CustomEnvironmentVariables'])} env vars, {len(plan['configuration']['CustomRuntimeSettings'])} runtime settings")
    log("review plan/*.json, replace <placeholders>, then run 'pmp create-env'")


# --------------------------------------------------------------------------- source code


def cmd_mirror_repo(ctx):
    """Mirror the Team Server Git repository into the organisation Git provider used by PMP."""
    which("git")
    app = ctx.app(ctx.args.app)
    cfg = ctx.require_config()
    project_id = app.get("mx_project_id")
    if not project_id or project_id.startswith("<"):
        die("mx_project_id missing (Team Server project id from the Mendix Portal, Settings > General)")
    ts_pat = env("TEAMSERVER_PAT")
    tgt_user, tgt_token = env("TARGET_GIT_USER"), env("TARGET_GIT_TOKEN")
    src = f"{cfg['source']['teamserver_base_url'].rstrip('/')}/{project_id}.git"
    src_auth = src.replace("https://", f"https://pat:{urllib.parse.quote(ts_pat, safe='')}@")
    dst = app["target_repo"]
    dst_auth = dst.replace("https://", f"https://{urllib.parse.quote(tgt_user, safe='')}:{urllib.parse.quote(tgt_token, safe='')}@")
    mirror_dir = ctx.path("repos", f"{app['name']}.git")
    if os.path.isdir(os.path.join(mirror_dir, "objects")):
        run(["git", "-C", mirror_dir, "remote", "set-url", "origin", src_auth], redact=(ts_pat,))
        run(["git", "-C", mirror_dir, "fetch", "--prune", "origin"], redact=(ts_pat,))
    else:
        shutil.rmtree(mirror_dir, ignore_errors=True)
        run(["git", "clone", "--mirror", src_auth, mirror_dir], redact=(ts_pat,))
    run(["git", "-C", mirror_dir, "remote", "set-url", "origin", src])  # strip token from stored remote
    branches = run(["git", "-C", mirror_dir, "for-each-ref", "--format=%(refname:short)", "refs/heads"], capture=True).stdout.split()
    log(f"{len(branches)} branches: {', '.join(branches[:15])}{' ...' if len(branches) > 15 else ''}")
    if ctx.args.dry_run:
        log(f"dry-run: would push --mirror to {dst}")
        return
    run(["git", "-C", mirror_dir, "push", "--mirror", dst_auth], redact=(tgt_token,))
    head = run(["git", "-C", mirror_dir, "rev-parse", "HEAD"], capture=True).stdout.strip()
    ctx.mark(app["name"], "mirror-repo", source=src, target=dst, head=head, branches=len(branches))
    log(f"mirrored to {dst} (HEAD {head[:10]}). Register {dst} as the app's repository in PMP and set pmp_app_id in config.")


# --------------------------------------------------------------------------- PMP side


def _pmp_path(ctx, key, **fmt):
    return ctx.require_config()["pmp"]["paths"][key].format(**fmt)


def _pmp_ids(ctx, app, mode=None, need_env=True):
    app_id = app.get("pmp_app_id")
    if not app_id:
        die(f"pmp_app_id missing for {app['name']} (create the app in PMP from the mirrored repo, then set it in config)")
    env_id = None
    if mode:
        env_id = ctx.app_env(app, mode).get("pmp_env_id")
        if need_env and not env_id:
            die(f"pmp_env_id missing for {app['name']}/{mode}; run 'pmp create-env' or 'pmp list-envs' and record it")
    return app_id, env_id


def _extract_id(obj):
    if isinstance(obj, dict):
        for k in ("id", "environmentId", "packageId", "Id", "ID"):
            if obj.get(k) is not None:
                return obj[k]
    return None


def _items(resp):
    if isinstance(resp, dict):
        for k in ("items", "data", "packages", "environments", "results"):
            if isinstance(resp.get(k), list):
                return resp[k]
        return []
    return resp or []


def cmd_pmp_list_envs(ctx):
    app = ctx.app(ctx.args.app)
    app_id, _ = _pmp_ids(ctx, app, need_env=False)
    print(json.dumps(ctx.pmp().get(_pmp_path(ctx, "environments", appId=app_id)), indent=2))


def cmd_pmp_create_env(ctx):
    app = ctx.app(ctx.args.app)
    mode = ctx.args.mode
    app_id, env_id = _pmp_ids(ctx, app, mode, need_env=False)
    if env_id and not ctx.args.force:
        die(f"{mode} already has pmp_env_id {env_id}; use --force to create another")
    plan = read_json(ctx.path("plan", app["name"], f"{mode}.json"))
    body = plan["environment_request"]
    missing = [k for k, v in body.items() if isinstance(v, str) and v.startswith("<")]
    if missing:
        die(f"placeholders not filled in plan/config: {missing}")
    if ctx.args.dry_run:
        print(json.dumps(body, indent=2))
        return
    pmp = ctx.pmp()
    resp = pmp.post(_pmp_path(ctx, "environments", appId=app_id), body)
    new_id = _extract_id(resp)
    if not new_id:
        for e in _items(pmp.get(_pmp_path(ctx, "environments", appId=app_id))):
            if isinstance(e, dict) and e.get("displayName") == body["displayName"]:
                new_id = _extract_id(e)
    if new_id:
        ctx.app_env(app, mode)["pmp_env_id"] = str(new_id)
        ctx.save_config()
        ctx.mark(app["name"], f"create-env:{mode}", pmp_env_id=str(new_id))
        log(f"environment created: id {new_id} (saved to config)")
    else:
        log(f"environment requested but id not found in response: {resp}. Run 'pmp list-envs' and set pmp_env_id manually.", "WARN")


def cmd_pmp_apply_config(ctx):
    app = ctx.app(ctx.args.app)
    mode = ctx.args.mode
    app_id, env_id = _pmp_ids(ctx, app, mode)
    plan = read_json(ctx.path("plan", app["name"], f"{mode}.json"))
    body = plan["configuration"]
    if ctx.args.dry_run:
        print(json.dumps(body, indent=2))
        return
    pmp = ctx.pmp()
    pmp.patch(_pmp_path(ctx, "configurations", appId=app_id, envId=env_id), body)
    log("configuration patched")
    if not ctx.args.no_apply:
        pmp.post(_pmp_path(ctx, "env_request", appId=app_id, envId=env_id) + "?action=ApplyConfig")
        log("ApplyConfig requested")
    ctx.mark(app["name"], f"apply-config:{mode}", env_vars=len(body["CustomEnvironmentVariables"]), runtime_settings=len(body["CustomRuntimeSettings"]))


def _status_of(p):
    for k in ("status", "Status", "state", "buildStatus", "runtimeStatus"):
        if k in p:
            return str(p[k])
    return ""


def cmd_pmp_build(ctx):
    app = ctx.app(ctx.args.app)
    app_id, _ = _pmp_ids(ctx, app, need_env=False)
    pmp = ctx.pmp()
    revision = ctx.args.revision
    if not revision:
        mirror_dir = ctx.path("repos", f"{app['name']}.git")
        if os.path.isdir(mirror_dir):
            revision = run(["git", "-C", mirror_dir, "rev-parse", ctx.args.branch], capture=True).stdout.strip()
            log(f"revision from mirror: {revision}")
        else:
            die("--revision required (or run mirror-repo first)")
    try:
        major, minor, patch = (int(x) for x in ctx.args.version.split("."))
    except ValueError:
        die("--version must be major.minor.patch")
    body = {"branchName": ctx.args.branch, "revisionId": revision, "majorVersion": major, "minorVersion": minor, "patchVersion": patch}
    if ctx.args.dry_run:
        print(json.dumps(body, indent=2))
        return
    resp = pmp.post(_pmp_path(ctx, "packages", appId=app_id), body)
    pkg_id = _extract_id(resp)
    log(f"build requested: {'package id ' + str(pkg_id) if pkg_id else resp}")
    deadline = time.time() + ctx.args.timeout
    while time.time() < deadline:
        cand = None
        for p in _items(pmp.get(_pmp_path(ctx, "packages", appId=app_id))):
            if not isinstance(p, dict):
                continue
            if pkg_id is not None and str(_extract_id(p)) == str(pkg_id):
                cand = p
                break
            if str(p.get("revisionId") or p.get("revision") or "")[:10] == revision[:10] and f"{major}.{minor}.{patch}" in json.dumps(p):
                cand = p
        if cand:
            st = _status_of(cand)
            log(f"  package {_extract_id(cand)} status: {st or json.dumps(cand)[:120]}")
            if re.search(r"(succe|complete|built|ready|finish)", st, re.I):
                ctx.mark(app["name"], "build", package_id=str(_extract_id(cand)), version=ctx.args.version, revision=revision)
                log(f"build complete: packageId {_extract_id(cand)}")
                return
            if re.search(r"(fail|error|cancel)", st, re.I):
                die(f"build failed: {json.dumps(cand)[:500]}")
        time.sleep(ctx.args.poll)
    die("timed out waiting for build (check the PMP portal; status field names may need adjusting)")


def _env_status_from_dashboard(dash, env_id):
    txt = json.dumps(dash)
    m = re.search(rf'"(?:id|environmentId)"\s*:\s*"?{re.escape(str(env_id))}"?[^{{}}]*?"(?:status|state|runtimeStatus)"\s*:\s*"([^"]+)"', txt)
    return m.group(1) if m else None


def cmd_pmp_deploy(ctx):
    app = ctx.app(ctx.args.app)
    mode = ctx.args.mode
    app_id, env_id = _pmp_ids(ctx, app, mode)
    action = ctx.args.action
    pkg = ctx.args.package_id or (ctx.state.get(app["name"], {}).get("build") or {}).get("package_id")
    if action == "Deploy" and not pkg:
        die("--package-id required (or run 'pmp build' first)")
    q = f"?action={action}" + (f"&packageId={pkg}" if action == "Deploy" else "")
    if ctx.args.dry_run:
        print(_pmp_path(ctx, "env_request", appId=app_id, envId=env_id) + q)
        return
    pmp = ctx.pmp()
    pmp.post(_pmp_path(ctx, "env_request", appId=app_id, envId=env_id) + q)
    log(f"{action} requested" + (f" (package {pkg})" if action == "Deploy" else ""))
    if ctx.args.no_wait:
        return
    want = r"(stopped|idle)" if action == "Stop" else r"(running|started|healthy|online)"
    deadline = time.time() + ctx.args.timeout
    last = None
    while time.time() < deadline:
        st = _env_status_from_dashboard(pmp.get(_pmp_path(ctx, "dashboard", appId=app_id)), env_id)
        if st != last:
            log(f"  environment status: {st or 'unknown (inspect pmp status)'}")
            last = st
        if st and re.search(want, st, re.I):
            ctx.mark(app["name"], f"deploy:{mode}", package_id=str(pkg), action=action)
            return
        if st and re.search(r"(fail|error|crash)", st, re.I):
            die(f"environment reported {st}")
        time.sleep(ctx.args.poll)
    log("timed out waiting for target state; check the PMP portal", "WARN")


def cmd_pmp_status(ctx):
    app = ctx.app(ctx.args.app)
    app_id, _ = _pmp_ids(ctx, app, need_env=False)
    print(json.dumps(ctx.pmp().get(_pmp_path(ctx, "dashboard", appId=app_id)), indent=2))


# --------------------------------------------------------------------------- data migration


def _db_url_from_secret(ns, secret, context=None):
    data = {k: b64d(v) for k, v in (kubectl_json(["get", "secret", secret, "-n", ns], context).get("data") or {}).items()}

    def pick(*names):
        for n in names:
            for k in data:
                if k.lower() == n.lower():
                    return data[k]
        return None
    url = pick("database-url", "DATABASE_URL", "url", "connection-string")
    if url and url.startswith(("postgres://", "postgresql://")):
        return url
    host = pick("database-host", "host", "DATABASE_HOST", "PGHOST")
    name = pick("database-name", "database", "dbname", "DATABASE_NAME", "PGDATABASE")
    user = pick("database-username", "username", "user", "DATABASE_USERNAME", "PGUSER")
    pw = pick("database-password", "password", "DATABASE_PASSWORD", "PGPASSWORD")
    port = pick("database-port", "port", "PGPORT") or "5432"
    dbtype = (pick("database-type", "type") or "postgres").lower()
    if dbtype not in ("postgres", "postgresql"):
        die(f"database type '{dbtype}' is not supported by migrate-db (PostgreSQL only); migrate manually")
    if not all([host, name, user, pw]):
        die(f"could not map secret keys; available keys: {sorted(data)}")
    return f"postgres://{urllib.parse.quote(user, safe='')}:{urllib.parse.quote(pw, safe='')}@{host}:{port}/{name}"


def cmd_db_url(ctx):
    """Print an export line for SOURCE_DB_URL / TARGET_DB_URL from a Kubernetes secret (StorageInstance output)."""
    var = "SOURCE_DB_URL" if ctx.args.side == "source" else "TARGET_DB_URL"
    url = _db_url_from_secret(ctx.args.namespace, ctx.args.secret, ctx.args.context or (ctx.kctx if ctx.args.side == "source" else None))
    print(f"export {var}='{url}'")


def cmd_migrate_db(ctx):
    """pg_dump the source environment database and pg_restore it into the PMP environment database."""
    app = ctx.app(ctx.args.app)
    mode = ctx.args.mode
    which("pg_dump")
    which("pg_restore")
    src, dst = env("SOURCE_DB_URL"), env("TARGET_DB_URL")
    dump = ctx.args.dump_file or ctx.path("dumps", app["name"], f"{mode}-{dt.datetime.now():%Y%m%d-%H%M%S}.dump")
    os.makedirs(os.path.dirname(dump), exist_ok=True)
    if not ctx.args.execute:
        log(f"dry-run: would pg_dump source -> {dump} then pg_restore --clean into target. Add --execute to run.")
        log("stop the SOURCE environment first for a consistent cut-over copy (or accept that writes after the dump are lost)", "WARN")
        return
    if not ctx.args.yes:
        die("restore is destructive for the target database (--clean drops objects); add --yes to confirm")
    if not ctx.args.skip_dump:
        res = run(["pg_dump", "--format=custom", "--no-owner", "--no-privileges", f"--jobs=1", "-f", dump, src], check=False, capture=True, redact=(src,))
        if res.returncode != 0:
            log(res.stderr[-3000:], "ERROR")
            die("pg_dump failed")
        log(f"dump written: {dump} ({os.path.getsize(dump)/1e6:.1f} MB)")
    res = run(["pg_restore", "--no-owner", "--no-privileges", "--clean", "--if-exists", f"--jobs={ctx.args.jobs}", "-d", dst, dump], check=False, capture=True, redact=(dst,))
    if res.returncode != 0:
        log(res.stderr[-3000:], "ERROR")
        if not ctx.args.ignore_restore_errors:
            die("pg_restore reported errors; re-run with --ignore-restore-errors --skip-dump if they are benign (e.g. extension ownership)")
    ctx.mark(app["name"], f"migrate-db:{mode}", dump=dump)
    log("database migrated. The PMP runtime runs schema synchronisation on next Start.")


def _file_key(defaults, name):
    prefix = defaults.get("target_file_prefix", "")
    if defaults.get("file_key_layout", "flat") == "nested" and "/" not in name:
        return f"{prefix}{name[0:2]}/{name[2:4]}/{name}"
    return f"{prefix}{name}"


def cmd_migrate_files(ctx):
    """Copy the source file-storage bucket (S3-compatible) into the PMP environment's bucket."""
    app = ctx.app(ctx.args.app)
    mode = ctx.args.mode
    e = ctx.app_env(app, mode)
    defaults = ctx.require_config().get("defaults", {})
    src = ctx.args.source_bucket or e.get("source_bucket") or die("--source-bucket required (see export secrets / StorageInstance output)")
    dst = ctx.args.target_bucket or e.get("target_bucket") or die("--target-bucket required (PMP environment storage secret)")
    if src == dst:
        log("source and target bucket are identical: nothing to copy (the PMP environment can reuse the bucket if its storage plan points at it)")
        return
    which("aws")
    extra = []
    if ctx.args.endpoint_url:
        extra += ["--endpoint-url", ctx.args.endpoint_url]
    if defaults.get("file_key_layout", "flat") == "flat" and not defaults.get("target_file_prefix"):
        cmd = ["aws", "s3", "sync", f"s3://{src}", f"s3://{dst}", "--only-show-errors"] + extra
        if not ctx.args.execute:
            cmd.append("--dryrun")
        run(cmd)
    else:
        # re-key objects: list source, copy one by one with the target layout
        lst = run(["aws", "s3api", "list-objects-v2", "--bucket", src.split("/")[0], "--output", "json"] + extra, capture=True)
        objs = (json.loads(lst.stdout or "{}").get("Contents") or [])
        manifest = ctx.path("dumps", app["name"], f"{mode}-file-manifest.tsv")
        with open(manifest, "w") as mf:
            for o in objs:
                key = o["Key"]
                tkey = _file_key(defaults, key.rsplit("/", 1)[-1])
                mf.write(f"{key}\t{tkey}\n")
                if ctx.args.execute:
                    run(["aws", "s3", "cp", f"s3://{src}/{key}", f"s3://{dst}/{tkey}", "--only-show-errors"] + extra)
        log(f"{len(objs)} objects {'copied' if ctx.args.execute else 'planned'}; manifest {manifest}")
    if ctx.args.execute:
        ctx.mark(app["name"], f"migrate-files:{mode}", source=src, target=dst)
    else:
        log("dry-run complete; add --execute to copy")


# --------------------------------------------------------------------------- validate / report


def cmd_validate(ctx):
    app = ctx.app(ctx.args.app)
    mode = ctx.args.mode
    plan = read_json(ctx.path("plan", app["name"], f"{mode}.json"))
    app_id, env_id = _pmp_ids(ctx, app, mode)
    pmp = ctx.pmp()
    verify = ctx.require_config()["pmp"].get("verify_tls", True)
    results = []
    try:
        live = json.dumps(pmp.get(_pmp_path(ctx, "configurations", appId=app_id, envId=env_id)))
        want = plan["configuration"]["CustomEnvironmentVariables"] + plan["configuration"]["CustomRuntimeSettings"]
        missing = [w["name"] for w in want if w["name"] not in live]
        results.append(("configuration keys present in PMP", not missing, f"missing: {missing}" if missing else f"{len(want)} keys"))
    except HttpError as e:
        results.append(("configuration readable", False, str(e)))
    try:
        st = _env_status_from_dashboard(pmp.get(_pmp_path(ctx, "dashboard", appId=app_id)), env_id)
        results.append(("PMP environment running", bool(st and re.search(r"(running|started|healthy)", st, re.I)), st or "status not found in dashboard"))
    except HttpError as e:
        results.append(("dashboard readable", False, str(e)))
    url = ctx.args.url or ctx.app_env(app, mode).get("app_url") or plan["configuration"].get("appUrl")
    if url:
        try:
            status, body, _ = Http(url, {}, verify_tls=verify)._req("GET", "/", raw=True)
            results.append((f"app responds at {url}", status < 400, f"HTTP {status}, {len(body)} bytes"))
        except Exception as ex:  # noqa: BLE001
            results.append((f"app responds at {url}", False, str(ex)[:120]))
    # source environment state
    e = ctx.app_env(app, mode)
    try:
        cr = kubectl_json(["get", MXAPP_KINDS[0], e["source_cr"], "-n", e["source_namespace"]], ctx.kctx)
        s = summarize_cr(cr)
        results.append(("source environment", True, f"replicas={s['replicas']} status={s['status']} (scale to 0 / stop after cut-over)"))
    except SystemExit:
        results.append(("source environment readable", False, "kubectl get MendixApp failed (already removed?)"))
    ok = all(r[1] for r in results)
    for name, passed, detail in results:
        print(f"{'PASS' if passed else 'FAIL':<5} {name:<45} {detail}")
    ctx.mark(app["name"], f"validate:{mode}", passed=ok, checks=[{"check": n, "ok": p, "detail": d} for n, p, d in results])
    sys.exit(0 if ok else 1)


def cmd_report(ctx):
    cfg = ctx.require_config()
    steps = ["export", "mirror-repo", "create-env", "apply-config", "build", "deploy", "migrate-db", "migrate-files", "validate"]
    lines = [f"# mx2pmp migration status ({dt.date.today()})", "", "| App | " + " | ".join(steps) + " |", "|---|" + "---|" * len(steps)]
    for a in cfg.get("apps", []):
        s = ctx.state.get(a["name"], {})
        cells = []
        for st in steps:
            hits = [k for k in s if k == st or k.startswith(st + ":")]
            if not hits:
                cells.append("")
            elif st == "validate":
                cells.append(" ".join(("✅ " if s[h].get("passed") else "❌ ") + h.split(":", 1)[1] for h in hits))
            else:
                cells.append("✅ " + ", ".join(h.split(":", 1)[1] for h in hits if ":" in h) if any(":" in h for h in hits) else "✅")
        lines.append(f"| {a['name']} | " + " | ".join(cells) + " |")
    out = "\n".join(lines) + "\n"
    with open(ctx.path("report.md"), "w") as f:
        f.write(out)
    print(out)


# --------------------------------------------------------------------------- CLI


def main(argv=None):
    p = argparse.ArgumentParser(prog="mx2pmp", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--config", default="mx2pmp.json", help="config file (default mx2pmp.json)")
    p.add_argument("--version", action="version", version=VERSION)
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("init", help="write a config template"); s.add_argument("--force", action="store_true"); s.set_defaults(fn=cmd_init)
    s = sub.add_parser("inventory", help="list MendixApp environments in the source cluster"); s.add_argument("--namespace", "-n"); s.add_argument("--add-to-config", action="store_true"); s.set_defaults(fn=cmd_inventory)
    s = sub.add_parser("export", help="export MendixApp CR + storage + secret metadata"); s.add_argument("app"); s.add_argument("--mode"); s.add_argument("--include-secrets", action="store_true", help="also dump secret values (0600)"); s.set_defaults(fn=cmd_export)
    s = sub.add_parser("plan", help="translate export into PMP payloads"); s.add_argument("app"); s.add_argument("--mode"); s.add_argument("--overrides", help="JSON {constant: value} or {mode: {...}}"); s.set_defaults(fn=cmd_plan)
    s = sub.add_parser("mirror-repo", help="mirror Team Server repo to the organisation Git provider"); s.add_argument("app"); s.add_argument("--dry-run", action="store_true"); s.set_defaults(fn=cmd_mirror_repo)

    pm = sub.add_parser("pmp", help="operations against PMP").add_subparsers(dest="pmpcmd", required=True)
    s = pm.add_parser("list-envs"); s.add_argument("app"); s.set_defaults(fn=cmd_pmp_list_envs)
    s = pm.add_parser("create-env"); s.add_argument("app"); s.add_argument("--mode", required=True); s.add_argument("--dry-run", action="store_true"); s.add_argument("--force", action="store_true"); s.set_defaults(fn=cmd_pmp_create_env)
    s = pm.add_parser("apply-config"); s.add_argument("app"); s.add_argument("--mode", required=True); s.add_argument("--dry-run", action="store_true"); s.add_argument("--no-apply", action="store_true"); s.set_defaults(fn=cmd_pmp_apply_config)
    s = pm.add_parser("build"); s.add_argument("app"); s.add_argument("--branch", default="main"); s.add_argument("--revision"); s.add_argument("--version", default="1.0.0", dest="version")
    s.add_argument("--timeout", type=int, default=2700); s.add_argument("--poll", type=int, default=20); s.add_argument("--dry-run", action="store_true"); s.set_defaults(fn=cmd_pmp_build)
    s = pm.add_parser("deploy"); s.add_argument("app"); s.add_argument("--mode", required=True); s.add_argument("--package-id"); s.add_argument("--action", default="Deploy", choices=["Deploy", "Start", "Stop", "Restart", "ApplyConfig"])
    s.add_argument("--timeout", type=int, default=900); s.add_argument("--poll", type=int, default=15); s.add_argument("--no-wait", action="store_true"); s.add_argument("--dry-run", action="store_true"); s.set_defaults(fn=cmd_pmp_deploy)
    s = pm.add_parser("status"); s.add_argument("app"); s.set_defaults(fn=cmd_pmp_status)

    s = sub.add_parser("db-url", help="derive SOURCE_DB_URL/TARGET_DB_URL from a Kubernetes secret"); s.add_argument("side", choices=["source", "target"]); s.add_argument("--namespace", "-n", required=True); s.add_argument("--secret", required=True); s.add_argument("--context"); s.set_defaults(fn=cmd_db_url)
    s = sub.add_parser("migrate-db", help="pg_dump source DB -> pg_restore into PMP DB"); s.add_argument("app"); s.add_argument("--mode", required=True); s.add_argument("--dump-file"); s.add_argument("--jobs", type=int, default=4)
    s.add_argument("--skip-dump", action="store_true"); s.add_argument("--ignore-restore-errors", action="store_true"); s.add_argument("--execute", action="store_true"); s.add_argument("--yes", action="store_true"); s.set_defaults(fn=cmd_migrate_db)
    s = sub.add_parser("migrate-files", help="copy file-storage bucket to the PMP bucket"); s.add_argument("app"); s.add_argument("--mode", required=True); s.add_argument("--source-bucket"); s.add_argument("--target-bucket"); s.add_argument("--endpoint-url"); s.add_argument("--execute", action="store_true"); s.set_defaults(fn=cmd_migrate_files)
    s = sub.add_parser("validate", help="post-migration checks"); s.add_argument("app"); s.add_argument("--mode", required=True); s.add_argument("--url"); s.set_defaults(fn=cmd_validate)
    s = sub.add_parser("report", help="markdown status table from state"); s.set_defaults(fn=cmd_report)

    args = p.parse_args(argv)
    ctx = Ctx(args)
    try:
        args.fn(ctx)
    except HttpError as e:
        die(str(e))
    except KeyboardInterrupt:
        die("interrupted", 130)


if __name__ == "__main__":
    main()
