import logging

from cli.output import print_status_table, print_info
from api.errors import die
from api.prm_builder import PROPERTIES_TEMPLATE_MAP, TEMPLATE_VARS

log = logging.getLogger(__name__)


def handle_templates(args, client, cfg):
    if args.action == "list":
        rows = []
        for name in sorted(PROPERTIES_TEMPLATE_MAP):
            spec = TEMPLATE_VARS.get(name, {})
            rows.append((name, spec.get("kind", "")))
        if not rows:
            log.info("No templates registered")
            return
        print_status_table(rows)

    elif args.action == "show":
        if args.name not in PROPERTIES_TEMPLATE_MAP:
            available = ", ".join(sorted(PROPERTIES_TEMPLATE_MAP))
            die(f"Unknown template '{args.name}'. Available: {available}")
        spec = TEMPLATE_VARS.get(args.name)
        if not spec:
            die(f"Template '{args.name}' has no variable contract registered")

        print_info({
            "template":     args.name,
            "kind":         spec.get("kind", ""),
            "file":         PROPERTIES_TEMPLATE_MAP[args.name],
            "description":  spec.get("description", ""),
            "required":     ", ".join(spec.get("required", [])) or "(none)",
            "optional":     ", ".join(f"{k}={v}" for k, v in spec.get("optional", {}).items()) or "(none)",
            "auto_injected": ", ".join(spec.get("auto_injected", [])) or "(none)",
        })
