import logging
import os

from cli._io import load_lines, load_json_object
from cli.output import print_name_list
from api.errors import UserInputError

from api.config_file_ops import (
    list_config_files,
    get_config_file,
    create_config_file,
    replace_config_file,
    upsert_config_file,
    delete_config_file,
)
from api.prm_builder import build_properties_lines, load_target_connection, apply_target_field_map

log = logging.getLogger(__name__)


def _resolve_secrets(secret_env_args):
    secrets = {}
    for entry in (secret_env_args or []):
        if "=" not in entry:
            raise UserInputError(
                f"Invalid --secret-env format '{entry}': expected TEMPLATE_VAR=ENV_VAR_NAME"
            )
        template_var, env_var = entry.split("=", 1)
        value = os.environ.get(env_var)
        if value is None:
            raise UserInputError(
                f"Environment variable '{env_var}' (referenced by --secret-env) is not set"
            )
        secrets[template_var.strip()] = value
    return secrets


def handle_config_file(args, client, cfg):
    if args.action == "list":
        files = list_config_files(client)
        if not files:
            log.info("No config files found")
            return
        print_name_list(files)

    elif args.action == "get":
        lines = get_config_file(client, args.name)
        print("\n".join(lines))

    elif args.action == "create":
        if args.template:
            context = {"deployment_name": cfg.get("deployment"), "env": cfg.get("env")}
            if args.target_name:
                target_data = load_target_connection(args.target_name)
                context.update(apply_target_field_map(args.template, target_data))
            if args.vars_json:
                context.update(load_json_object(args.vars_json, "--vars-json"))
            if args.secret_env:
                context.update(_resolve_secrets(args.secret_env))
            lines = build_properties_lines(args.template, context)
            upsert_config_file(client, args.name, lines)
            log.info("Config file '%s' created from template '%s' (%d lines)",
                     args.name, args.template, len(lines))
        else:
            lines = load_lines(args)
            create_config_file(client, args.name, lines)
            log.info("Config file '%s' created (%d lines)", args.name, len(lines))

    elif args.action == "replace":
        lines = load_lines(args)
        replace_config_file(client, args.name, lines)
        log.info("Config file '%s' replaced (%d lines)", args.name, len(lines))

    elif args.action == "delete":
        delete_config_file(client, args.name)
        log.info("Config file '%s' deleted", args.name)
