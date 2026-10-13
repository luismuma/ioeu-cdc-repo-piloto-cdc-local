import logging
from pathlib import Path

from cli.output import print_info
from api.user_ops import (
    list_roles,
    list_users,
    get_user,
    create_user,
    update_user,
    delete_user,
    VALID_ROLES,
)
from api.errors import UserInputError

log = logging.getLogger(__name__)


def _read_password(password_file):
    path = Path(password_file)
    if not path.exists():
        raise UserInputError(f"Password file not found: {password_file}")
    return path.read_text().strip()


def handle_user(args, client):
    if args.action == "list":
        if args.role:
            users = list_users(client, args.role)
            if not users:
                log.info("No users found for role '%s'", args.role)
                return
            for u in users:
                print(f"{u['user']:<40} {u['role']}")
        else:
            roles = list_roles(client)
            if not roles:
                log.info("No roles found")
                return
            found_any = False
            for role in roles:
                try:
                    users = list_users(client, role)
                    for u in users:
                        print(f"{u['user']:<40} {role}")
                        found_any = True
                except Exception:
                    pass
            if not found_any:
                log.info("No users found")

    elif args.action == "get":
        user = get_user(client, args.role, args.username)
        print_info(user)

    elif args.action == "create":
        password = _read_password(args.password_file)
        create_user(
            client,
            role=args.role,
            username=args.username,
            password=password,
            info=args.info,
        )
        log.info("User '%s' created with role '%s'", args.username, args.role)

    elif args.action == "update":
        password = _read_password(args.password_file) if args.password_file else None
        update_user(
            client,
            role=args.role,
            username=args.username,
            password=password,
            info=args.info,
        )
        log.info("User '%s' updated", args.username)

    elif args.action == "delete":
        delete_user(client, args.role, args.username)
        log.info("User '%s' deleted from role '%s'", args.username, args.role)
