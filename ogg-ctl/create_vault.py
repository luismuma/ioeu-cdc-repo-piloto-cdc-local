#!/usr/bin/env python3

import argparse
import getpass
import hashlib
import hmac as _hmac
import os
import sys
from binascii import hexlify, unhexlify


def _derive_keys(b_password: bytes, b_salt: bytes):
    b_derived = hashlib.pbkdf2_hmac("sha256", b_password, b_salt, 10000, dklen=80)
    return b_derived[:32], b_derived[32:64], b_derived[64:80]  # key1, key2, iv


def encrypt_vault(plaintext: str, password: str, vault_id: str = "") -> str:
    try:
        from cryptography.hazmat.backends import default_backend
        from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
    except ImportError:
        sys.exit("cryptography package not found — run: pip install cryptography")

    b_plaintext = plaintext.encode()
    b_salt = os.urandom(32)
    b_key1, b_key2, b_iv = _derive_keys(password.encode(), b_salt)

    # PKCS7 padding (AES block = 16 bytes)
    pad_len = 16 - len(b_plaintext) % 16
    b_padded = b_plaintext + bytes([pad_len] * pad_len)

    # AES-256-CTR encryption
    cipher = Cipher(algorithms.AES(b_key1), modes.CTR(b_iv), backend=default_backend())
    enc = cipher.encryptor()
    b_ciphertext = enc.update(b_padded) + enc.finalize()

    # HMAC-SHA256 over ciphertext
    b_hmac = _hmac.new(b_key2, b_ciphertext, hashlib.sha256).hexdigest().encode()

    # Ansible Vault body: hexlify( hexlify(salt) + \n + hmac_hex + \n + hexlify(ciphertext) )
    b_inner = hexlify(b_salt) + b"\n" + b_hmac + b"\n" + hexlify(b_ciphertext)
    hex_body = hexlify(b_inner).decode()

    # Wrap at 80 characters per line
    wrapped = "\n".join(hex_body[i:i + 80] for i in range(0, len(hex_body), 80))

    if vault_id:
        header = f"$ANSIBLE_VAULT;1.2;AES256;{vault_id}"
    else:
        header = "$ANSIBLE_VAULT;1.1;AES256"

    return f"{header}\n{wrapped}\n"


def decrypt_vault(vault_text: str, password: str):
    """Decrypt an Ansible Vault AES256 string and return (plaintext, vault_id).
    Supports formats $ANSIBLE_VAULT;1.1;AES256 and $ANSIBLE_VAULT;1.2;AES256;<vault_id>."""
    try:
        from cryptography.hazmat.backends import default_backend
        from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
    except ImportError:
        sys.exit("cryptography package not found — run: pip install cryptography")

    lines = vault_text.splitlines()
    if not lines or not lines[0].startswith("$ANSIBLE_VAULT;"):
        raise ValueError("Not an Ansible Vault file (missing $ANSIBLE_VAULT header)")

    header_parts = lines[0].split(";")
    if len(header_parts) < 3 or header_parts[2] != "AES256":
        raise ValueError(f"Unsupported vault header: {lines[0]!r} (expected AES256)")
    vault_id = header_parts[3] if len(header_parts) >= 4 else ""

    hex_body = "".join(line.strip() for line in lines[1:] if line.strip())
    try:
        inner = unhexlify(hex_body)
    except Exception as e:
        raise ValueError(f"Body is not valid hex: {e}")

    parts = inner.split(b"\n")
    if len(parts) != 3:
        raise ValueError(f"Expected 3 body parts (salt/hmac/ciphertext), got {len(parts)}")
    b_salt        = unhexlify(parts[0])
    expected_hmac = parts[1]                # already hex-encoded bytes
    b_ciphertext  = unhexlify(parts[2])

    b_key1, b_key2, b_iv = _derive_keys(password.encode(), b_salt)

    # Verify HMAC first — wrong password fails here, before any decryption
    actual_hmac = _hmac.new(b_key2, b_ciphertext, hashlib.sha256).hexdigest().encode()
    if not _hmac.compare_digest(actual_hmac, expected_hmac):
        raise ValueError("HMAC mismatch — wrong vault password (or file is corrupt)")

    cipher = Cipher(algorithms.AES(b_key1), modes.CTR(b_iv), backend=default_backend())
    dec = cipher.decryptor()
    b_padded = dec.update(b_ciphertext) + dec.finalize()

    pad_len = b_padded[-1]
    if pad_len < 1 or pad_len > 16:
        raise ValueError("Invalid PKCS7 padding — file is corrupt")
    plaintext = b_padded[:-pad_len].decode("utf-8")

    return plaintext, vault_id


def _verify_cmd(file_path, show):
    if not file_path:
        file_path = input("Vault file path: ").strip()
    if not os.path.isfile(file_path):
        sys.exit(f"Error: file not found: {file_path}")

    password = getpass.getpass("Vault password: ")
    try:
        with open(file_path, "r") as f:
            plaintext, vault_id = decrypt_vault(f.read(), password)
    except ValueError as e:
        sys.exit(f"Decryption failed: {e}")

    print(f"\nOK — vault decrypted successfully")
    print(f"File:     {file_path}")
    print(f"Vault ID: {vault_id or '(none — format 1.1)'}")
    if show:
        print("\n--- decrypted content ---")
        print(plaintext)
    else:
        print(f"\n(rerun with --show to print the decrypted content)")


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Create or verify an Ansible Vault AES256-encrypted file without Ansible installed.\n\n"
            "Each environment (dev, pre, prod, ...) should have its own vault file,\n"
            "its own Vault ID, and its own password. In AAP, create one Vault credential\n"
            "per environment and set the matching Vault Identifier field. Ansible will\n"
            "automatically decrypt only the file whose ID matches the credential.\n\n"
            "The encrypted file is safe to commit to the repository — only the vault\n"
            "password can decrypt it, and that password must never be committed."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "examples:\n"
            "  python create_vault.py                                # interactive create\n"
            "  python create_vault.py ogg-install/ansible/group_vars/pre/vault.yml\n"
            "  python create_vault.py --verify ogg-install/ansible/group_vars/pre/vault.yml\n"
            "  python create_vault.py --verify path/to/vault.yml --show\n\n"
            "workflow:\n"
            "  1. Run this script once per environment (dev, pre, prod, ...).\n"
            "  2. Use a different Vault ID and password for each.\n"
            "  3. Commit all generated vault files to the repository.\n"
            "  4. In AAP, create one Vault credential per environment:\n"
            "       Name: e.g. 'OGG Vault - DEV'\n"
            "       Vault Password: the password you chose\n"
            "       Vault Identifier: the Vault ID you chose (e.g. 'oggadmin-dev-credential')\n"
            "  5. Attach all Vault credentials to the Job Template.\n"
            "     Ansible decrypts only the file whose ID matches."
        ),
    )
    parser.add_argument(
        "output_file", nargs="?",
        help="Path to write the encrypted vault file (prompted interactively if omitted)",
    )
    parser.add_argument(
        "--verify", dest="verify_file", nargs="?", const="", metavar="FILE",
        help="Verify an existing vault file decrypts cleanly. Provide a path, or omit to be prompted. Use --show to print the decrypted content.",
    )
    parser.add_argument(
        "--show", action="store_true",
        help="With --verify: also print the decrypted YAML content to stdout",
    )
    args = parser.parse_args()

    # Verify mode — skip the encrypt flow entirely
    if args.verify_file is not None:
        _verify_cmd(args.verify_file or None, args.show)
        return

    env = input("Environment (dev / pre / prod — required): ").strip().lower()
    if env not in ("dev", "pre", "prod"):
        sys.exit("Error: environment must be one of: dev, pre, prod.")

    default_vault_id = f"oggadmin-{env}-credential"
    vault_id = input(f"Vault ID [{default_vault_id}]: ").strip() or default_vault_id

    suggestion = f"ogg-install/ansible/group_vars/{env}/vault.yml"
    if args.output_file:
        out = args.output_file
    else:
        entered = input(f"Output file [{suggestion}]: ").strip()
        out = entered if entered else suggestion

    password = getpass.getpass("Vault password: ")
    confirm  = getpass.getpass("Confirm vault password: ")
    if password != confirm:
        sys.exit("Error: passwords do not match.")

    ogg_pass = getpass.getpass("OGG admin password: ")

    plaintext = f'vault_ogg_admin_password: "{ogg_pass}"\n'
    vault_text = encrypt_vault(plaintext, password, vault_id)

    os.makedirs(os.path.dirname(os.path.abspath(out)), exist_ok=True)
    with open(out, "w", newline="\n") as f:
        f.write(vault_text)

    print(f"\nCreated: {out}")
    print(f"Vault ID: {vault_id}  ← set this in AAP Vault credential → 'Vault Identifier'")
    print(f"Commit:  git add {out} && git commit -m 'add encrypted {env} vault'")
    print("Note:    keep your vault password safe — store it in AAP as a Vault credential.")


if __name__ == "__main__":
    main()
