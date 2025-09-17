import os
import sys
import webbrowser
from pathlib import Path

from dotenv import load_dotenv
from kiteconnect import KiteConnect


def ensure_env_loaded() -> None:
    # Load .env from the project root (parent of scripts/) so CWD doesn't matter
    script_path = Path(__file__).resolve()
    project_root = script_path.parents[1]
    env_file = project_root / ".env"
    if env_file.exists():
        load_dotenv(dotenv_path=env_file, override=True)
    else:
        # Fallback to CWD for environments that run from root
        load_dotenv(override=True)


def _sanitize(value: str) -> str:
    if value is None:
        return ""
    v = value.strip()
    if (v.startswith('"') and v.endswith('"')) or (v.startswith("'") and v.endswith("'")):
        v = v[1:-1].strip()
    return v


def _looks_like_placeholder(value: str) -> bool:
    if not value:
        return True
    v = _sanitize(value).lower()
    placeholders = {
        "your_key",
        "your_key_here",
        "your_api_key",
        "your_secret",
        "your_secret_here",
        "your_api_secret",
    }
    return v in placeholders


def get_or_prompt_secret(var_name: str, human_label: str) -> str:
    current = _sanitize(os.getenv(var_name, ""))
    if _looks_like_placeholder(current):
        print(f"{var_name} is missing or a placeholder.")
        entered = input(f"Enter {human_label}: ").strip()
        if not entered:
            print(f"No {var_name} provided. Exiting.")
            sys.exit(1)
        env_path = Path.cwd() / ".env"
        write_env_value(env_path, var_name, _sanitize(entered))
        # Reload so downstream reads see the saved value
        load_dotenv(override=True)
        return _sanitize(entered)
    return _sanitize(current)


def write_env_value(env_path: Path, key: str, value: str) -> None:
    lines = []
    if env_path.exists():
        lines = env_path.read_text(encoding="utf-8").splitlines()

    key_prefix = f"{key}="
    updated = False
    for i, line in enumerate(lines):
        if line.strip().startswith(key_prefix):
            lines[i] = f"{key}={value}"
            updated = True
            break

    if not updated:
        lines.append(f"{key}={value}")

    env_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    print("Zerodha token helper")
    print("====================")

    # 1) Load env and read API creds
    ensure_env_loaded()
    api_key = get_or_prompt_secret("ZERODHA_API_KEY", "ZERODHA_API_KEY (from developer.kite.trade)")
    api_secret = get_or_prompt_secret("ZERODHA_API_SECRET", "ZERODHA_API_SECRET (from developer.kite.trade)")


    # Debug hint if user still faces wrong URL issues
    ak_mask = f"{api_key[:4]}...{api_key[-4:]}" if len(api_key) >= 8 else api_key
    print(f"Using API Key: {ak_mask}")
    if _looks_like_placeholder(api_key) or _looks_like_placeholder(api_secret):
        print("Provided API key/secret look invalid. Please re-run and enter real values.")

    # 2) Create login URL and open browser
    kite = KiteConnect(api_key=api_key)
    login_url = kite.login_url()
    print("\nOpen this URL to login and authorize:")
    print(login_url)
    try:
        webbrowser.open(login_url)
    except Exception:
        pass

    # 3) Prompt for request_token from redirect URL
    print("\nAfter logging in, you'll be redirected to your Redirect URL.")
    print("Copy the 'request_token' query parameter value and paste it below.")
    request_token = input("request_token: ").strip()
    if not request_token:
        print("No request_token provided. Exiting.")
        sys.exit(1)

    # 4) Exchange for access_token
    print("\nExchanging request_token for access_token...")
    session = kite.generate_session(request_token, api_secret=api_secret)
    access_token = session["access_token"]
    print("\nSUCCESS. Your access_token is:")
    print(access_token)

    # 5) Save to .env in current working directory
    env_path = Path.cwd() / ".env"
    write_env_value(env_path, "ZERODHA_ACCESS_TOKEN", access_token)
    print(f"\nSaved ZERODHA_ACCESS_TOKEN to {env_path}")

    print("\nYou're all set. Restart your app/processes that read .env.")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Error: {exc}")
        sys.exit(1)


