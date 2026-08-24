#!/bin/sh
set -eu

api_base_url="${MMPF_CONSOLE_API_BASE_URL:-/api}"
poll_interval_ms="${MMPF_CONSOLE_POLL_INTERVAL_MS:-5000}"
style_preview_base_url="${MMPF_CONSOLE_STYLE_PREVIEW_BASE_URL:-}"
tileset_preview_base_url="${MMPF_CONSOLE_TILESET_PREVIEW_BASE_URL:-}"
config_path="${MMPF_CONSOLE_CONFIG_PATH:-/usr/share/nginx/html/console-config.json}"

validate_base_url() {
    name="$1"
    value="$2"
    allow_empty="$3"
    if [ "$allow_empty" = true ] && [ -z "$value" ]; then
        return
    fi
    case "$value" in
        /*|http://?*|https://?*) ;;
        *)
            echo "$name must be an absolute path or HTTP(S) URL without query or fragment" >&2
            exit 1
            ;;
    esac
    if printf '%s' "$value" | LC_ALL=C grep -q '[[:space:][:cntrl:]"?#@]' || \
        printf '%s' "$value" | grep -q '[\\]'; then
        echo "$name must be an absolute path or HTTP(S) URL without query or fragment" >&2
        exit 1
    fi
}

validate_base_url MMPF_CONSOLE_API_BASE_URL "$api_base_url" false
validate_base_url MMPF_CONSOLE_STYLE_PREVIEW_BASE_URL "$style_preview_base_url" true
validate_base_url MMPF_CONSOLE_TILESET_PREVIEW_BASE_URL "$tileset_preview_base_url" true

case "$poll_interval_ms" in
    ''|*[!0-9]*)
        echo "MMPF_CONSOLE_POLL_INTERVAL_MS must be an integer between 2000 and 60000" >&2
        exit 1
        ;;
esac
if [ "$poll_interval_ms" -lt 2000 ] || [ "$poll_interval_ms" -gt 60000 ]; then
    echo "MMPF_CONSOLE_POLL_INTERVAL_MS must be an integer between 2000 and 60000" >&2
    exit 1
fi

config_tmp="${config_path}.tmp"
printf '{\n  "apiBaseUrl": "%s",\n  "pollIntervalMs": %s,\n  "stylePreviewBaseUrl": "%s",\n  "tilesetPreviewBaseUrl": "%s"\n}\n' \
    "$api_base_url" "$poll_interval_ms" "$style_preview_base_url" "$tileset_preview_base_url" > "$config_tmp"
mv "$config_tmp" "$config_path"
