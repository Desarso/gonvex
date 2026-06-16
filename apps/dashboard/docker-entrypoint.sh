#!/bin/sh
set -eu

auth_config="/etc/nginx/conf.d/dashboard-auth.conf"
auth_file="/etc/nginx/auth/dashboard.htpasswd"

if [ "${DASHBOARD_BASIC_AUTH_ENABLED:-true}" = "false" ]; then
  printf 'auth_basic off;\n' > "$auth_config"
else
  if [ -z "${DASHBOARD_BASIC_AUTH_USER:-}" ] || [ -z "${DASHBOARD_BASIC_AUTH_PASSWORD:-}" ]; then
    echo "DASHBOARD_BASIC_AUTH_USER and DASHBOARD_BASIC_AUTH_PASSWORD are required when dashboard basic auth is enabled." >&2
    exit 1
  fi

  mkdir -p "$(dirname "$auth_file")"
  password_hash="$(openssl passwd -apr1 "$DASHBOARD_BASIC_AUTH_PASSWORD")"
  printf '%s:%s\n' "$DASHBOARD_BASIC_AUTH_USER" "$password_hash" > "$auth_file"
  chmod 644 "$auth_file"

  {
    printf 'auth_basic "Gonvex Dashboard";\n'
    printf 'auth_basic_user_file %s;\n' "$auth_file"
  } > "$auth_config"
fi

exec nginx -g "daemon off;"
