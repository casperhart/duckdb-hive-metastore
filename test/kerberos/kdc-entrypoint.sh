#!/usr/bin/env bash
# Initialise a throwaway MIT Kerberos realm and start the KDC + kadmind.
# Writes the client-facing krb5.conf and the service/client keytabs to the
# shared /krb-secrets volume so both the metastore container and the host test
# runner can consume them. LOCAL TEST USE ONLY.
set -euo pipefail

REALM="${REALM:-EXAMPLE.COM}"
SECRETS=/krb-secrets
mkdir -p "$SECRETS" /var/lib/krb5kdc /etc/krb5kdc

cat > /etc/krb5kdc/kdc.conf <<EOF
[kdcdefaults]
    kdc_ports = 88
    kdc_tcp_ports = 88
[realms]
    ${REALM} = {
        database_name = /var/lib/krb5kdc/principal
        admin_keytab = /etc/krb5kdc/kadm5.keytab
        acl_file = /etc/krb5kdc/kadm5.acl
        key_stash_file = /var/lib/krb5kdc/stash
        max_life = 24h
        max_renewable_life = 7d
        supported_enctypes = aes256-cts-hmac-sha1-96:normal aes128-cts-hmac-sha1-96:normal
    }
EOF

# The KDC's own view: it is "localhost" to itself.
cat > /etc/krb5.conf <<EOF
[libdefaults]
    default_realm = ${REALM}
    dns_lookup_realm = false
    dns_lookup_kdc = false
    dns_canonicalize_hostname = false
    rdns = false
    forwardable = true
[realms]
    ${REALM} = {
        kdc = localhost
        admin_server = localhost
    }
EOF

# Host-facing krb5.conf: the KDC is reachable via the mapped ports on localhost.
# dns_canonicalize_hostname/rdns are off so the "localhost" instance in the SPN
# (hive/localhost@REALM) is used verbatim — this is what lets a host client that
# dials thrift://localhost:9083 match the metastore's service principal.
cat > "${SECRETS}/krb5.conf" <<EOF
[libdefaults]
    default_realm = ${REALM}
    dns_lookup_realm = false
    dns_lookup_kdc = false
    dns_canonicalize_hostname = false
    rdns = false
    forwardable = true
[realms]
    ${REALM} = {
        kdc = localhost:8088
        admin_server = localhost:8749
    }
[domain_realm]
    localhost = ${REALM}
EOF

if [ ! -f /var/lib/krb5kdc/principal ]; then
    kdb5_util create -s -r "${REALM}" -P masterkey
    kadmin.local -q "addprinc -randkey hive/localhost@${REALM}"
    kadmin.local -q "addprinc -randkey client@${REALM}"
    rm -f "${SECRETS}/hive.keytab" "${SECRETS}/client.keytab"
    kadmin.local -q "ktadd -k ${SECRETS}/hive.keytab   hive/localhost@${REALM}"
    kadmin.local -q "ktadd -k ${SECRETS}/client.keytab client@${REALM}"
    chmod 0644 "${SECRETS}"/*.keytab "${SECRETS}/krb5.conf"
fi

krb5kdc
exec kadmind -nofork
