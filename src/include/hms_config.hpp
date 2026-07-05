//===----------------------------------------------------------------------===//
//                         DuckDB
//
// hms_config.hpp
//
// Ambient Hadoop/Hive configuration discovery. Locates and parses
// hive-site.xml from the environment (HADOOP_CONF_DIR / HIVE_CONF_DIR / ...)
// so the metastore URI and Kerberos settings can be picked up automatically
// without any DuckDB secrets or ATTACH options.
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

// Parsed subset of hive-site.xml that this extension cares about.
struct HMSSiteConfig {
	// True if a hive-site.xml was located and parsed. When false, callers must
	// preserve the pre-existing behaviour (plaintext, endpoint from ATTACH).
	bool found = false;
	// hive.metastore.uris, split on ',' (e.g. thrift://host:9083). May be empty.
	vector<string> metastore_uris;
	// hive.metastore.sasl.enabled — the signal that the metastore requires
	// SASL/Kerberos authentication.
	bool sasl_enabled = false;
	// True when hive.metastore.sasl.enabled appeared explicitly in hive-site.xml.
	// An explicit false must not be overridden by hadoop.security.authentication.
	bool sasl_set = false;
	// hive.metastore.kerberos.principal, e.g. "hive/_HOST@REALM".
	string kerberos_principal;
	// hadoop.security.authentication (from hive-site.xml or core-site.xml). When
	// "kerberos" it implies SASL unless hive.metastore.sasl.enabled says otherwise.
	string hadoop_auth;
	// Absolute path of the config file we loaded, for diagnostics.
	string source_path;
};

// Locate and parse hive-site.xml from the ambient Hadoop/Hive environment.
// Never throws for "not found" — returns found=false so the non-Kerberos,
// endpoint-from-ATTACH path is completely unaffected.
HMSSiteConfig HMSLoadSiteConfig();

// Parse a hive-site.xml document already held in memory. Exposed separately so
// the parser can be unit-tested without a filesystem or a live cluster.
HMSSiteConfig HMSParseSiteConfig(const string &xml);

// Split a Kerberos principal "primary/instance@REALM" into its service primary
// and instance. `service` falls back to "hive" (the metastore default) when the
// principal is empty/unparseable; `instance` is "" when absent. Callers treat a
// literal "_HOST" instance as "substitute the endpoint host" (Hive semantics)
// and a concrete instance as the exact SPN host to authenticate against.
void HMSKerberosPrincipalParts(const string &principal, string &service, string &instance);

} // namespace duckdb
