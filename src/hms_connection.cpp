#include "hms_connection.hpp"

#include "hms_api.hpp"
#include "hms_config.hpp"
#include "hms_kerberos.hpp"
#include "duckdb/common/string_util.hpp"

#include <random>

namespace duckdb {

HMSConnection::HMSConnection(string attach_path) : attach_path(std::move(attach_path)) {
}

void HMSConnection::Reset() {
	client.reset();
}

namespace {

// Split a comma-separated endpoint list (as hive.metastore.uris uses), trimming
// whitespace and dropping empties.
vector<string> SplitEndpoints(const string &s) {
	vector<string> out;
	for (auto &part : StringUtil::Split(s, ",")) {
		auto trimmed = part;
		StringUtil::Trim(trimmed);
		if (!trimmed.empty()) {
			out.push_back(trimmed);
		}
	}
	return out;
}

} // namespace

void HMSConnection::ResolveTargets() {
	// Ambient Hadoop/Hive config (hive-site.xml). Absent => behave like the
	// original extension: use the ATTACH endpoint, plaintext.
	HMSSiteConfig site = HMSLoadSiteConfig();

	if (!attach_path.empty()) {
		endpoints = SplitEndpoints(attach_path);
	} else if (site.found && !site.metastore_uris.empty()) {
		endpoints = site.metastore_uris;
	}
	if (endpoints.empty()) {
		throw InvalidInputException(
		    "No Hive Metastore endpoint was provided and none could be discovered from hive-site.xml. Pass a "
		    "'thrift://host:port' path to ATTACH, or set HADOOP_CONF_DIR/HIVE_CONF_DIR so hive.metastore.uris can be "
		    "found.");
	}

	// Kerberos only when the metastore config explicitly requires SASL. fqdn is
	// left empty so each endpoint's own host is used to build its SPN.
	if (site.found && site.sasl_enabled) {
		auth.kerberos = true;
		auth.service = HMSKerberosServiceFromPrincipal(site.kerberos_principal);
	}

	// Random starting URI so load spreads across the HA endpoints (matches
	// Hive's default hive.metastore.uri.selection=RANDOM).
	if (endpoints.size() > 1) {
		std::random_device rd;
		std::mt19937 gen(rd());
		std::uniform_int_distribution<size_t> dist(0, endpoints.size() - 1);
		next_endpoint = dist(gen);
	}
	resolved = true;
}

HMSClient &HMSConnection::GetOrConnect() {
	if (client && client->IsConnected()) {
		return *client;
	}
	client.reset();

	if (!resolved) {
		ResolveTargets();
	}

	// Try every endpoint once, starting at next_endpoint, until one connects.
	const idx_t n = endpoints.size();
	string errors;
	for (idx_t i = 0; i < n; i++) {
		idx_t idx = (next_endpoint + i) % n;
		try {
			client = HMSAPI::CreateClient(endpoints[idx], auth);
			// Next (re)connect starts at the following endpoint.
			next_endpoint = (idx + 1) % n;
			return *client;
		} catch (std::exception &e) {
			if (!errors.empty()) {
				errors += "; ";
			}
			errors += endpoints[idx] + ": " + e.what();
		}
	}
	throw IOException("Failed to connect to any Hive Metastore endpoint (%llu tried): %s", (unsigned long long)n,
	                  errors);
}

} // namespace duckdb
