#include "hms_config.hpp"

#include "duckdb/common/string_util.hpp"

#include <cstdlib>
#include <fstream>
#include <sstream>

namespace duckdb {

namespace {

// Read an env var into `out`; returns false if unset or empty.
bool GetEnv(const char *name, string &out) {
	const char *v = std::getenv(name);
	if (v == nullptr || v[0] == '\0') {
		return false;
	}
	out = string(v);
	return true;
}

// Read an entire local file into memory. Returns false if it cannot be opened.
bool ReadFile(const string &path, string &out) {
	std::ifstream in(path, std::ios::binary);
	if (!in.good()) {
		return false;
	}
	std::ostringstream ss;
	ss << in.rdbuf();
	out = ss.str();
	return true;
}

// Remove XML comments (<!-- ... -->) so their contents can't be mistaken for
// real <property> blocks. Hadoop config files routinely comment out examples.
string StripXmlComments(const string &in) {
	string out;
	out.reserve(in.size());
	size_t pos = 0;
	while (pos < in.size()) {
		size_t start = in.find("<!--", pos);
		if (start == string::npos) {
			out.append(in, pos, in.size() - pos);
			break;
		}
		out.append(in, pos, start - pos);
		size_t end = in.find("-->", start + 4);
		if (end == string::npos) {
			break; // unterminated comment: drop the rest
		}
		pos = end + 3;
	}
	return out;
}

// Decode the handful of XML entities that can legitimately appear in a config
// value (the comma-separated URI list occasionally uses &amp;).
string DecodeEntities(const string &in) {
	string out;
	out.reserve(in.size());
	for (size_t i = 0; i < in.size();) {
		if (in[i] == '&') {
			if (in.compare(i, 5, "&amp;") == 0) {
				out.push_back('&');
				i += 5;
				continue;
			}
			if (in.compare(i, 4, "&lt;") == 0) {
				out.push_back('<');
				i += 4;
				continue;
			}
			if (in.compare(i, 4, "&gt;") == 0) {
				out.push_back('>');
				i += 4;
				continue;
			}
			if (in.compare(i, 6, "&quot;") == 0) {
				out.push_back('"');
				i += 6;
				continue;
			}
		}
		out.push_back(in[i]);
		i++;
	}
	return out;
}

// Return the text between <tag> and </tag> inside `block`, or "" if absent.
// Hadoop config <name>/<value> elements never carry attributes, so a plain
// tag match is sufficient and keeps us free of an XML-library dependency.
string ExtractTag(const string &block, const string &tag) {
	const string open = "<" + tag + ">";
	const string close = "</" + tag + ">";
	size_t s = block.find(open);
	if (s == string::npos) {
		return "";
	}
	s += open.size();
	size_t e = block.find(close, s);
	if (e == string::npos) {
		return "";
	}
	return block.substr(s, e - s);
}

// StringUtil::Trim mutates in place; wrap it so we can trim by value.
string Trimmed(const string &s) {
	string copy = s;
	StringUtil::Trim(copy);
	return copy;
}

bool ParseBool(const string &v) {
	return StringUtil::Lower(Trimmed(v)) == "true";
}

vector<string> SplitCsv(const string &v) {
	vector<string> out;
	for (auto &part : StringUtil::Split(v, ",")) {
		auto trimmed = Trimmed(part);
		if (!trimmed.empty()) {
			out.push_back(trimmed);
		}
	}
	return out;
}

} // namespace

HMSSiteConfig HMSParseSiteConfig(const string &xml_in) {
	HMSSiteConfig cfg;
	string xml = StripXmlComments(xml_in);

	size_t pos = 0;
	while (true) {
		size_t p = xml.find("<property", pos);
		if (p == string::npos) {
			break;
		}
		size_t pend = xml.find("</property>", p);
		if (pend == string::npos) {
			break;
		}
		string block = xml.substr(p, pend - p);
		pos = pend + 11; // length of "</property>"

		string name = Trimmed(ExtractTag(block, "name"));
		if (name.empty()) {
			continue;
		}
		string value = Trimmed(DecodeEntities(ExtractTag(block, "value")));

		if (name == "hive.metastore.uris") {
			cfg.metastore_uris = SplitCsv(value);
		} else if (name == "hive.metastore.sasl.enabled") {
			cfg.sasl_enabled = ParseBool(value);
			cfg.sasl_set = true;
		} else if (name == "hive.metastore.kerberos.principal") {
			cfg.kerberos_principal = value;
		} else if (name == "hadoop.security.authentication") {
			cfg.hadoop_auth = StringUtil::Lower(value);
		}
	}
	return cfg;
}

namespace {

// Candidate config directories, most specific first — mirrors how the
// Hadoop/Hive clients resolve their configuration.
vector<string> CandidateConfigDirs() {
	vector<string> dirs;
	string env;
	if (GetEnv("HIVE_CONF_DIR", env)) {
		dirs.push_back(env);
	}
	if (GetEnv("HADOOP_CONF_DIR", env)) {
		dirs.push_back(env);
	}
	if (GetEnv("HIVE_HOME", env)) {
		dirs.push_back(env + "/conf");
	}
	if (GetEnv("HADOOP_HOME", env)) {
		dirs.push_back(env + "/etc/hadoop");
	}
	// Common package-install locations as a last resort.
	dirs.push_back("/etc/hive/conf");
	dirs.push_back("/etc/hadoop/conf");
	return dirs;
}

// Read the first `name` file found across `dirs`; returns "" if none exists.
// `found_path` receives the path that was read.
bool ReadFirst(const vector<string> &dirs, const char *name, string &contents, string &found_path) {
	for (auto &dir : dirs) {
		string path = dir + "/" + name;
		if (ReadFile(path, contents)) {
			found_path = path;
			return true;
		}
	}
	return false;
}

} // namespace

HMSSiteConfig HMSLoadSiteConfig() {
	auto dirs = CandidateConfigDirs();

	// hive-site.xml carries the metastore URI + Hive-specific security settings.
	HMSSiteConfig cfg;
	string contents, path;
	if (ReadFirst(dirs, "hive-site.xml", contents, path)) {
		cfg = HMSParseSiteConfig(contents);
		cfg.found = true;
		cfg.source_path = path;
	}

	// core-site.xml supplements hadoop.security.authentication (kerberos) when
	// hive-site.xml doesn't set it directly. A core-site.xml alone (no
	// hive-site.xml) still tells us the cluster is kerberized, so it counts as a
	// discovered config — an explicit ATTACH endpoint then authenticates with
	// SASL instead of failing against the secured metastore in plaintext.
	string core_contents, core_path;
	if (cfg.hadoop_auth.empty() && ReadFirst(dirs, "core-site.xml", core_contents, core_path)) {
		HMSSiteConfig core = HMSParseSiteConfig(core_contents);
		cfg.hadoop_auth = core.hadoop_auth;
		if (!cfg.found && !cfg.hadoop_auth.empty()) {
			cfg.found = true;
			cfg.source_path = core_path;
		}
	}

	// A Kerberized cluster (hadoop.security.authentication=kerberos) implies SASL
	// when hive.metastore.sasl.enabled is not set explicitly; an explicit
	// hive.metastore.sasl.enabled=false (an unsecured metastore inside a
	// kerberized cluster) wins over the cluster-wide default.
	if (!cfg.sasl_set && cfg.hadoop_auth == "kerberos") {
		cfg.sasl_enabled = true;
	}
	return cfg;
}

void HMSKerberosPrincipalParts(const string &principal, string &service, string &instance) {
	// principal is "primary/instance@REALM"; both '/' and '@' parts are optional.
	// Default the service to "hive" (the metastore convention).
	service = "hive";
	instance = "";
	if (principal.empty()) {
		return;
	}
	size_t at = principal.find('@');
	string without_realm = principal.substr(0, at);
	size_t slash = without_realm.find('/');
	string primary = (slash == string::npos) ? without_realm : without_realm.substr(0, slash);
	if (!primary.empty()) {
		service = primary;
	}
	if (slash != string::npos) {
		instance = without_realm.substr(slash + 1);
	}
}

} // namespace duckdb
