#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/storage/storage_extension.hpp"

#include "storage/hms_catalog.hpp"
#include "storage/hms_transaction_manager.hpp"
#include "hive_metastore_extension.hpp"
#include "hms_config.hpp"

#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"

#include <thrift/Thrift.h>
#include <cstdlib>

namespace duckdb {

static unique_ptr<Catalog> HMSCatalogAttach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                            AttachedDatabase &db, const string &name, AttachInfo &info,
                                            AttachOptions &attach_options) {
	string default_schema;
	string warehouse_location;
	for (auto &entry : info.options) {
		auto lower_name = StringUtil::Lower(entry.first);
		if (lower_name == "type" || lower_name == "read_only") {
			// already handled
		} else if (lower_name == "default_schema") {
			default_schema = entry.second.ToString();
		} else if (lower_name == "warehouse_location") {
			warehouse_location = entry.second.ToString();
		} else {
			throw BinderException("Unrecognized option for HMS attach: %s", entry.first);
		}
	}

	if (default_schema.empty()) {
		default_schema = "default";
	}

	string catalog_name = "hive_metastore";
	return make_uniq<HMSCatalog>(db, info.path, attach_options, info.path, default_schema, warehouse_location,
	                             catalog_name);
}

static unique_ptr<TransactionManager> CreateTransactionManager(optional_ptr<StorageExtensionInfo> storage_info,
                                                               AttachedDatabase &db, Catalog &catalog) {
	auto &hms_catalog = catalog.Cast<HMSCatalog>();
	return make_uniq<HMSTransactionManager>(db, hms_catalog);
}

class HiveMetastoreStorageExtension : public StorageExtension {
public:
	HiveMetastoreStorageExtension() {
		attach = HMSCatalogAttach;
		create_transaction_manager = CreateTransactionManager;
	}
};

// True unless HMS_AUTOATTACH is set to a falsey value.
static bool AutoAttachEnabled() {
	const char *v = std::getenv("HMS_AUTOATTACH");
	if (v == nullptr || v[0] == '\0') {
		return true;
	}
	auto s = StringUtil::Lower(v);
	return !(s == "0" || s == "false" || s == "off" || s == "no");
}

// If a Hive/Hadoop config is discovered on the environment, attach the metastore
// automatically on LOAD so it's queryable without a manual ATTACH. When nothing
// is discovered this is a no-op and behaviour matches the original extension.
static void AutoAttachFromConfig(ExtensionLoader &loader) {
	if (!AutoAttachEnabled()) {
		return;
	}
	HMSSiteConfig site = HMSLoadSiteConfig();
	if (!site.found || site.metastore_uris.empty()) {
		return;
	}

	string name = "hive_metastore";
	const char *name_env = std::getenv("HMS_AUTOATTACH_NAME");
	if (name_env != nullptr && name_env[0] != '\0') {
		name = name_env;
	}

	// ATTACH '' re-discovers the URI list from config, so the auto-attached
	// catalog gets full HA failover. Best-effort and idempotent: a name clash
	// (e.g. re-LOAD, or the user already attached one) just errors and is
	// swallowed — the user can still attach manually, which surfaces any real
	// problem. ATTACH is lazy, so this never touches the metastore at load time.
	Connection con(loader.GetDatabaseInstance());
	auto result = con.Query("ATTACH '' AS \"" + name + "\" (TYPE hive_metastore)");
	(void)result;
}

static void LoadInternal(ExtensionLoader &loader) {
	// Thrift logs transport errors (e.g. connection-refused while failing over
	// across metastore URIs) to stderr by default. Our exceptions already carry
	// that detail, so silence the duplicate, alarming-looking noise.
	apache::thrift::GlobalOutput.setOutputFunction([](const char *) {});

	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	StorageExtension::Register(config, "hive_metastore", make_uniq<HiveMetastoreStorageExtension>());
	StorageExtension::Register(config, "hms_catalog", make_uniq<HiveMetastoreStorageExtension>());

	AutoAttachFromConfig(loader);
}

void HiveMetastoreExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string HiveMetastoreExtension::Name() {
	return "hive_metastore";
}

std::string HiveMetastoreExtension::Version() const {
#ifdef EXT_VERSION_HIVE_METASTORE
	return EXT_VERSION_HIVE_METASTORE;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(hive_metastore, loader) {
	duckdb::LoadInternal(loader);
}
}
