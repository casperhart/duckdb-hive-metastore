#pragma once

#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/function/table_function.hpp"
#include "hms_api.hpp"

namespace duckdb {

// Partition metadata resolved from HMS for one scan, shared (via shared_ptr)
// across the scan's MultiFileReader copies. `partition_key_names`/`_types` are
// the table's partition keys in declared order; each HMSAPIPartition's `values`
// are positional to that same order, and its `location` is where that
// partition's files actually live (possibly outside the table root).
struct HMSPartitionScanInfo {
	vector<string> partition_key_names;
	vector<LogicalType> partition_key_types;
	vector<HMSAPIPartition> partitions;
};

// Attached to the scan TableFunction (as function_info) so the
// get_multi_file_reader factory can hand the resolved partition set to each
// reader instance it builds.
struct HMSMultiFileReaderFunctionInfo : public TableFunctionInfo {
	explicit HMSMultiFileReaderFunctionInfo(shared_ptr<HMSPartitionScanInfo> info_p) : scan_info(std::move(info_p)) {
	}
	shared_ptr<HMSPartitionScanInfo> scan_info;
};

// A MultiFileReader that makes HMS authoritative for partition columns.
//
// Unlike DuckDB's default hive-partitioning (which infers partition columns and
// their values by parsing `key=value` segments out of file paths), this reader:
//   - appends the partition columns to the scan schema in the table's *declared*
//     order with the HMS-declared types (BindOptions), and
//   - fills each partition column's value from HMS, choosing the partition by
//     matching the data file to that partition's recorded location (FinalizeBind).
// This is what lets relocated / externally-located / non-`key=value` partitions
// read correctly, and gives a Spark-consistent column order.
class HMSMultiFileReader : public MultiFileReader {
public:
	explicit HMSMultiFileReader(shared_ptr<HMSPartitionScanInfo> scan_info_p) : scan_info(std::move(scan_info_p)) {
	}

	// Factory for TableFunction::get_multi_file_reader.
	static unique_ptr<MultiFileReader> CreateInstance(const TableFunction &table_function);

	void BindOptions(MultiFileOptions &options, MultiFileList &files, vector<LogicalType> &return_types,
	                 vector<string> &names, MultiFileReaderBindData &bind_data) override;

	void FinalizeBind(MultiFileReaderData &reader_data, const MultiFileOptions &file_options,
	                  const MultiFileReaderBindData &options, const vector<MultiFileColumnDefinition> &global_columns,
	                  const vector<ColumnIndex> &global_column_ids, ClientContext &context,
	                  optional_ptr<MultiFileReaderGlobalState> global_state) override;

	unique_ptr<MultiFileReader> Copy() const override;

private:
	// Returns the partition whose location is the longest prefix of `file_path`,
	// or nullptr if none matches.
	const HMSAPIPartition *FindPartitionForFile(const string &file_path) const;

	shared_ptr<HMSPartitionScanInfo> scan_info;
};

} // namespace duckdb
