#include "storage/hms_multi_file_reader.hpp"

#include "duckdb/common/hive_partitioning.hpp"
#include "duckdb/common/multi_file/multi_file_data.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

unique_ptr<MultiFileReader> HMSMultiFileReader::CreateInstance(const TableFunction &table_function) {
	if (!table_function.function_info) {
		throw InternalException("HMSMultiFileReader::CreateInstance called without HMS partition function info");
	}
	auto &info = table_function.function_info->Cast<HMSMultiFileReaderFunctionInfo>();
	auto reader = make_uniq<HMSMultiFileReader>(info.scan_info);
	reader->function_name = table_function.name;
	return std::move(reader);
}

unique_ptr<MultiFileReader> HMSMultiFileReader::Copy() const {
	auto reader = make_uniq<HMSMultiFileReader>(scan_info);
	reader->function_name = function_name;
	return std::move(reader);
}

void HMSMultiFileReader::BindOptions(MultiFileOptions &options, MultiFileList &files, vector<LogicalType> &return_types,
                                     vector<string> &names, MultiFileReaderBindData &bind_data) {
	// Optional generated filename column (mirror the base reader so this feature
	// keeps working if a caller ever enables it).
	if (options.filename) {
		if (std::find(names.begin(), names.end(), options.filename_column) != names.end()) {
			throw BinderException("Option filename adds column \"%s\", but a column with this name is also in the "
			                      "file. Try setting a different name: filename='<filename column name>'",
			                      options.filename_column);
		}
		bind_data.filename_idx = names.size();
		return_types.emplace_back(LogicalType::VARCHAR);
		names.emplace_back(options.filename_column);
	}

	// Append the partition columns in HMS's declared order, with HMS types. We
	// deliberately do NOT use options.hive_partitioning (path inference): the
	// values come from HMS in FinalizeBind. Record each as a hive-partitioning
	// index so the rest of the pipeline treats it as a generated (constant)
	// column that need not exist in the data files.
	for (idx_t k = 0; k < scan_info->partition_key_names.size(); k++) {
		auto &key_name = scan_info->partition_key_names[k];
		auto &key_type = scan_info->partition_key_types[k];
		idx_t global_index;
		auto lookup = std::find_if(names.begin(), names.end(),
		                           [&](const string &col_name) { return StringUtil::CIEquals(col_name, key_name); });
		if (lookup != names.end()) {
			// Also physically present in the files: override its type and reuse it.
			global_index = NumericCast<idx_t>(lookup - names.begin());
			return_types[global_index] = key_type;
		} else {
			global_index = names.size();
			return_types.emplace_back(key_type);
			names.emplace_back(key_name);
		}
		bind_data.hive_partitioning_indexes.emplace_back(key_name, global_index);
	}
}

const HMSAPIPartition *HMSMultiFileReader::FindPartitionForFile(const string &file_path) const {
	const HMSAPIPartition *best = nullptr;
	idx_t best_len = 0;
	for (auto &partition : scan_info->partitions) {
		auto &loc = partition.location;
		if (loc.empty() || file_path.size() < loc.size()) {
			continue;
		}
		if (file_path.compare(0, loc.size(), loc) == 0) {
			if (loc.size() > best_len) {
				best = &partition;
				best_len = loc.size();
			}
		}
	}
	return best;
}

void HMSMultiFileReader::FinalizeBind(MultiFileReaderData &reader_data, const MultiFileOptions &file_options,
                                      const MultiFileReaderBindData &options,
                                      const vector<MultiFileColumnDefinition> &global_columns,
                                      const vector<ColumnIndex> &global_column_ids, ClientContext &context,
                                      optional_ptr<MultiFileReaderGlobalState> global_state) {
	auto &filename = reader_data.reader->GetFileName();

	// Which HMS partition does this file belong to? Matched by location prefix,
	// since HMS records each partition's real storage path.
	const HMSAPIPartition *partition = FindPartitionForFile(filename);

	// name -> declared-order ordinal, to line partition columns up with the
	// partition's `values` array.
	case_insensitive_map_t<idx_t> key_ordinal;
	for (idx_t k = 0; k < scan_info->partition_key_names.size(); k++) {
		key_ordinal[scan_info->partition_key_names[k]] = k;
	}

	for (idx_t i = 0; i < global_column_ids.size(); i++) {
		auto global_idx = MultiFileGlobalIndex(i);
		auto &col_id = global_column_ids[i];
		auto column_id = col_id.GetPrimaryIndex();

		if ((options.filename_idx.IsValid() && column_id == options.filename_idx.GetIndex()) ||
		    column_id == MultiFileReader::COLUMN_IDENTIFIER_FILENAME) {
			reader_data.constant_map.Add(global_idx, Value(filename));
			continue;
		}
		if (column_id == MultiFileReader::COLUMN_IDENTIFIER_FILE_INDEX) {
			reader_data.constant_map.Add(global_idx, Value::UBIGINT(reader_data.reader->file_list_idx.GetIndex()));
			continue;
		}
		if (IsVirtualColumn(column_id)) {
			continue;
		}

		// Partition column? Supply the value HMS recorded for this file's partition.
		bool handled = false;
		for (auto &entry : options.hive_partitioning_indexes) {
			if (column_id != entry.index) {
				continue;
			}
			auto ord_it = key_ordinal.find(entry.value);
			if (ord_it == key_ordinal.end()) {
				break;
			}
			idx_t ordinal = ord_it->second;
			auto &type = scan_info->partition_key_types[ordinal];
			Value value;
			if (partition && ordinal < partition->values.size()) {
				value = HivePartitioning::GetValue(context, entry.value, partition->values[ordinal], type);
			} else {
				// No matching partition / missing value: expose NULL rather than fail.
				value = Value(type);
			}
			reader_data.constant_map.Add(global_idx, value);
			handled = true;
			break;
		}
		if (handled) {
			continue;
		}
	}
}

} // namespace duckdb
