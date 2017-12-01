#include "table_scan.hpp"

#include <functional>
#include <limits>
#include <memory>
#include <vector>

#include "resolve_type.hpp"
#include "storage/base_column.hpp"
#include "storage/dictionary_column.hpp"
#include "storage/fitted_attribute_vector.hpp"
#include "storage/reference_column.hpp"
#include "storage/table.hpp"
#include "storage/value_column.hpp"
#include "table_scan_impl.hpp"

namespace opossum {

TableScan::TableScan(const std::shared_ptr<const AbstractOperator> in, ColumnID column_id, const ScanType scan_type,
                     const AllTypeVariant search_value)
    : AbstractOperator(in), _column_id{column_id}, _scan_type{scan_type}, _search_value{search_value} {}

ColumnID TableScan::column_id() const { return _column_id; }

ScanType TableScan::scan_type() const { return _scan_type; }

const AllTypeVariant& TableScan::search_value() const { return _search_value; }

std::shared_ptr<const Table> TableScan::_on_execute() {
  auto input_table = _input_table_left();
  Assert(input_table, "No output");
  const auto& column_type = input_table->column_type(_column_id);
  auto table_scan_impl = make_shared_by_column_type<BaseTableScanImpl, TypedTableScanImpl>(column_type, *this);

  auto pos_list_ptr = table_scan_impl->on_execute();

  // We assume:
  // 1. A table either consists of ReferenceColumns (X)OR Value- or DictionaryColumns.
  // 2. All ReferenceColumns in a table reference the same "original" table.
  // Therefore, we only need to handle one single PosList to create our new Table / ReferenceColumn.

  // If the input table consists of ReferenceColumns, take the original reference to avoid multiple indirections
  auto referenced_table = input_table;
  auto input_reference_column_ptr =
      std::dynamic_pointer_cast<ReferenceColumn>(input_table->get_chunk(ChunkID{0}).get_column(_column_id));
  if (input_reference_column_ptr) {
    referenced_table = input_reference_column_ptr->referenced_table();
  }

  auto output_table = std::make_shared<Table>();

  // Copy column definitions from referenced table
  for (ColumnID column_id{0}; column_id < referenced_table->col_count(); ++column_id) {
    output_table->add_column_definition(referenced_table->column_name(column_id),
                                        referenced_table->column_type(column_id));
  }

  // Add ReferenceColumns to first chunk (all using the same PosList)
  auto& chunk = output_table->get_chunk(ChunkID{0});
  for (ColumnID column_id{0}; column_id < referenced_table->col_count(); ++column_id) {
    auto reference_column = std::make_shared<ReferenceColumn>(referenced_table, column_id, pos_list_ptr);
    chunk.add_column(reference_column);
  }

  return output_table;
}

}  // namespace opossum
