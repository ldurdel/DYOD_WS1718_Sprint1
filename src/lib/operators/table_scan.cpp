#include "table_scan.hpp"

#include "resolve_type.hpp"
#include "storage/table.hpp"

#include "storage/base_column.hpp"
#include "storage/dictionary_column.hpp"
#include "storage/reference_column.hpp"
#include "storage/value_column.hpp"

namespace opossum {

class BaseTableScanImpl {
 public:
  BaseTableScanImpl(const TableScan& table_scan) : _table_scan{table_scan} {}

  virtual const std::shared_ptr<const PosList> on_execute() = 0;

 protected:
  const TableScan& _table_scan;
};

template <typename T>
class TypedTableScanImpl : public BaseTableScanImpl {
 public:
  TypedTableScanImpl(const TableScan& table_scan)
      : BaseTableScanImpl{table_scan}, _search_value{type_cast<T>(table_scan.search_value())} {}

  virtual const std::shared_ptr<const PosList> on_execute() override {
    _pos_list = std::make_shared<PosList>();

    auto input_table_ptr = _table_scan.input_left()->get_output();

    for (ChunkID chunk_id{0}; chunk_id < input_table_ptr->chunk_count(); ++chunk_id) {
      const auto& chunk = input_table_ptr->get_chunk(chunk_id);
      _process_column(chunk_id, chunk.get_column(_table_scan.column_id()));
    }

    return _pos_list;
  }

 protected:
  bool _compare(const T& lhs, const T& rhs) {
    switch (_scan_type) {
      case OpEquals:
        return lhs == rhs;
        break;
      case OpNotEquals:
        return lhs != rhs;
        break;
      case OpLessThan:
        return lhs < rhs;
        break;
      case OpLessThanEquals:
        return lhs <= rhs;
        break;
      case OpGreaterThan:
        return lhs > rhs;
        break;
      case OpGreaterThanEquals:
        return lhs >= rhs;
        break;
      default:
        throw std::runtime_error("Operator not known");
    }
  }

  void _process_column(ChunkID chunk_id, std::shared_ptr<BaseColumn> column) {
    auto value_column_ptr = std::dynamic_pointer_cast<ValueColumn<T>>(column);
    if (value_column_ptr) {
      return _process_value_column(value_column_ptr);
    }

    auto dictionary_column_ptr = std::dynamic_pointer_cast<DictionaryColumn<T>>(column);
    if (dictionary_column_ptr) {
      return _process_dictionary_column(dictionary_column_ptr);
    }

    auto reference_column_ptr = std::dynamic_pointer_cast<ReferenceColumn>(column);
    if (reference_column_ptr) {
      return _process_reference_column(reference_column_ptr);
    }

    throw std::runtime_error("Unknown column type");
  }

  void _process_value_column(ChunkID chunk_id, std::shared_ptr<ValueColumn<T>> column) {
    const auto& values = column->values();
    for (ChunkOffset chunk_offset{0}; chunk_offset < column->size(); ++chunk_offset) {
      const auto& value = values[chunk_offset];
      if (_compare(value, _search_value)) {
        _pos_list->emplace_back(chunk_id, chunk_offset);
      }
    }
  }

  void _process_dictionary_column(ChunkID chunk_id, std::shared_ptr<DictionaryColumn<T>> column) {
    const auto& value_ids = column->attribute_vector();
    const auto& search_value_id = column->lower_bound(_search_value);
    for (ChunkOffset chunk_offset{0}; chunk_offset < column->size(); ++chunk_offset) {
      const auto& value = value_ids[chunk_offset];
      if (_compare(value, search_value_id)) {
        _pos_list->emplace_back(chunk_id, chunk_offset);
      }
    }
  }

  void _process_reference_column(ChunkID chunk_id, std::shared_ptr<ReferenceColumn> column) {
    auto table = column->referenced_table();

    for (const auto& row_id : column->pos_list()) {
      const auto& chunk = table->get_chunk(row_id.chunk_id);
      auto referenced_column_ptr = chunk.get_column(column->referenced_column_id());

      auto value_column_ptr = std::dynamic_pointer_cast<ValueColumn<T>>(referenced_column_ptr);
      if (value_column_ptr) {
        const auto& value = value_column_ptr->values()[row_id.chunk_offset];
        if (_compare(value, search_value)) {
          _pos_list->emplace_back(row_id);
        }

        continue;
      }

      auto dictionary_column_ptr = std::dynamic_pointer_cast<DictionaryColumn<T>>(referenced_column_ptr);
      if (dictionary_column_ptr) {
        const auto& value_id = dictionary_column_ptr->attribute_vector()[row_id.chunk_offset];
        const auto& value = dictionary_column_ptr->dictionary()[value_id];
        if (_compare(value, search_value)) {
          _pos_list->emplace_back(row_id);
        }

        continue;
      }

      throw std::runtime_error("Unknown referenced column type");
    }
  }

  std::shared_ptr<PosList> _pos_list;
  const T _search_value;
  const ScanType _scan_type;
};

TableScan::TableScan(const std::shared_ptr<const AbstractOperator> in, ColumnID column_id, const ScanType scan_type,
                     const AllTypeVariant search_value)
    : AbstractOperator(in), _column_id{column_id}, _scan_type{scan_type}, _search_value{search_value} {}

ColumnID TableScan::column_id() const { return _column_id; }

ScanType TableScan::scan_type() const { return _scan_type; }

const AllTypeVariant& TableScan::search_value() const { return _search_value; }

std::shared_ptr<const Table> TableScan::_on_execute() {
  auto input_table = _input_left->get_output();
  Assert(input_table, "No output");
  const auto& column_type = input_table->column_type(_column_id);
  auto table_scan_impl = make_shared_by_column_type<BaseTableScanImpl, TypedTableScanImpl>(column_type, *this);

  //    // TODO wrap PosList in new table
  //    auto pos_list_ptr = table_scan_impl->on_execute();

  ////    auto reference_column = make_shared_by_column_type<BaseColumn, ReferenceColumn>();
  ////    // const std::shared_ptr<const Table> referenced_table, const ColumnID referenced_column_id,
  ////    //const std::shared_ptr<const PosList> pos

  //    auto output_table = std::make_shared<Table>();

  //    auto & chunk = output_table->get_chunk(0);

  //    // TODO get referenced table pointer if input table contains ReferenceColumn

  //    for (ColumnID column_id{0}; column_id < input_table->col_count(); ++column_id) {

  //    }

  //    auto chunk = Chunk{};

  //    chunk.
}

}  // namespace opossum
