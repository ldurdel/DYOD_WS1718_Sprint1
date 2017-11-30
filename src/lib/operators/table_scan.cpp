#include "table_scan.hpp"

#include <memory>

#include "resolve_type.hpp"
#include "storage/table.hpp"

#include "storage/base_column.hpp"
#include "storage/dictionary_column.hpp"
#include "storage/fitted_attribute_vector.hpp"
#include "storage/reference_column.hpp"
#include "storage/value_column.hpp"

namespace opossum {

template <typename T>
struct IdentityGetter {
    const T& operator()(const T & element) {
        return element;
    }
};

template <typename T>
struct ReferenceGetter {
    explicit ReferenceGetter(const Table & table, ColumnID column_id) : _table{table}, _column_id{column_id} {}
    const T& operator()(const RowID & row_id) {
        if (_last_chunk_id != row_id.chunk_id)
        {
            const Chunk& chunk = _table.get_chunk(row_id.chunk_id);
            const auto & column_ptr = chunk.get_column(_column_id);

            _last_value_ptr = std::dynamic_pointer_cast<ValueColumn<T>>(column_ptr);
            _last_dictionary_ptr = std::dynamic_pointer_cast<DictionaryColumn<T>>(column_ptr);
            _last_chunk_id = row_id.chunk_id;
        }

        if (_last_value_ptr) {
            return _get_from_value_column(_last_value_ptr, row_id.chunk_offset);
        }

        if (_last_dictionary_ptr) {
            return _get_from_dictionary_column(_last_dictionary_ptr, row_id.chunk_offset);
        }

        throw std::runtime_error("Unknown referenced column type");
    }
protected:
    const T& _get_from_value_column(std::shared_ptr<ValueColumn<T>> value_column, ChunkOffset chunk_offset) {
        return value_column->values()[chunk_offset];
    }

    const T& _get_from_dictionary_column(std::shared_ptr<DictionaryColumn<T>> dictionary_column, ChunkOffset chunk_offset) {
        // TODO FIXME this is slow
        return dictionary_column->get(chunk_offset);
    }

    const Table& _table;
    const ColumnID _column_id;

    mutable ChunkID _last_chunk_id;
    mutable std::shared_ptr<ValueColumn<T>> _last_value_ptr;
    mutable std::shared_ptr<DictionaryColumn<T>> _last_dictionary_ptr;
};

template <typename T, typename Getter, typename CompType>
void vector_scan_impl(const std::vector<T> & values, const Getter & getter, const T& compare_value, PosList & pos_list, ChunkID chunk_id) {
    CompType comparator;
    for (ChunkOffset chunk_offset{0}; chunk_offset < values.size(); ++chunk_offset) {
      const auto& value = getter(values[chunk_offset]);
      if (comparator(value, compare_value)) {
            pos_list.emplace_back(RowID{chunk_id, chunk_offset});
        }
    }
}

template <typename T, typename Getter, typename CompType>
void vector_scan_impl(const std::vector<RowID> & values, const Getter & getter, const T& compare_value, PosList & pos_list, ChunkID chunk_id) {
    CompType comparator;
    for (const RowID & row_id : values) {
      const auto& value = getter(row_id);
      if (comparator(value, compare_value)) {
            pos_list.emplace_back(row_id);
        }
    }
}

template <typename T, typename Getter>
void vector_scan(const std::vector<T> & values, const Getter & getter, const T& compare_value, PosList & pos_list, ChunkID chunk_id, ScanType scan_type) {
    switch (scan_type) {
      case ScanType::OpEquals:
        vector_scan_impl<T, Getter, std::equal_to<T>>(values, getter, compare_value, pos_list, chunk_id);
        break;
      case ScanType::OpNotEquals:
        vector_scan_impl<T, Getter, std::not_equal_to<T>>(values, getter, compare_value, pos_list, chunk_id);
        break;
      case ScanType::OpLessThan:
        vector_scan_impl<T, Getter, std::less<T>>(values, getter, compare_value, pos_list, chunk_id);
        break;
      case ScanType::OpLessThanEquals:
        vector_scan_impl<T, Getter, std::less_equal<T>>(values, getter, compare_value, pos_list, chunk_id);
        break;
      case ScanType::OpGreaterThan:
        vector_scan_impl<T, Getter, std::greater<T>>(values, getter, compare_value, pos_list, chunk_id);
        break;
      case ScanType::OpGreaterThanEquals:
        vector_scan_impl<T, Getter, std::greater_equal<T>>(values, getter, compare_value, pos_list, chunk_id);
        break;
      default:
        throw std::runtime_error("Operator not known");
    }
}

class BaseTableScanImpl {
 public:
  explicit BaseTableScanImpl(const TableScan& table_scan) : _table_scan{table_scan} {}

  virtual const std::shared_ptr<const PosList> on_execute() = 0;

 protected:
  const TableScan& _table_scan;
};

template <typename T>
class TypedTableScanImpl : public BaseTableScanImpl {
 public:
  explicit TypedTableScanImpl(const TableScan& table_scan)
      : BaseTableScanImpl{table_scan},
        _search_value{type_cast<T>(table_scan.search_value())},
        _scan_type{table_scan.scan_type()} {}

  const std::shared_ptr<const PosList> on_execute() override {
    _pos_list = std::make_shared<PosList>();

    auto input_table_ptr = _table_scan.input_left()->get_output();

    for (ChunkID chunk_id{0}; chunk_id < input_table_ptr->chunk_count(); ++chunk_id) {
      const auto& chunk = input_table_ptr->get_chunk(chunk_id);
      _process_column(chunk_id, chunk.get_column(_table_scan.column_id()));
    }

    return _pos_list;
  }

 protected:

  void _process_column(ChunkID chunk_id, std::shared_ptr<BaseColumn> column) {
    auto value_column_ptr = std::dynamic_pointer_cast<ValueColumn<T>>(column);
    if (value_column_ptr) {
      return _process_value_column(chunk_id, value_column_ptr);
    }

    auto dictionary_column_ptr = std::dynamic_pointer_cast<DictionaryColumn<T>>(column);
    if (dictionary_column_ptr) {
      return _process_dictionary_column(chunk_id, dictionary_column_ptr);
    }

    auto reference_column_ptr = std::dynamic_pointer_cast<ReferenceColumn>(column);
    if (reference_column_ptr) {
      return _process_reference_column(chunk_id, reference_column_ptr);
    }

    throw std::runtime_error("Unknown column type");
  }

  void _process_value_column(ChunkID chunk_id, std::shared_ptr<ValueColumn<T>> column) {
    const auto& values = column->values();
    IdentityGetter<T> identity_getter;
    vector_scan(values, identity_getter, _search_value, *_pos_list, chunk_id, _scan_type);
  }

  void _process_dictionary_column(ChunkID chunk_id, std::shared_ptr<DictionaryColumn<T>> column) {
    const auto& value_ids = column->attribute_vector();
    const auto& lower_bound_value_id = column->lower_bound(_search_value);

    auto dictionary_scan_type = _scan_type;
    auto comp_value_id = lower_bound_value_id;

    // if lower_bound returns INVALID_VALUE_ID, either return all values or no value at all
    if (lower_bound_value_id == INVALID_VALUE_ID) {
      switch (dictionary_scan_type) {
        case ScanType::OpEquals:
        case ScanType::OpGreaterThan:
        case ScanType::OpGreaterThanEquals:
          // Early exit
          return;
          break;
        case ScanType::OpLessThan:
        case ScanType::OpLessThanEquals:
        case ScanType::OpNotEquals:
          // Return all
          dictionary_scan_type = ScanType::OpNotEquals;
          comp_value_id = INVALID_VALUE_ID;
          break;
        default:
          throw std::runtime_error("unknown scan type");
          break;
      }
    } else {
      // lower_bound returned an existing value
      auto lower_bound_equals_search_value = column->value_by_value_id(lower_bound_value_id) == _search_value;
      comp_value_id = lower_bound_value_id;

      // If value ID returned by lower_bound does not correspond to original search value, we need to modify our search value and / or operator
      switch (dictionary_scan_type) {
        case ScanType::OpEquals:
          if (!lower_bound_equals_search_value) {
            // Value does not exist in chunk -- early exit
            return;
          }
          break;
        case ScanType::OpGreaterThan:
          if (!lower_bound_equals_search_value) {
            dictionary_scan_type = ScanType::OpGreaterThanEquals;
          }
          break;
        case ScanType::OpGreaterThanEquals:
          break;
        case ScanType::OpLessThan:
          break;
        case ScanType::OpLessThanEquals:
          if (!lower_bound_equals_search_value) {
            dictionary_scan_type = ScanType::OpLessThan;
          }
          break;
        case ScanType::OpNotEquals:
          if (!lower_bound_equals_search_value) {
            // Simply dump all values from this chunk
            comp_value_id = INVALID_VALUE_ID;
          }
          break;
        default:
          throw std::runtime_error("unknown scan type");
          break;
      }
    }

    switch(column->attribute_vector()->width()) {
    case 1:
    {
        IdentityGetter<uint8_t> identity_getter;
        auto attribute_vector_ptr = std::dynamic_pointer_cast<FittedAttributeVector<uint8_t>>(column->attribute_vector());
        vector_scan(attribute_vector_ptr->values(), identity_getter, comp_value_id, _pos_list, chunk_id, dictionary_scan_type);
        break;
     }
    case 2:
    {
        IdentityGetter<uint16_t> identity_getter;
        auto attribute_vector_ptr = std::dynamic_pointer_cast<FittedAttributeVector<uint16_t>>(column->attribute_vector());
        vector_scan(attribute_vector_ptr->values(), identity_getter, comp_value_id, _pos_list, chunk_id, dictionary_scan_type);
        break;
     }
    case 4:
    {
        IdentityGetter<uint32_t> identity_getter;
        auto attribute_vector_ptr = std::dynamic_pointer_cast<FittedAttributeVector<uint32_t>>(column->attribute_vector());
        vector_scan(attribute_vector_ptr->values(), identity_getter, comp_value_id, _pos_list, chunk_id, dictionary_scan_type);
        break;
     }
    default:
        throw std::runtime_error("Unknown underlying type for attribute vector");
    }
  }

  void _process_reference_column(ChunkID chunk_id, std::shared_ptr<ReferenceColumn> column) {
    ReferenceGetter<T> reference_getter{*column->referenced_table(), column->referenced_column_id()};
    vector_scan(*column->pos_list(), reference_getter, _search_value, _pos_list, chunk_id, _scan_type);
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
