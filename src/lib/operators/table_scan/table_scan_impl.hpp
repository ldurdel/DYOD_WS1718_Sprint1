#pragma once

#include <memory>
#include <vector>

#include "storage/base_attribute_vector.hpp"
#include "storage/base_column.hpp"
#include "table_scan.hpp"
#include "types.hpp"
#include "vector_scan.hpp"

namespace opossum {

template <typename T>
struct IdentityGetter {
  const T& operator()(const T& element) const { return element; }
};

template <typename T>
struct ReferenceGetter {
  explicit ReferenceGetter(const Table& table, ColumnID column_id)
      : _table{table},
        _column_id{column_id},
        _is_last_chunk_valid{false},
        _last_chunk_id{0},
        _last_value_ptr{nullptr},
        _last_dictionary_ptr{nullptr} {}
  const T& operator()(const RowID& row_id) const {
    if (!_is_last_chunk_valid || _last_chunk_id != row_id.chunk_id) {
      const Chunk& chunk = _table.get_chunk(row_id.chunk_id);
      const auto& column_ptr = chunk.get_column(_column_id);

      _last_value_ptr = std::dynamic_pointer_cast<ValueColumn<T>>(column_ptr);
      _last_dictionary_ptr = std::dynamic_pointer_cast<DictionaryColumn<T>>(column_ptr);
      _last_chunk_id = row_id.chunk_id;

      _is_last_chunk_valid = true;  // TODO is there an INVALID_CHUNK_ID?!
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
  const T& _get_from_value_column(std::shared_ptr<ValueColumn<T>> value_column, ChunkOffset chunk_offset) const {
    return value_column->values()[chunk_offset];
  }

  const T& _get_from_dictionary_column(std::shared_ptr<DictionaryColumn<T>> dictionary_column,
                                       ChunkOffset chunk_offset) const {
    // TODO anyone: This is slow, but DictionaryColumn does not provide a faster access method
    return dictionary_column->get(chunk_offset);
  }

  const Table& _table;
  const ColumnID _column_id;

  mutable bool _is_last_chunk_valid;
  mutable ChunkID _last_chunk_id;
  mutable std::shared_ptr<ValueColumn<T>> _last_value_ptr;
  mutable std::shared_ptr<DictionaryColumn<T>> _last_dictionary_ptr;
};

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
  template <typename FittedT>
  void _scan_attribute_vector(std::shared_ptr<const BaseAttributeVector> attribute_vector, ValueID compare_value_id,
                              ChunkID chunk_id, ScanType scan_type) {
    IdentityGetter<FittedT> identity_getter;
    auto fitted_comp_value_id = static_cast<FittedT>(compare_value_id);
    auto attribute_vector_ptr = std::dynamic_pointer_cast<const FittedAttributeVector<FittedT>>(attribute_vector);
    vector_scan<FittedT, FittedT, IdentityGetter<FittedT>>(attribute_vector_ptr->values(), identity_getter,
                                                           fitted_comp_value_id, *_pos_list, chunk_id, scan_type);
  }

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
    vector_scan<T, T, IdentityGetter<T>>(values, identity_getter, _search_value, *_pos_list, chunk_id, _scan_type);
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

    switch (column->attribute_vector()->width()) {
      case 1: {
        _scan_attribute_vector<uint8_t>(column->attribute_vector(), comp_value_id, chunk_id, dictionary_scan_type);
        break;
      }
      case 2: {
        _scan_attribute_vector<uint16_t>(column->attribute_vector(), comp_value_id, chunk_id, dictionary_scan_type);
        break;
      }
      case 4: {
        _scan_attribute_vector<uint32_t>(column->attribute_vector(), comp_value_id, chunk_id, dictionary_scan_type);
        break;
      }
      default:
        throw std::runtime_error("Unknown underlying type for attribute vector");
    }
  }

  void _process_reference_column(ChunkID chunk_id, std::shared_ptr<ReferenceColumn> column) {
    ReferenceGetter<T> reference_getter{*column->referenced_table(), column->referenced_column_id()};
    vector_scan<RowID, T, ReferenceGetter<T>>(*column->pos_list(), reference_getter, _search_value, *_pos_list,
                                              chunk_id, _scan_type);
  }

  std::shared_ptr<PosList> _pos_list;
  const T _search_value;
  const ScanType _scan_type;
};

}  // namespace opossum
