#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "storage/base_attribute_vector.hpp"
#include "storage/base_column.hpp"
#include "table_scan.hpp"
#include "types.hpp"
#include "value_getter.hpp"
#include "vector_scan.hpp"

namespace opossum {

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
      _scan_column(chunk_id, chunk.get_column(_table_scan.column_id()));
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
    vector_scan(attribute_vector_ptr->values(), identity_getter, fitted_comp_value_id, *_pos_list, chunk_id, scan_type);
  }

  // Dispatch column-type specific scan operations
  void _scan_column(ChunkID chunk_id, std::shared_ptr<BaseColumn> column) {
    auto value_column_ptr = std::dynamic_pointer_cast<ValueColumn<T>>(column);
    if (value_column_ptr) {
      return _scan_value_column(chunk_id, value_column_ptr);
    }

    auto dictionary_column_ptr = std::dynamic_pointer_cast<DictionaryColumn<T>>(column);
    if (dictionary_column_ptr) {
      return _scan_dictionary_column(chunk_id, dictionary_column_ptr);
    }

    auto reference_column_ptr = std::dynamic_pointer_cast<ReferenceColumn>(column);
    if (reference_column_ptr) {
      return _scan_reference_column(chunk_id, reference_column_ptr);
    }

    throw std::runtime_error("Unknown column type");
  }

  void _scan_value_column(ChunkID chunk_id, std::shared_ptr<ValueColumn<T>> column) {
    const auto& values = column->values();
    IdentityGetter<T> identity_getter;
    vector_scan(values, identity_getter, _search_value, *_pos_list, chunk_id, _scan_type);
  }

  // Determines scan parameters to be used when scanning a DictionaryColumn by
  // looking up the compare value in the dictionary.
  std::pair<ValueID, ScanType> _determine_attribute_vector_scan(std::shared_ptr<DictionaryColumn<T>> column) {
    // Early exit for trivial cases
    if (_scan_type == ScanType::OpAll)
    {
        return std::make_pair(INVALID_VALUE_ID, ScanType::OpAll);
    }
    if (_scan_type == ScanType::OpNone)
    {
        return std::make_pair(INVALID_VALUE_ID, ScanType::OpNone);
    }

    const auto& lower_bound_value_id = column->lower_bound(_search_value);
    auto dictionary_scan_type = _scan_type;

    // If lower_bound returns INVALID_VALUE_ID, no value in the dictionary is >= comp_value
    // => Either return all values or no value at all
    if (lower_bound_value_id == INVALID_VALUE_ID) {
      switch (dictionary_scan_type) {
        case ScanType::OpEquals:
        case ScanType::OpGreaterThan:
        case ScanType::OpGreaterThanEquals:
          return std::make_pair(INVALID_VALUE_ID, ScanType::OpNone);
        case ScanType::OpLessThan:
        case ScanType::OpLessThanEquals:
        case ScanType::OpNotEquals:
          return std::make_pair(INVALID_VALUE_ID, ScanType::OpAll);
        default:
          throw std::runtime_error("unknown scan type");
          break;
      }
    }

    // lower_bound returned an existing value
    // "new" comparison value is value at lower bound
    auto value_at_lower_bound = column->value_by_value_id(lower_bound_value_id);

    // If value ID returned by lower_bound does not correspond to original search
    // value, we need to modify our search value and / or operator
    if (value_at_lower_bound != _search_value) {
      switch (dictionary_scan_type) {
        case ScanType::OpEquals:
          return std::make_pair(INVALID_VALUE_ID, ScanType::OpNone);
        case ScanType::OpGreaterThan:
        case ScanType::OpGreaterThanEquals:
          return std::make_pair(lower_bound_value_id, ScanType::OpGreaterThanEquals);
        case ScanType::OpLessThan:
        case ScanType::OpLessThanEquals:
          return std::make_pair(lower_bound_value_id, ScanType::OpLessThan);
        case ScanType::OpNotEquals:
          return std::make_pair(INVALID_VALUE_ID, ScanType::OpAll);
        default:
          throw std::runtime_error("unknown scan type");
          break;
      }
    }

    // ValueID returned by lower_bound does indeed correspond to original search
    return std::make_pair(lower_bound_value_id, dictionary_scan_type);
  }

  void _scan_dictionary_column(ChunkID chunk_id, std::shared_ptr<DictionaryColumn<T>> column) {
    auto scan_parameters = _determine_attribute_vector_scan(column);
    auto attribute_vector_comp_value = scan_parameters.first;
    auto attribute_vector_scan_type = scan_parameters.second;

    // A FittedAttributeVector can only be scanned efficiently if its specific underlying type is known
    switch (column->attribute_vector()->width()) {
      case 1: {
        _scan_attribute_vector<uint8_t>(column->attribute_vector(), attribute_vector_comp_value, chunk_id,
                                        attribute_vector_scan_type);
        break;
      }
      case 2: {
        _scan_attribute_vector<uint16_t>(column->attribute_vector(), attribute_vector_comp_value, chunk_id,
                                         attribute_vector_scan_type);
        break;
      }
      case 4: {
        _scan_attribute_vector<uint32_t>(column->attribute_vector(), attribute_vector_comp_value, chunk_id,
                                         attribute_vector_scan_type);
        break;
      }
      default:
        throw std::runtime_error("Unknown underlying type for attribute vector");
    }
  }

  void _scan_reference_column(ChunkID chunk_id, std::shared_ptr<ReferenceColumn> column) {
    ReferenceGetter<T> reference_getter{*column->referenced_table(), column->referenced_column_id()};
    vector_scan(*column->pos_list(), reference_getter, _search_value, *_pos_list, chunk_id, _scan_type);
  }

  std::shared_ptr<PosList> _pos_list;
  const T _search_value;
  const ScanType _scan_type;
};

}  // namespace opossum
