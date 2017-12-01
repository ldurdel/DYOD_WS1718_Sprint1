#pragma once

#include <functional>
#include <vector>

#include "types.hpp"

namespace opossum {

template <typename T, typename Getter, typename CompType>
void vector_scan_impl(const std::vector<T>& values, const Getter& getter, const T& compare_value, PosList& pos_list,
                      ChunkID chunk_id) {
  CompType comparator;
  for (ChunkOffset chunk_offset{0}; chunk_offset < values.size(); ++chunk_offset) {
    const auto& value = getter(values[chunk_offset]);
    if (comparator(value, compare_value)) {
      pos_list.emplace_back(RowID{chunk_id, chunk_offset});
    }
  }
}

template <typename T, typename Getter, typename CompType>
void vector_scan_impl(const std::vector<RowID>& values, const Getter& getter, const T& compare_value, PosList& pos_list,
                      ChunkID chunk_id) {
  CompType comparator;
  for (const RowID& row_id : values) {
    const auto& value = getter(row_id);
    if (comparator(value, compare_value)) {
      pos_list.emplace_back(row_id);
    }
  }
}

template <typename VectorT, typename CompareT, typename Getter>
void vector_scan(const std::vector<VectorT>& values, const Getter& getter, const CompareT& compare_value,
                 PosList& pos_list, ChunkID chunk_id, ScanType scan_type) {
  switch (scan_type) {
    case ScanType::OpEquals:
      vector_scan_impl<CompareT, Getter, std::equal_to<CompareT>>(values, getter, compare_value, pos_list, chunk_id);
      break;
    case ScanType::OpNotEquals:
      vector_scan_impl<CompareT, Getter, std::not_equal_to<CompareT>>(values, getter, compare_value, pos_list,
                                                                      chunk_id);
      break;
    case ScanType::OpLessThan:
      vector_scan_impl<CompareT, Getter, std::less<CompareT>>(values, getter, compare_value, pos_list, chunk_id);
      break;
    case ScanType::OpLessThanEquals:
      vector_scan_impl<CompareT, Getter, std::less_equal<CompareT>>(values, getter, compare_value, pos_list, chunk_id);
      break;
    case ScanType::OpGreaterThan:
      vector_scan_impl<CompareT, Getter, std::greater<CompareT>>(values, getter, compare_value, pos_list, chunk_id);
      break;
    case ScanType::OpGreaterThanEquals:
      vector_scan_impl<CompareT, Getter, std::greater_equal<CompareT>>(values, getter, compare_value, pos_list,
                                                                       chunk_id);
      break;
    default:
      throw std::runtime_error("Operator not known");
  }
}

}  // namespace opossum
