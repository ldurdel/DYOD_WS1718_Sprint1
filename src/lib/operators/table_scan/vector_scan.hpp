#pragma once

#include <functional>
#include <vector>

#include "types.hpp"

namespace opossum {

// Generic vector scan implementation that stores offsets of matching values.
template <typename T, typename ValueGetterT, typename CompType>
void vector_scan_impl(const std::vector<T>& values, const ValueGetterT& getter, const T& compare_value,
                      PosList& pos_list, ChunkID chunk_id) {
  CompType comparator;
  for (ChunkOffset chunk_offset{0}; chunk_offset < values.size(); ++chunk_offset) {
    const auto& value = getter(values[chunk_offset]);
    if (comparator(value, compare_value)) {
      pos_list.emplace_back(RowID{chunk_id, chunk_offset});
    }
  }
}

// PosList-specific vector scan implementation that stores matching RowIDs directly
// instead of their offsets.
template <typename T, typename ValueGetterT, typename CompType>
void vector_scan_impl(const std::vector<RowID>& values, const ValueGetterT& getter, const T& compare_value,
                      PosList& pos_list, ChunkID chunk_id) {
  CompType comparator;
  for (const RowID& row_id : values) {
    const auto& value = getter(row_id);
    if (comparator(value, compare_value)) {
      pos_list.emplace_back(row_id);
    }
  }
}

// Generic method to create a PosList referencing all elements from a vector.
template <typename T>
void vector_dump(const std::vector<T>& values, PosList& pos_list, ChunkID chunk_id) {
  for (ChunkOffset chunk_offset{0}; chunk_offset < values.size(); ++chunk_offset) {
    pos_list.emplace_back(RowID{chunk_id, chunk_offset});
  }
}

// PosList-specific method to create a PosList referencing all elements from a vector.
// For some reason, this method is listed as not covered by test cases, but e.g.
// the ScanOnReferencedDictColumn test calls it (verified by breakpoint)
template <>
void vector_dump(const std::vector<RowID>& values, PosList& pos_list, ChunkID chunk_id) {
  for (const RowID& row_id : values) {
    pos_list.emplace_back(row_id);
  }
}

// Performs a scan on a std::vector based on a compare_value and a scan_type.
// The resulting (matching) offsets are stored with the given chunk_id in the pos_list.
// Values are retrieved from the vector using the provided ValueGetter.
template <typename VectorT, typename CompareT, typename ValueGetterT>
void vector_scan(const std::vector<VectorT>& values, const ValueGetterT& getter, const CompareT& compare_value,
                 PosList& pos_list, ChunkID chunk_id, ScanType scan_type) {
  switch (scan_type) {
    case ScanType::OpEquals:
      vector_scan_impl<CompareT, ValueGetterT, std::equal_to<CompareT>>(values, getter, compare_value, pos_list,
                                                                        chunk_id);
      break;
    case ScanType::OpNotEquals:
      vector_scan_impl<CompareT, ValueGetterT, std::not_equal_to<CompareT>>(values, getter, compare_value, pos_list,
                                                                            chunk_id);
      break;
    case ScanType::OpLessThan:
      vector_scan_impl<CompareT, ValueGetterT, std::less<CompareT>>(values, getter, compare_value, pos_list, chunk_id);
      break;
    case ScanType::OpLessThanEquals:
      vector_scan_impl<CompareT, ValueGetterT, std::less_equal<CompareT>>(values, getter, compare_value, pos_list,
                                                                          chunk_id);
      break;
    case ScanType::OpGreaterThan:
      vector_scan_impl<CompareT, ValueGetterT, std::greater<CompareT>>(values, getter, compare_value, pos_list,
                                                                       chunk_id);
      break;
    case ScanType::OpGreaterThanEquals:
      vector_scan_impl<CompareT, ValueGetterT, std::greater_equal<CompareT>>(values, getter, compare_value, pos_list,
                                                                             chunk_id);
      break;
    case ScanType::OpAll:
      vector_dump<VectorT>(values, pos_list, chunk_id);
      break;
    case ScanType::OpNone:
      // early exit
      return;
    default:
      throw std::runtime_error("Operator not known");
  }
}

}  // namespace opossum
