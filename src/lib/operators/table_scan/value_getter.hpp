#pragma once

#include <memory>
#include <vector>

#include "storage/base_attribute_vector.hpp"
#include "storage/base_column.hpp"
#include "storage/dictionary_column.hpp"
#include "storage/reference_column.hpp"
#include "storage/value_column.hpp"
#include "types.hpp"

namespace opossum {

// A ValueGetter is used in a vector_scan to retrieve the actual value from the
// value/attribute/PosList vector that can then be used to be compared to the
// compare_value.

// An IdentityGetter is completely transparent, it returns the value itself
// and should be optimized out in the best case.
// It is used for...
// * ValueColumn, as it holds the values directly.
// * DictionaryColumn, as the comparison is performed on the ValueID directly.
template <typename T>
class IdentityGetter {
 public:
  const T& operator()(const T& element) const { return element; }
};

// A ReferenceGetter retrieves the referenced value based on a RowID
// from the supplied Table and ColumnID.
// Consequently, it is used when scanning a ReferenceColumn.
template <typename T>
class ReferenceGetter {
 public:
  explicit ReferenceGetter(const Table& table, ColumnID column_id)
      : _table{table},
        _column_id{column_id},
        _is_last_chunk_valid{false},
        _last_chunk_id{0},
        _last_value_ptr{nullptr},
        _last_dictionary_ptr{nullptr} {}
  const T& operator()(const RowID& row_id) const {
    // This implementation caches the last seen chunk by its ID.
    // This is only useful based on the assumption that PosLists are usually
    // sorted by ChunkID.
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

}  // namespace opossum
