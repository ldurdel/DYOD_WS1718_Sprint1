#pragma once

// the linter wants this to be above everything else
#include <shared_mutex>

#include <atomic>
#include <memory>
#include <string>
#include <vector>

#include "all_type_variant.hpp"
#include "types.hpp"

namespace opossum {

class BaseIndex;
class BaseColumn;

// A chunk is a horizontal partition of a table.
// It stores the data column by column.
//
// Find more information about this in our wiki: https://github.com/hyrise/zweirise/wiki/chunk-concept
class Chunk : private Noncopyable {
 public:
  Chunk() = default;

  // we need to explicitly set the move constructor to default when
  // we overwrite the copy constructor
  Chunk(Chunk&&) = default;
  Chunk& operator=(Chunk&&) = default;

  // adds a column to the "right" of the chunk
  void add_column(std::shared_ptr<BaseColumn> column);

  // returns the number of columns (cannot exceed ColumnID (uint16_t))
  uint16_t col_count() const;

  // returns the number of rows (cannot exceed ChunkOffset (uint32_t))
  uint32_t size() const;

  // adds a new row, given as a list of values, to the chunk
  // note this is slow and not thread-safe and should be used for testing purposes only
  void append(std::vector<AllTypeVariant> values);

  // Returns the column at a given position
  std::shared_ptr<BaseColumn> get_column(ColumnID column_id) const;

 protected:
  std::vector<std::shared_ptr<BaseColumn>> _columns;
};

}  // namespace opossum
