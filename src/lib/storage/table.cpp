#include "table.hpp"

#include <algorithm>
#include <iomanip>
#include <limits>
#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "value_column.hpp"

#include "resolve_type.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

Table::Table(const uint32_t chunk_size)
    : _chunk_size{chunk_size}, _chunks{}, _column_names{}, _column_types{}, _chunk_matches_definitions{true} {
  create_new_chunk();
}

void Table::add_column_definition(const std::string& name, const std::string& type) {
  auto nonEmptyErrorMessage = "Column definition modification may only take place on an empty table";
  Assert(_chunks.size() == 1, nonEmptyErrorMessage);
  Assert(_chunks.front().size() == 0, nonEmptyErrorMessage);
  DebugAssert(_column_names.size() == _column_types.size(), "Every column needs a name and type");

  // This does not work because numeric_limits does not work on STRONG_TYPEDEFs
  //Assert(_column_names.size() < static_cast<size_t>(std::numeric_limits<ChunkID>::max()), "Too many columns");

  _column_names.emplace_back(name);
  _column_types.emplace_back(type);
  _chunk_matches_definitions = false;
}

void Table::add_column(const std::string& name, const std::string& type) {
  add_column_definition(name, type);
  _create_missing_columns();
}

void Table::append(std::vector<AllTypeVariant> values) {
  if (!_chunk_matches_definitions) {
    _create_missing_columns();
  }
  if (_chunk_size != 0 && _chunks.back().size() >= _chunk_size) {
    create_new_chunk();
  }
  _chunks.back().append(values);
}

void Table::create_new_chunk() {
  Assert(_chunks.size() == 0 || _chunks.back().size() > 0, "Cannot create chunk on top of empty chunk");
  DebugAssert(_chunk_matches_definitions, "Creating a new chunk implies that column modifications are synchronized");

  _chunks.emplace_back();

  // Automatically populates the empty new chunk with the specified column definitions
  _create_missing_columns();
}

void Table::_create_missing_columns() {
  DebugAssert(_column_names.size() == _column_types.size(), "Every column needs a name and type");

  Chunk& lastChunk = _chunks.back();

  // Assuming that columns may only be created, we can assume that already existing columns match
  // our stored definition
  auto firstMissingColumnIndex = lastChunk.col_count();

  for (auto index = firstMissingColumnIndex; index < _column_types.size(); ++index) {
    auto column = make_shared_by_column_type<BaseColumn, ValueColumn>(_column_types[index]);
    lastChunk.add_column(column);
  }

  _chunk_matches_definitions = true;
}

uint16_t Table::col_count() const {
  DebugAssert(_column_names.size() == _column_types.size(), "Every column needs a name and type");
  return _column_names.size();
}

uint64_t Table::row_count() const {
  return std::accumulate(_chunks.cbegin(), _chunks.cend(), 0,
                         [](auto acc, const Chunk& chunk) { return acc + chunk.size(); });
}

ChunkID Table::chunk_count() const { return static_cast<ChunkID>(_chunks.size()); }

ColumnID Table::column_id_by_name(const std::string& column_name) const {
  DebugAssert(_column_names.size() == _column_types.size(), "Every column needs a name and type");
  for (auto column_index = 0u; column_index < _column_names.size(); ++column_index) {
    if (_column_names[column_index] == column_name) {
      return static_cast<ColumnID>(column_index);
    }
  }
  throw std::out_of_range("Column '" + column_name + "' not in table");
}

uint32_t Table::chunk_size() const { return _chunk_size; }

const std::vector<std::string>& Table::column_names() const { return _column_names; }

const std::string& Table::column_name(ColumnID column_id) const {
  Assert(column_id < _column_names.size(), "column id out of range");
  return _column_names[column_id];
}

const std::string& Table::column_type(ColumnID column_id) const {
  Assert(column_id < _column_types.size(), "column id out of range");
  return _column_types[column_id];
}

Chunk& Table::get_chunk(ChunkID chunk_id) {
  Assert(chunk_id < _chunks.size(), "chunk id out of range");
  return _chunks[chunk_id];
}

const Chunk& Table::get_chunk(ChunkID chunk_id) const {
  Assert(chunk_id < _chunks.size(), "chunk id out of range");
  return _chunks[chunk_id];
}

}  // namespace opossum
