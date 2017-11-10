#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../lib/resolve_type.hpp"
#include "../lib/storage/dictionary_column.hpp"
#include "../lib/storage/table.hpp"

namespace opossum {

class StorageTableTest : public BaseTest {
 protected:
  void SetUp() override {
    t.add_column("col_1", "int");
    t.add_column("col_2", "string");
  }

  Table t{2};
};

TEST_F(StorageTableTest, ChunkCount) {
  EXPECT_EQ(t.chunk_count(), 1u);
  t.append({4, "Hello,"});
  t.append({6, "world"});
  t.append({3, "!"});
  EXPECT_EQ(t.chunk_count(), 2u);
}

TEST_F(StorageTableTest, GetChunk) {
  t.get_chunk(ChunkID{0});
  t.append({4, "Hello,"});
  t.append({6, "world"});
  t.append({3, "!"});
  t.get_chunk(ChunkID{1});
  EXPECT_ANY_THROW(t.get_chunk(ChunkID{2}));

  // For the coverage
  const auto& const_chunk = t.get_chunk(ChunkID{0});
  EXPECT_EQ(const_chunk.size(), 2u);
}

TEST_F(StorageTableTest, ColCount) { EXPECT_EQ(t.col_count(), 2u); }

TEST_F(StorageTableTest, RowCount) {
  EXPECT_EQ(t.row_count(), 0u);
  t.append({4, "Hello,"});
  t.append({6, "world"});
  t.append({3, "!"});
  EXPECT_EQ(t.row_count(), 3u);
}

TEST_F(StorageTableTest, GetColumnName) {
  EXPECT_EQ(t.column_name(ColumnID{0}), "col_1");
  EXPECT_EQ(t.column_name(ColumnID{1}), "col_2");
  // TODO(anyone): Do we want checks here?
  // EXPECT_THROW(t.column_name(ColumnID{2}), std::exception);
}

TEST_F(StorageTableTest, GetColumnType) {
  EXPECT_EQ(t.column_type(ColumnID{0}), "int");
  EXPECT_EQ(t.column_type(ColumnID{1}), "string");
  // TODO(anyone): Do we want checks here?
  // EXPECT_THROW(t.column_type(ColumnID{2}), std::exception);
}

TEST_F(StorageTableTest, GetColumnIdByName) {
  EXPECT_EQ(t.column_id_by_name("col_2"), 1u);
  EXPECT_THROW(t.column_id_by_name("no_column_name"), std::exception);
}

TEST_F(StorageTableTest, GetChunkSize) { EXPECT_EQ(t.chunk_size(), 2u); }

TEST_F(StorageTableTest, AddColumnsToNonEmptyTable) {
  EXPECT_NO_THROW(t.add_column("foo1", "int"));
  t.append({4, "Hello,", 27});
  EXPECT_THROW(t.add_column("foo2", "int"), std::exception);
}

TEST_F(StorageTableTest, LazyColumnDefinitionInitialization) {
  t.add_column_definition("col_3", "string");
  EXPECT_NO_THROW(t.append({1, "stringCol1", "stringCol2"}));
}

TEST_F(StorageTableTest, ColumnNames) {
  auto column_names = t.column_names();
  EXPECT_EQ(column_names.size(), 2u);
  EXPECT_EQ(column_names[0], "col_1");
  EXPECT_EQ(column_names[1], "col_2");
}

TEST_F(StorageTableTest, CompressChunk) {
  t.append({4, "Hello,"});
  t.append({6, "world"});
  t.append({3, "!"});

  EXPECT_EQ(t.row_count(), 3u);
  EXPECT_EQ(t.chunk_count(), 2u);

  t.compress_chunk(ChunkID{0});
  EXPECT_EQ(t.row_count(), 3u);
  EXPECT_EQ(t.chunk_count(), 2u);

  auto column_ptr = t.get_chunk(ChunkID{0}).get_column(ColumnID{0});
  auto dictionary_column_ptr = std::dynamic_pointer_cast<opossum::DictionaryColumn<int>>(column_ptr);
  EXPECT_TRUE(dictionary_column_ptr);
  EXPECT_EQ(dictionary_column_ptr->unique_values_count(), 2u);
}

}  // namespace opossum
