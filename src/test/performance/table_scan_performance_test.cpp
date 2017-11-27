#include <memory>
#include <random>
#include <string>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "operators/table_wrapper.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

class PerformanceTableScanTest : public BaseTest {
 protected:
  void SetUp() override {
    _random_table_small = std::make_shared<Table>(_entries_small / 10);
    _sorted_table_small = std::make_shared<Table>(_entries_small / 10);
    _random_table_medium = std::make_shared<Table>(_entries_medium / 10);
    _sorted_table_medium = std::make_shared<Table>(_entries_medium / 10);
    _random_table_large = std::make_shared<Table>(_entries_large / 10);
    _sorted_table_large = std::make_shared<Table>(_entries_large / 10);

    StorageManager::get().add_table("randomSmall", _random_table_small);
    StorageManager::get().add_table("sortedSmall", _sorted_table_small);
    StorageManager::get().add_table("randomMedium", _random_table_medium);
    StorageManager::get().add_table("sortedMedium", _sorted_table_medium);
    StorageManager::get().add_table("randomLarge", _random_table_large);
    StorageManager::get().add_table("sortedLarge", _sorted_table_large);

    _fill_with_sorted_values(_sorted_table_small, _entries_small);
    _fill_with_sorted_values(_sorted_table_medium, _entries_medium);
    _fill_with_sorted_values(_sorted_table_large, _entries_large);

    _fill_with_random_values(_random_table_small, _entries_small);
    _fill_with_random_values(_random_table_medium, _entries_medium);
    _fill_with_random_values(_random_table_large, _entries_large);
  }

  void TearDown() override {
    std::vector<std::string> table_names = {"randomSmall",  "sortedSmall", "randomMedium",
                                            "sortedMedium", "randomLarge", "sortedLarge"};

    for (auto table_name : table_names) {
      if (StorageManager::get().has_table(table_name)) {
        StorageManager::get().drop_table(table_name);
      }
    }
  }

  // Simulates duplicate and sorted values. Fills the table with one column of unsigned integers,
  // duplicating 10 values before proceeding to the next one.
  void _fill_with_sorted_values(std::shared_ptr<Table> table, int numberOfValues) {
    table->add_column("testColumn", "int");

    int currentValue = 0;

    for (auto index = 0; index < numberOfValues; ++index) {
      if (index % 10 == 0) {
        ++currentValue;
      }
      table->append({currentValue});
    }
  }

  // Simulates unsorted and random values. Fills the table with one column of unsigned, random integer values
  // From the range of 0 to 1000000.
  void _fill_with_random_values(std::shared_ptr<Table> table, int numberOfValues) {
    table->add_column("testColumn", "int");

    // https://stackoverflow.com/a/19728404
    std::random_device random_device;
    std::mt19937 random_number_generator(random_device());
    std::uniform_int_distribution<int> distribution(0, 1000000);

    for (auto index = 0; index < numberOfValues; ++index) {
      table->append({distribution(random_number_generator)});
    }
  }

  // Defines the number of entries for each type of table.
  // There will be number/10 entries per chunk
  const int _entries_small = 100;
  const int _entries_medium = 10000;
  const int _entries_large = 1000000;

  std::shared_ptr<Table> _random_table_small;
  std::shared_ptr<Table> _sorted_table_small;
  std::shared_ptr<Table> _random_table_medium;
  std::shared_ptr<Table> _sorted_table_medium;
  std::shared_ptr<Table> _random_table_large;
  std::shared_ptr<Table> _sorted_table_large;
};

TEST_F(PerformanceTableScanTest, EmptyTest) {}

}  // namespace opossum
