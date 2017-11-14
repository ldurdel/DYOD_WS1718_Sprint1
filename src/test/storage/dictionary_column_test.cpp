#include <memory>
#include <string>

#include "gtest/gtest.h"

#include "../../lib/resolve_type.hpp"
#include "../../lib/storage/base_column.hpp"
#include "../../lib/storage/dictionary_column.hpp"
#include "../../lib/storage/value_column.hpp"

// class StorageDictionaryColumnTest : public ::testing::Test {
//  protected:
//   std::shared_ptr<opossum::ValueColumn<int>> vc_int = std::make_shared<opossum::ValueColumn<int>>();
//   std::shared_ptr<opossum::ValueColumn<std::string>> vc_str = std::make_shared<opossum::ValueColumn<std::string>>();
// };

// TEST_F(StorageDictionaryColumnTest, CompressColumnString) {
//   vc_str->append("Bill");
//   vc_str->append("Steve");
//   vc_str->append("Alexander");
//   vc_str->append("Steve");
//   vc_str->append("Hasso");
//   vc_str->append("Bill");

//   auto col = opossum::make_shared_by_column_type<opossum::BaseColumn, opossum::DictionaryColumn>("string", vc_str);
//   auto dict_col = std::dynamic_pointer_cast<opossum::DictionaryColumn<std::string>>(col);

//   // Test attribute_vector size
//   EXPECT_EQ(dict_col->size(), 6u);

//   // Test dictionary size (uniqueness)
//   EXPECT_EQ(dict_col->unique_values_count(), 4u);

//   // Test sorting
//   auto dict = dict_col->dictionary();
//   EXPECT_EQ((*dict)[0], "Alexander");
//   EXPECT_EQ((*dict)[1], "Bill");
//   EXPECT_EQ((*dict)[2], "Hasso");
//   EXPECT_EQ((*dict)[3], "Steve");
// }

// TEST_F(StorageDictionaryColumnTest, LowerUpperBound) {
//   for (int i = 0; i <= 10; i += 2) vc_int->append(i);
//   auto col = opossum::make_shared_by_column_type<opossum::BaseColumn, opossum::DictionaryColumn>("int", vc_int);
//   auto dict_col = std::dynamic_pointer_cast<opossum::DictionaryColumn<int>>(col);

//   EXPECT_EQ(dict_col->lower_bound(4), (opossum::ValueID)2);
//   EXPECT_EQ(dict_col->upper_bound(4), (opossum::ValueID)3);

//   EXPECT_EQ(dict_col->lower_bound(5), (opossum::ValueID)3);
//   EXPECT_EQ(dict_col->upper_bound(5), (opossum::ValueID)3);

//   EXPECT_EQ(dict_col->lower_bound(15), opossum::INVALID_VALUE_ID);
//   EXPECT_EQ(dict_col->upper_bound(15), opossum::INVALID_VALUE_ID);
// }

// TODO(student): You should add some more tests here (full coverage would be appreciated) and possibly in other files.
