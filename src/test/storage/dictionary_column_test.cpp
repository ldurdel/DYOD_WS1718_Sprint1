#include <limits>
#include <memory>
#include <string>

#include "gtest/gtest.h"

#include "../../lib/resolve_type.hpp"
#include "../../lib/storage/base_column.hpp"
#include "../../lib/storage/dictionary_column.hpp"
#include "../../lib/storage/value_column.hpp"
#include "../../lib/type_cast.hpp"

class StorageDictionaryColumnTest : public ::testing::Test {
 protected:
  std::shared_ptr<opossum::ValueColumn<int>> vc_int = std::make_shared<opossum::ValueColumn<int>>();
  std::shared_ptr<opossum::ValueColumn<std::string>> vc_str = std::make_shared<opossum::ValueColumn<std::string>>();
};

TEST_F(StorageDictionaryColumnTest, CompressColumnString) {
  vc_str->append("Bill");
  vc_str->append("Steve");
  vc_str->append("Alexander");
  vc_str->append("Steve");
  vc_str->append("Hasso");
  vc_str->append("Bill");

  auto col = opossum::make_shared_by_column_type<opossum::BaseColumn, opossum::DictionaryColumn>("string", vc_str);
  auto dict_col = std::dynamic_pointer_cast<opossum::DictionaryColumn<std::string>>(col);

  // Test attribute_vector size
  EXPECT_EQ(dict_col->size(), 6u);
  // Test dictionary size (uniqueness)
  EXPECT_EQ(dict_col->unique_values_count(), 4u);

  // Test sorting
  auto dict = dict_col->dictionary();
  EXPECT_EQ((*dict)[0], "Alexander");
  EXPECT_EQ((*dict)[1], "Bill");
  EXPECT_EQ((*dict)[2], "Hasso");
  EXPECT_EQ((*dict)[3], "Steve");
}

TEST_F(StorageDictionaryColumnTest, LowerUpperBound) {
  for (int i = 0; i <= 10; i += 2) vc_int->append(i);

  auto col = opossum::make_shared_by_column_type<opossum::BaseColumn, opossum::DictionaryColumn>("int", vc_int);
  auto dict_col = std::dynamic_pointer_cast<opossum::DictionaryColumn<int>>(col);

  EXPECT_EQ(dict_col->lower_bound(4), (opossum::ValueID)2);
  EXPECT_EQ(dict_col->upper_bound(4), (opossum::ValueID)3);
  EXPECT_EQ(dict_col->lower_bound(5), (opossum::ValueID)3);
  EXPECT_EQ(dict_col->upper_bound(5), (opossum::ValueID)3);

  EXPECT_EQ(dict_col->lower_bound(15), opossum::INVALID_VALUE_ID);
  EXPECT_EQ(dict_col->upper_bound(15), opossum::INVALID_VALUE_ID);
}

TEST_F(StorageDictionaryColumnTest, AccessBySubscript) {
  vc_str->append("Bill");
  vc_str->append("Steve");
  vc_str->append("Alexander");
  vc_str->append("Steve");
  vc_str->append("Hasso");
  vc_str->append("Bill");

  auto col = opossum::make_shared_by_column_type<opossum::BaseColumn, opossum::DictionaryColumn>("string", vc_str);
  auto dict_col = std::dynamic_pointer_cast<opossum::DictionaryColumn<std::string>>(col);

  // Check access with [] operator returning an AllTypeVariant
  EXPECT_EQ(opossum::type_cast<std::string>((*dict_col)[0]), "Bill");
  EXPECT_EQ(opossum::type_cast<std::string>((*dict_col)[1]), "Steve");
  EXPECT_EQ(opossum::type_cast<std::string>((*dict_col)[2]), "Alexander");
  EXPECT_EQ(opossum::type_cast<std::string>((*dict_col)[3]), "Steve");
  EXPECT_EQ(opossum::type_cast<std::string>((*dict_col)[4]), "Hasso");
  EXPECT_EQ(opossum::type_cast<std::string>((*dict_col)[5]), "Bill");

  EXPECT_ANY_THROW((*dict_col)[999]);
}

TEST_F(StorageDictionaryColumnTest, AccessByGet) {
  vc_str->append("Bill");
  vc_str->append("Steve");
  vc_str->append("Alexander");
  vc_str->append("Steve");
  vc_str->append("Hasso");
  vc_str->append("Bill");

  auto col = opossum::make_shared_by_column_type<opossum::BaseColumn, opossum::DictionaryColumn>("string", vc_str);
  auto dict_col = std::dynamic_pointer_cast<opossum::DictionaryColumn<std::string>>(col);

  // Check access with get method
  EXPECT_EQ(dict_col->get(0), "Bill");
  EXPECT_EQ(dict_col->get(1), "Steve");
  EXPECT_EQ(dict_col->get(2), "Alexander");
  EXPECT_EQ(dict_col->get(3), "Steve");
  EXPECT_EQ(dict_col->get(4), "Hasso");
  EXPECT_EQ(dict_col->get(5), "Bill");

  EXPECT_ANY_THROW(dict_col->get(999));
}

TEST_F(StorageDictionaryColumnTest, AccessByAttributeVector) {
  vc_str->append("Bill");
  vc_str->append("Steve");
  vc_str->append("Alexander");
  vc_str->append("Steve");
  vc_str->append("Hasso");
  vc_str->append("Bill");

  auto col = opossum::make_shared_by_column_type<opossum::BaseColumn, opossum::DictionaryColumn>("string", vc_str);
  auto dict_col = std::dynamic_pointer_cast<opossum::DictionaryColumn<std::string>>(col);

  // Check access via attribute vector and dictionary
  auto attribute_vector = dict_col->attribute_vector();
  EXPECT_EQ(dict_col->value_by_value_id(attribute_vector->get(0)), "Bill");
  EXPECT_EQ(dict_col->value_by_value_id(attribute_vector->get(1)), "Steve");
  EXPECT_EQ(dict_col->value_by_value_id(attribute_vector->get(2)), "Alexander");
  EXPECT_EQ(dict_col->value_by_value_id(attribute_vector->get(3)), "Steve");
  EXPECT_EQ(dict_col->value_by_value_id(attribute_vector->get(4)), "Hasso");
  EXPECT_EQ(dict_col->value_by_value_id(attribute_vector->get(5)), "Bill");

  EXPECT_ANY_THROW(attribute_vector->get(999));
  EXPECT_ANY_THROW(dict_col->value_by_value_id(opossum::ValueID{999}));
  EXPECT_ANY_THROW(dict_col->value_by_value_id(opossum::INVALID_VALUE_ID));
}

TEST_F(StorageDictionaryColumnTest, ImmutableDictionaryColumn) {
  auto col = opossum::make_shared_by_column_type<opossum::BaseColumn, opossum::DictionaryColumn>("string", vc_str);
  auto dict_col = std::dynamic_pointer_cast<opossum::DictionaryColumn<std::string>>(col);

  EXPECT_ANY_THROW(dict_col->append("ohai"));
}

TEST_F(StorageDictionaryColumnTest, DictionaryWidthDistinctValues) {
  // Expect the correct dictionary width if the value column is filled with distinct values
  uint32_t unique_values_count = 0;

  auto col = opossum::make_shared_by_column_type<opossum::BaseColumn, opossum::DictionaryColumn>("int", vc_int);
  auto dict_col = std::dynamic_pointer_cast<opossum::DictionaryColumn<int>>(col);
  EXPECT_EQ(dict_col->attribute_vector()->width(), 1);

  // Last value ID (in this case 255) is reserved for INVALID_VALUE_ID
  for (; unique_values_count < std::numeric_limits<uint8_t>::max(); ++unique_values_count) {
    vc_int->append(static_cast<int>(unique_values_count));
  }

  col = opossum::make_shared_by_column_type<opossum::BaseColumn, opossum::DictionaryColumn>("int", vc_int);
  dict_col = std::dynamic_pointer_cast<opossum::DictionaryColumn<int>>(col);
  EXPECT_EQ(dict_col->attribute_vector()->width(), 1);

  // Now add the one value that forces a new width datatype (unit16)
  vc_int->append(static_cast<int>(unique_values_count++));

  col = opossum::make_shared_by_column_type<opossum::BaseColumn, opossum::DictionaryColumn>("int", vc_int);
  dict_col = std::dynamic_pointer_cast<opossum::DictionaryColumn<int>>(col);
  EXPECT_EQ(dict_col->attribute_vector()->width(), 2);

  // Now test the last uint16 value
  // Last value ID (in this case 65535) is reserved for INVALID_VALUE_ID
  for (; unique_values_count < std::numeric_limits<uint16_t>::max(); ++unique_values_count) {
    vc_int->append(static_cast<int>(unique_values_count));
  }

  col = opossum::make_shared_by_column_type<opossum::BaseColumn, opossum::DictionaryColumn>("int", vc_int);
  dict_col = std::dynamic_pointer_cast<opossum::DictionaryColumn<int>>(col);
  EXPECT_EQ(dict_col->attribute_vector()->width(), 2);

  // lastly uint32
  vc_int->append(static_cast<int>(unique_values_count++));

  col = opossum::make_shared_by_column_type<opossum::BaseColumn, opossum::DictionaryColumn>("int", vc_int);
  dict_col = std::dynamic_pointer_cast<opossum::DictionaryColumn<int>>(col);
  EXPECT_EQ(dict_col->attribute_vector()->width(), 4);
}

TEST_F(StorageDictionaryColumnTest, DictionaryWithDuplicateValues) {
  // Expect the correct dictionary width if the value column is filled with partly duplicate values
  // Last value ID (in this case 255) is reserved for INVALID_VALUE_ID
  uint32_t values_count = 0;
  uint32_t unique_values_modulo = 255;

  // First, we add 255 unique values, but add 64k values in total. They should still fit into an 8 bit dictionary
  for (; values_count < std::numeric_limits<uint16_t>::max(); ++values_count) {
    vc_int->append(static_cast<int>(values_count % unique_values_modulo));
  }

  auto col = opossum::make_shared_by_column_type<opossum::BaseColumn, opossum::DictionaryColumn>("int", vc_int);
  auto dict_col = std::dynamic_pointer_cast<opossum::DictionaryColumn<int>>(col);
  EXPECT_EQ(dict_col->attribute_vector()->width(), 1);
}

// TODO(student): You should add some more tests here (full coverage would be appreciated) and possibly in other files.
