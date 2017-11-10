#pragma once

#include <vector>

#include "base_attribute_vector.hpp"
#include "types.hpp"

namespace opossum {

// Stores entries with the same bit length as the data type behind ValueID.
class DefaultAttributeVector : public BaseAttributeVector {
 public:
  explicit DefaultAttributeVector(size_t number_of_entries);

  ValueID get(const size_t i) const override;

  void set(const size_t i, const ValueID value_id) override;

  size_t size() const override;

  AttributeVectorWidth width() const override;

 protected:
  std::vector<ValueID> _values;
};
}  // namespace opossum
