#include "default_attribute_vector.hpp"

#include "utils/assert.hpp"

namespace opossum {

DefaultAttributeVector::DefaultAttributeVector(size_t number_of_entries) : _values{number_of_entries} {}

ValueID DefaultAttributeVector::get(const size_t i) { return _values.at(i); }

void DefaultAttributeVector::set(const size_t i, const ValueID value_id) {
  Assert(i < _values.size(), "Index out of bounds");
  _values[i] = value_id;
}

size_t DefaultAttributeVector::size() { return _values.size(); }

AttributeVectorWidth DefaultAttributeVector::width() { return sizeof(ValueID); }

}  // namespace opossum
