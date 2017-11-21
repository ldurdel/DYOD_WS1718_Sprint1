#include "table_scan.hpp"

#include "resolve_type.hpp"
#include "storage/table.hpp"

namespace opossum {

class BaseTableScanImpl {
public:
    BaseTableScanImpl(const TableScan& table_scan)
        : _table_scan{table_scan}
    { }

    virtual std::shared_ptr<const Table> on_execute() = 0;

protected:
    const TableScan& _table_scan;
};

template <typename T>
class TypedTableScanImpl : public BaseTableScanImpl {
public:
    TypedTableScanImpl(const TableScan& table_scan)
        : BaseTableScanImpl{table_scan}
    { }

    virtual std::shared_ptr<const Table> on_execute() override {
        // TODO demux column types (value, dictionary, reference)
        return nullptr;
    }
protected:
};

TableScan::TableScan(const std::shared_ptr<const AbstractOperator> in, ColumnID column_id, const ScanType scan_type, const AllTypeVariant search_value)
    : AbstractOperator(in)
    , _column_id{column_id}
    , _scan_type{scan_type}
    , _search_value{search_value}
{
}

ColumnID TableScan::column_id() const
{
return _column_id;
}

ScanType TableScan::scan_type() const
{
return _scan_type;
}

const AllTypeVariant &TableScan::search_value() const
{
return _search_value;
}

std::shared_ptr<const Table> TableScan::_on_execute()
{
    Assert(_input_left->get_output(), "No output");
    const auto& column_type = _input_left->get_output()->column_type(_column_id);
    auto table_scan_impl = make_shared_by_column_type<BaseTableScanImpl, TypedTableScanImpl>(column_type, *this);

    return table_scan_impl->on_execute();
}



}  // namespace opossum
