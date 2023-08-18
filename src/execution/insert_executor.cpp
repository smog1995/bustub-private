//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <vector>

#include "catalog/schema.h"
#include "common/rid.h"
#include "execution/executors/insert_executor.h"
#include "storage/table/tuple.h"

namespace bustub {

InsertExecutor::InsertExecutor(
    ExecutorContext *exec_ctx, const InsertPlanNode *plan,
    std::unique_ptr<AbstractExecutor> &&child_executor)  //  注意这里并不是万能引用（只有用模板才是），这是右值引用
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  Column column("affect row num", TINYINT);
  std::vector<Column> values(1);
  values[0] = column;
  integer_schema_ = std::make_unique<Schema>(Schema(values));
}

void InsertExecutor::Init() {}
//  需要使用catalog来创建index等，

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  Tuple child_tuple;
  RID child_rid;
  std::vector<Value> values(1);
  bool child_res = child_executor_->Next(&child_tuple, &child_rid);
  if (!child_res) {
    Value row_affect_num(TINYINT, 0);
    values[0] = row_affect_num;
    tuple = new Tuple(values, integer_schema_.get());
    return false;
  }

  table_info_->table_->InsertTuple(child_tuple, &child_rid, exec_ctx_->GetTransaction());
  auto table_indexes_info = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  if (!table_indexes_info.empty()) {
    for (auto &index_info : table_indexes_info) {
      index_info->index_->InsertEntry(child_tuple, child_rid, exec_ctx_->GetTransaction());
    }
  }
  Value row_affect_num(TINYINT, 1);
  tuple = new Tuple(values, integer_schema_.get());
  return true;
}

}  // namespace bustub
