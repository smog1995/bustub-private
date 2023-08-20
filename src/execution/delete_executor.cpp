//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <memory>

#include "execution/executors/delete_executor.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan),child_executor_(std::move(child_executor)){
        table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
        Column column("affect row num", TINYINT);
        std::vector<Column> values;
        values.emplace_back(column);
        integer_schema_ = std::make_unique<Schema>(Schema(values));
    }

void DeleteExecutor::Init() {}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if(execute_finish_flag_) {
    return false;
  }
  std::vector<Value> values;
  Tuple *res_tuple;
  int delete_row = NextHelper();
  Value row_affect_num(INTEGER, delete_row);
  values.emplace_back(row_affect_num);
  res_tuple = new Tuple(values, &GetOutputSchema());
  *tuple = *res_tuple;
  delete res_tuple;
  execute_finish_flag_ = true;
  return true;
}


auto DeleteExecutor::NextHelper() -> int {
  Tuple child_tuple;
  RID child_rid;
  int delete_row = 0;
  bool child_res = child_executor_->Next(&child_tuple, &child_rid);
  while (child_res) {
    delete_row++;
    table_info_->table_->MarkDelete(child_rid, exec_ctx_->GetTransaction());
    std::cout<<"child_tuple:"<<child_tuple.ToString(&child_executor_->GetOutputSchema())<<std::endl;
    std::cout<<"child_rid:"<<child_rid.ToString()<<std::endl;
    auto table_indexes_info = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
    if (!table_indexes_info.empty()) {
      for (auto &index_info : table_indexes_info) {
        index_info->index_->DeleteEntry(child_tuple.KeyFromTuple(child_executor_->GetOutputSchema(), index_info->key_schema_, index_info->index_->GetKeyAttrs()), child_rid, exec_ctx_->GetTransaction());
      }
    }
    child_res = child_executor_->Next(&child_tuple, &child_rid);
  }
  return delete_row;
}
}  // namespace bustub
