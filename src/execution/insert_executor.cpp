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

#include <cstdint>
#include <memory>
#include <vector>

#include "catalog/schema.h"
#include "common/rid.h"
#include "execution/executors/insert_executor.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"

namespace bustub {

InsertExecutor::InsertExecutor(
    ExecutorContext *exec_ctx, const InsertPlanNode *plan,
    std::unique_ptr<AbstractExecutor> &&child_executor)  //  注意这里并不是万能引用（只有用模板才是），这是右值引用
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
}

void InsertExecutor::Init() {}
//  需要使用catalog来创建index等，

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (execute_finish_flag_) {
    return false;
  }
  std::vector<Value> values;

  int insert_row = NextHelper();  //  一次性插入子执行器的所有元祖（一条insert中可能插入多条元祖）
  Value row_affect_num(INTEGER, insert_row);
  values.emplace_back(row_affect_num);
  // std::cout<<"res_tuple";
  Tuple res_tuple(values, &GetOutputSchema());
  *tuple = res_tuple;
  execute_finish_flag_ = true;
  return true;
}

auto InsertExecutor::NextHelper() -> int {
  Tuple child_tuple;
  RID child_rid;
  int insert_row = 0;
  bool child_res = child_executor_->Next(&child_tuple, &child_rid);
  while (child_res) {
    insert_row++;
    // std::cout<<"child_tuple:"<<child_tuple.ToString(&child_executor_->GetOutputSchema())<<std::endl;
    table_info_->table_->InsertTuple(child_tuple, &child_rid, exec_ctx_->GetTransaction());

    auto table_indexes_info = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
    if (!table_indexes_info.empty()) {
      for (auto &index_info : table_indexes_info) {
        // std::cout<<"index_info"<<index_info->index_oid_<<std::endl;
        index_info->index_->InsertEntry(
            child_tuple.KeyFromTuple(child_executor_->GetOutputSchema(), index_info->key_schema_,
                                     index_info->index_->GetKeyAttrs()),
            child_rid, exec_ctx_->GetTransaction());
        // index_info->index_->InsertEntry(child_tuple, , exec_ctx_->GetTransaction());
      }
    }
    child_res = child_executor_->Next(&child_tuple, &child_rid);
    // std::cout<<child_res;
  }
  return insert_row;
}
}  // namespace bustub
