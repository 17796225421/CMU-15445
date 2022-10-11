//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      schema_(&exec_ctx->GetCatalog()->GetTable(plan->GetTableOid())->schema_),
      table_heap_(exec_ctx->GetCatalog()->GetTable(plan_->GetTableOid())->table_.get()),
      iter_(table_heap_->Begin(exec_ctx_->GetTransaction())) {}

void SeqScanExecutor::Init() { iter_ = table_heap_->Begin(exec_ctx_->GetTransaction()); }

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  // 1. 循环，直到输入表迭代器到输入表末尾，从计划节点获得输入表编号，从catalog获得输入表编号对应的输入表信息
  // 2. 准备输出行，遍历输出行每个列，从计划节点获得输出行的列类型数组，获得输出行的列数量
  // 3.
  // 使用当前输入行、输入行的列类型数组、输出行的当前列的列类型，来获取输出行当前列的值。输入行的列类型数组从输入表信息获取
  // 4. 使用计划节点的判断条件判断当前输出行是否满足判断条件，满足返回，不满足重新开始

  if (iter_ == table_heap_->End()) {
    return false;
  }

  *tuple = *iter_;
  *rid = tuple->GetRid();

  LockManager *lock_mgr = GetExecutorContext()->GetLockManager();
  Transaction *txn = GetExecutorContext()->GetTransaction();
  if (txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
    if (!lock_mgr->LockShared(txn, *rid)) {
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    }
  }

  std::vector<Value> values;
  for (size_t i = 0; i < plan_->OutputSchema()->GetColumnCount(); i++) {
    values.push_back(plan_->OutputSchema()->GetColumn(i).GetExpr()->Evaluate(tuple, schema_));
  }

  *tuple = Tuple(values, plan_->OutputSchema());

  if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    if (!lock_mgr->Unlock(txn, *rid)) {
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    }
  }

  ++iter_;

  const AbstractExpression *predict = plan_->GetPredicate();
  if (predict != nullptr && !predict->Evaluate(tuple, plan_->OutputSchema()).GetAs<bool>()) {
    return Next(tuple, rid);
  }

  return true;
}
}  // namespace bustub
