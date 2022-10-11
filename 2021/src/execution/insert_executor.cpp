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

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      catalog_(exec_ctx->GetCatalog()),
      table_info_(catalog_->GetTable(plan->TableOid())),
      table_heap_(table_info_->table_.get()) {}

void InsertExecutor::Init() {
  if (!plan_->IsRawInsert()) {
    child_executor_->Init();
  } else {
    iter_ = plan_->RawValues().begin();
  }
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  // 1. 判断values插入还是select插入，计划节点的输入行数组大小大于0是values插入，否则select插入
  //   a.
  //   如果是values插入，遍历输入行数组的每一输入行，利用输入行的列类型数组来获取当前输入行。输入行的列类型数组来自输入表信息，输入表信息来自catalog利用输入表编号获取，输入表编号来自计划节点
  //   b. 将输入行插入输入表指定行编号，行编号来自参数。
  //   c. 将输入行插入索引，索引来自catalog
  //   d. 继续遍历输入行数组的下一输入行
  // 2. 如果是select插入，循环调用儿子执行器的next获取每一输入行，直到next为false
  // 3. 将输入行插入输入表指定行编号，行编号来自参数，输入表来自catalog利用输入表编号获取，输入表编号来自计划节点
  // 4. 将输入行插入索引，索引来自catalog

  std::vector<Tuple> tuples;

  if (!plan_->IsRawInsert()) {
    if (!child_executor_->Next(tuple, rid)) {
      return false;
    }
  } else {
    if (iter_ == plan_->RawValues().end()) {
      return false;
    }
    *tuple = Tuple(*iter_, &table_info_->schema_);
    iter_++;
  }

  if (!table_heap_->InsertTuple(*tuple, rid, exec_ctx_->GetTransaction())) {
    LOG_DEBUG("INSERT FAIL");
    return false;
  }

  Transaction *txn = GetExecutorContext()->GetTransaction();
  LockManager *lock_mgr = GetExecutorContext()->GetLockManager();

  if (txn->IsSharedLocked(*rid)) {
    if (!lock_mgr->LockUpgrade(txn, *rid)) {
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    }
  } else {
    if (!lock_mgr->LockExclusive(txn, *rid)) {
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    }
  }

  for (const auto &index : catalog_->GetTableIndexes(table_info_->name_)) {
    index->index_->InsertEntry(
        tuple->KeyFromTuple(table_info_->schema_, *index->index_->GetKeySchema(), index->index_->GetKeyAttrs()), *rid,
        exec_ctx_->GetTransaction());
  }

  if (txn->GetIsolationLevel() != IsolationLevel::REPEATABLE_READ) {
    if (!lock_mgr->Unlock(txn, *rid)) {
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    }
  }

  return Next(tuple, rid);
}

}  // namespace bustub
