//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void LimitExecutor::Init() {
  numbers_ = 0;
  child_executor_->Init();
}

bool LimitExecutor::Next(Tuple *tuple, RID *rid) {
  // 1. 儿子执行器next获得输入行，将输入行作为输出行
  // 2.
  // 如果当前已经输出的行数超过最大行数，返回false，最大行数来自计划节点。当前行数是limit执行器的成员，每次next都会当前行数++。

  if (!child_executor_->Next(tuple, rid)) {
    return false;
  }

  numbers_++;
  return numbers_ <= plan_->GetLimit();
}

}  // namespace bustub
