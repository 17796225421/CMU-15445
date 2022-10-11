//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {}

void NestedLoopJoinExecutor::Init() { left_executor_->Init(); }

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  // 1. 答案队列不为空，取出来直接返回
  // 2. 外循环调用左儿子执行器next获取左输入行，然后调用右儿子执行器init。
  // 3. 内循环调用右儿子执行器next获取右输入行
  // 4. 左输入行、右输入行用计划节点的满足条件判断，如果不满足continue。
  // 5. 准备一个输出行，对输出行每个列遍历，输出行的列类型信息来自计划节点
  // 6. 使用左输入行、右输入行获取当前列的值
  // 7. 内循环结束，将输出行放到答案队列，准备新的输出行
  // 8. 最后从答案队列取出一个答案。

  if (!tmp_results_.empty()) {
    *tuple = tmp_results_.front();
    tmp_results_.pop();
    return true;
  }

  Tuple left_tuple;
  RID left_rid;

  Tuple right_tuple;
  RID right_rid;
  if (!left_executor_->Next(&left_tuple, &left_rid)) {
    return false;
  }

  right_executor_->Init();
  while (right_executor_->Next(&right_tuple, &right_rid)) {
    if (plan_->Predicate() == nullptr || plan_->Predicate()
                                             ->EvaluateJoin(&left_tuple, left_executor_->GetOutputSchema(),
                                                            &right_tuple, right_executor_->GetOutputSchema())
                                             .GetAs<bool>()) {
      std::vector<Value> output;
      for (const auto &col : GetOutputSchema()->GetColumns()) {
        output.push_back(col.GetExpr()->EvaluateJoin(&left_tuple, left_executor_->GetOutputSchema(), &right_tuple,
                                                     right_executor_->GetOutputSchema()));
      }
      tmp_results_.push(Tuple(output, GetOutputSchema()));
    }
  }

  return Next(tuple, rid);
}

}  // namespace bustub
