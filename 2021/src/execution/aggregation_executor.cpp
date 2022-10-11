//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(SimpleAggregationHashTable(plan->GetAggregates(), plan->GetAggregateTypes())),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  child_->Init();
  Tuple tuple;
  RID rid;
  while (child_->Next(&tuple, &rid)) {
    aht_.InsertCombine(MakeAggregateKey(&tuple), MakeAggregateValue(&tuple));
  }
  aht_iterator_ = aht_.Begin();
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  // 1. init函数循环使用儿子执行器next，将所有输入行放到哈希表
  // 2. 循环，遍历哈希表，取出当前kv
  // 3. 对于当前kv，如果不满足having条件，下一循环
  // 4. 如果满足having条件，准备一个输出行，对输出行的每个列遍历，得到输出行当前列的值
  // 5. 组装完输出行，返回

  if (aht_iterator_ == aht_.End()) {
    return false;
  }
  const AggregateKey &agg_key = aht_iterator_.Key();
  const AggregateValue &agg_value = aht_iterator_.Val();
  ++aht_iterator_;

  // 判断Having条件，符合返回，不符合则继续查找
  if (plan_->GetHaving() == nullptr ||
      plan_->GetHaving()->EvaluateAggregate(agg_key.group_bys_, agg_value.aggregates_).GetAs<bool>()) {
    std::vector<Value> ret;
    for (const auto &col : plan_->OutputSchema()->GetColumns()) {
      ret.push_back(col.GetExpr()->EvaluateAggregate(agg_key.group_bys_, agg_value.aggregates_));
    }
    *tuple = Tuple(ret, plan_->OutputSchema());
    return true;
  }
  return Next(tuple, rid);
}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

}  // namespace bustub
