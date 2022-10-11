//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <queue>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

struct HashJoinKey {
  std::vector<Value> join_bys_;

  bool operator==(const HashJoinKey &other) const {
    for (uint32_t i = 0; i < other.join_bys_.size(); i++) {
      if (join_bys_[i].CompareEquals(other.join_bys_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};
}  // namespace bustub

namespace std {

template <>
struct hash<bustub::HashJoinKey> {
  std::size_t operator()(const bustub::HashJoinKey &hash_join_key) const {
    size_t curr_hash = 0;
    for (const auto &key : hash_join_key.join_bys_) {
      if (!key.IsNull()) {
        curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&key));
      }
    }
    return curr_hash;
  }
};

}  // namespace std

namespace bustub {

class SimpleJoinHashTable {
 public:
  void Insert(const HashJoinKey &hash_join_key, const Tuple &tuple) {
    if (ht_.count(hash_join_key) > 0) {
      ht_[hash_join_key].push_back(tuple);
    } else {
      ht_.insert({hash_join_key, {tuple}});
    }
  }

  std::vector<Tuple> Get(const HashJoinKey &hash_join_key) { return ht_[hash_join_key]; }

  uint64_t Count(const HashJoinKey &hash_join_key) { return ht_.count(hash_join_key); }

 private:
  std::unordered_map<HashJoinKey, std::vector<Tuple>> ht_{};
};

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join
   * @param[out] rid The next tuple RID produced by the join
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  bool Next(Tuple *tuple, RID *rid) override;

  /** @return The output schema for the join */
  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); };

 private:
  HashJoinKey MakeLeftHashJoinKey(const Tuple *tuple) {
    std::vector<Value> keys;
    keys.push_back(plan_->LeftJoinKeyExpression()->Evaluate(tuple, left_child_->GetOutputSchema()));
    return {keys};
  }

  HashJoinKey MakeRightHashJoinKey(const Tuple *tuple) {
    std::vector<Value> keys;
    keys.push_back(plan_->RightJoinKeyExpression()->Evaluate(tuple, right_child_->GetOutputSchema()));
    return {keys};
  }

  /** The NestedLoopJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  const std::unique_ptr<AbstractExecutor> left_child_;
  const std::unique_ptr<AbstractExecutor> right_child_;
  SimpleJoinHashTable jht_{};
  std::queue<Tuple> tmp_results_{};
};

}  // namespace bustub
