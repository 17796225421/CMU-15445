//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_test.cpp
//
// Identification: test/container/hash_table_test.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <thread>  // NOLINT
#include <vector>

#include "buffer/buffer_pool_manager_instance.h"
#include "common/logger.h"
#include "container/hash/extendible_hash_table.h"
#include "gtest/gtest.h"
#include "murmur3/MurmurHash3.h"

namespace bustub {

// NOLINTNEXTLINE

// NOLINTNEXTLINE
TEST(HashTableTest, SampleTest) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(50, disk_manager);
  ExtendibleHashTable<int, int, IntComparator> ht("blah", bpm, IntComparator(), HashFunction<int>());

  // insert a few values
  for (int i = 0; i < 5; i++) {
    ht.Insert(nullptr, i, i);
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    EXPECT_EQ(1, res.size()) << "Failed to insert " << i << std::endl;
    EXPECT_EQ(i, res[0]);
  }

  ht.VerifyIntegrity();

  // check if the inserted values are all there
  for (int i = 0; i < 5; i++) {
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    EXPECT_EQ(1, res.size()) << "Failed to keep " << i << std::endl;
    EXPECT_EQ(i, res[0]);
  }

  ht.VerifyIntegrity();

  // insert one more value for each key
  for (int i = 0; i < 5; i++) {
    if (i == 0) {
      // duplicate values for the same key are not allowed
      EXPECT_FALSE(ht.Insert(nullptr, i, 2 * i));
    } else {
      EXPECT_TRUE(ht.Insert(nullptr, i, 2 * i));
    }
    ht.Insert(nullptr, i, 2 * i);
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    if (i == 0) {
      // duplicate values for the same key are not allowed
      EXPECT_EQ(1, res.size());
      EXPECT_EQ(i, res[0]);
    } else {
      EXPECT_EQ(2, res.size());
      if (res[0] == i) {
        EXPECT_EQ(2 * i, res[1]);
      } else {
        EXPECT_EQ(2 * i, res[0]);
        EXPECT_EQ(i, res[1]);
      }
    }
  }

  ht.VerifyIntegrity();

  // look for a key that does not exist
  std::vector<int> res;
  ht.GetValue(nullptr, 20, &res);
  EXPECT_EQ(0, res.size());

  // delete some values
  for (int i = 0; i < 5; i++) {
    EXPECT_TRUE(ht.Remove(nullptr, i, i));
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    if (i == 0) {
      // (0, 0) is the only pair with key 0
      EXPECT_EQ(0, res.size());
    } else {
      EXPECT_EQ(1, res.size());
      EXPECT_EQ(2 * i, res[0]);
    }
  }

  ht.VerifyIntegrity();

  // delete all values
  for (int i = 0; i < 5; i++) {
    if (i == 0) {
      // (0, 0) has been deleted
      EXPECT_FALSE(ht.Remove(nullptr, i, 2 * i));
    } else {
      EXPECT_TRUE(ht.Remove(nullptr, i, 2 * i));
    }
  }

  ht.VerifyIntegrity();

  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

TEST(HashTableTest, OwnTest) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(50, disk_manager);
  ExtendibleHashTable<int, int, IntComparator> ht("blah", bpm, IntComparator(), HashFunction<int>());

  for (int i = 0; i < 500; i++) {
    ASSERT_TRUE(ht.Insert(nullptr, i, i));
  }

  for (int i = 0; i < 500; i++) {
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    ASSERT_EQ(1, res.size());
    ASSERT_EQ(i, res[0]);
  }

  ht.VerifyIntegrity();

  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

#define EACH_BUCKET_SIZE 496

TEST(HashTableTest, GrowShrinkTest1) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(4, disk_manager);
  ExtendibleHashTable<int, int, IntComparator> ht("blah", bpm, IntComparator(), HashFunction<int>());

  for (int i = 0; i < 500; i++) {
    auto key = i;
    auto value = i;
    EXPECT_TRUE(ht.Insert(nullptr, key, value));
    std::vector<int> res;
    EXPECT_TRUE(ht.GetValue(nullptr, key, &res));
    EXPECT_EQ(1, res.size()) << "Failed to insert " << i << std::endl;
    EXPECT_EQ(value, res[0]);
  }

  ht.VerifyIntegrity();

  for (int i = 0; i < 500; i++) {
    auto key = i;
    auto value = i;
    std::vector<int> res;
    EXPECT_TRUE(ht.GetValue(nullptr, key, &res));
    EXPECT_EQ(1, res.size()) << "Failed to insert " << i << std::endl;
    EXPECT_EQ(value, res[0]);
  }

  ht.VerifyIntegrity();

  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

TEST(HashTableTest, GrowShrinkTest2) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(20, disk_manager);
  ExtendibleHashTable<int, int, IntComparator> ht("blah", bpm, IntComparator(), HashFunction<int>());

  const int data_size = 1000;

  for (int i = 0; i < data_size; i++) {
    ASSERT_TRUE(ht.Insert(nullptr, i, i));
  }

  for (int i = 0; i < data_size; i += 2) {
    ASSERT_TRUE(ht.Remove(nullptr, i, i));
  }

  ht.VerifyIntegrity();

  for (int i = 1; i < data_size; i += 2) {
    std::vector<int> res;
    ASSERT_TRUE(ht.GetValue(nullptr, i, &res));
    ASSERT_EQ(1, res.size());
    ASSERT_EQ(i, res[0]);
    ASSERT_TRUE(ht.Remove(nullptr, i, i));
  }

  ht.VerifyIntegrity();

  for (int i = 0; i < data_size; i++) {
    std::vector<int> res;
    ASSERT_FALSE(ht.GetValue(nullptr, i, &res));
  }

  for (int i = 0; i < data_size; i++) {
    ASSERT_TRUE(ht.Insert(nullptr, i, i));
  }

  for (int i = 0; i < data_size; i += 2) {
    ASSERT_TRUE(ht.Remove(nullptr, i, i));
  }

  ht.VerifyIntegrity();

  for (int i = 1; i < data_size; i += 2) {
    std::vector<int> res;
    ASSERT_TRUE(ht.GetValue(nullptr, i, &res));
    ASSERT_EQ(1, res.size());
    ASSERT_EQ(i, res[0]);
    ASSERT_TRUE(ht.Remove(nullptr, i, i));
  }

  ht.VerifyIntegrity();

  for (int i = 0; i < data_size; i++) {
    std::vector<int> res;
    ASSERT_FALSE(ht.GetValue(nullptr, i, &res));
  }

  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

TEST(HashTableTest, LargeInsertTest) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(30, disk_manager);
  ExtendibleHashTable<int, int, IntComparator> ht("blah", bpm, IntComparator(), HashFunction<int>());
  for (int i = 0; i < 5000; i++) {
    ASSERT_TRUE(ht.Insert(nullptr, i, i));
  }

  for (int i = 0; i < 5000; i++) {
    std::vector<int> res;
    ASSERT_TRUE(ht.GetValue(nullptr, i, &res));
    ASSERT_EQ(i, res[0]);
  }
  ht.VerifyIntegrity();

  for (int i = 0; i < 2500; i++) {
    ASSERT_TRUE(ht.Remove(nullptr, i, i));
  }

  ht.VerifyIntegrity();

  for (int i = 2500; i < 5000; i++) {
    std::vector<int> res;
    ASSERT_TRUE(ht.GetValue(nullptr, i, &res));
    ASSERT_EQ(i, res[0]);
  }

  ht.VerifyIntegrity();

  for (int i = 2500; i < 5000; i++) {
    ASSERT_TRUE(ht.Remove(nullptr, i, i));
  }

  ht.VerifyIntegrity();

  for (int i = 0; i < 5000; i++) {
    ASSERT_TRUE(ht.Insert(nullptr, i, i));
  }
  ht.VerifyIntegrity();

  for (int i = 0; i < 5000; i++) {
    std::vector<int> res;
    ASSERT_TRUE(ht.GetValue(nullptr, i, &res));
    ASSERT_EQ(i, res[0]);
  }
  ht.VerifyIntegrity();

  for (int i = 0; i < 5000; i++) {
    ASSERT_TRUE(ht.Remove(nullptr, i, i));
  }
  std::vector<int> res;
  ASSERT_FALSE(ht.GetValue(nullptr, 2500, &res));

  ht.VerifyIntegrity();

  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

TEST(HashTableTest, SplitInsertTest) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(30, disk_manager);
  ExtendibleHashTable<int, int, IntComparator> ht("blah", bpm, IntComparator(), HashFunction<int>());
  for (int i = 0; i < EACH_BUCKET_SIZE; i++) {
    ASSERT_TRUE(ht.Insert(nullptr, -1, i));
  }

  ht.VerifyIntegrity();

  for (int i = 0; i < EACH_BUCKET_SIZE; i++) {
    ASSERT_TRUE(ht.Insert(nullptr, 9, i));
  }

  ht.VerifyIntegrity();

  for (int i = 0; i < EACH_BUCKET_SIZE; i++) {
    ASSERT_TRUE(ht.Insert(nullptr, 23, i));
  }

  ht.VerifyIntegrity();

  for (int i = 0; i < EACH_BUCKET_SIZE; i++) {
    ASSERT_TRUE(ht.Insert(nullptr, 11, i));
  }

  ht.VerifyIntegrity();

  for (int i = 0; i < EACH_BUCKET_SIZE; i++) {
    ASSERT_TRUE(ht.Insert(nullptr, 15, i));
  }

  ht.VerifyIntegrity();

  for (int i = 0; i < EACH_BUCKET_SIZE; i++) {
    ASSERT_TRUE(ht.Insert(nullptr, 3, i));
  }

  ht.VerifyIntegrity();

  for (int i = 0; i < EACH_BUCKET_SIZE; i++) {
    ASSERT_TRUE(ht.Insert(nullptr, 338, i));
  }

  ht.VerifyIntegrity();

  for (int i = 0; i < EACH_BUCKET_SIZE; i++) {
    ASSERT_TRUE(ht.Insert(nullptr, 5, i));
  }

  ht.VerifyIntegrity();

  ASSERT_EQ(4, ht.GetGlobalDepth());

  for (int i = 0; i < EACH_BUCKET_SIZE; i++) {
    ASSERT_TRUE(ht.Remove(nullptr, -1, i));
    ASSERT_TRUE(ht.Remove(nullptr, 9, i));
    ASSERT_TRUE(ht.Remove(nullptr, 23, i));
    ASSERT_TRUE(ht.Remove(nullptr, 11, i));
    ASSERT_TRUE(ht.Remove(nullptr, 15, i));
    ASSERT_TRUE(ht.Remove(nullptr, 3, i));
    ASSERT_TRUE(ht.Remove(nullptr, 338, i));
    ASSERT_TRUE(ht.Remove(nullptr, 5, i));
  }

  ht.VerifyIntegrity();

  ASSERT_EQ(0, ht.GetGlobalDepth());

  // second times
  for (int i = 0; i < EACH_BUCKET_SIZE; i++) {
    ASSERT_TRUE(ht.Insert(nullptr, -1, i));
  }

  ht.VerifyIntegrity();

  for (int i = 0; i < EACH_BUCKET_SIZE; i++) {
    ASSERT_TRUE(ht.Insert(nullptr, 9, i));
  }

  ht.VerifyIntegrity();

  for (int i = 0; i < EACH_BUCKET_SIZE; i++) {
    ASSERT_TRUE(ht.Insert(nullptr, 23, i));
  }

  ht.VerifyIntegrity();

  for (int i = 0; i < EACH_BUCKET_SIZE; i++) {
    ASSERT_TRUE(ht.Insert(nullptr, 11, i));
  }

  ht.VerifyIntegrity();

  for (int i = 0; i < EACH_BUCKET_SIZE; i++) {
    ASSERT_TRUE(ht.Insert(nullptr, 15, i));
  }

  ht.VerifyIntegrity();

  for (int i = 0; i < EACH_BUCKET_SIZE; i++) {
    ASSERT_TRUE(ht.Insert(nullptr, 3, i));
  }

  ht.VerifyIntegrity();

  for (int i = 0; i < EACH_BUCKET_SIZE; i++) {
    ASSERT_TRUE(ht.Insert(nullptr, 338, i));
  }

  ht.VerifyIntegrity();

  for (int i = 0; i < EACH_BUCKET_SIZE; i++) {
    ASSERT_TRUE(ht.Insert(nullptr, 5, i));
  }

  ht.VerifyIntegrity();

  // todo
  for (int i = 0; i < EACH_BUCKET_SIZE; i++) {
    ASSERT_TRUE(ht.Insert(nullptr, 2, i));
  }

  ht.VerifyIntegrity();

  for (int i = 0; i < EACH_BUCKET_SIZE; i++) {
    ASSERT_TRUE(ht.Insert(nullptr, 351, i));
  }

  ht.VerifyIntegrity();

  for (int i = 0; i < EACH_BUCKET_SIZE; i++) {
    ASSERT_TRUE(ht.Insert(nullptr, 333, i));
  }

  ht.VerifyIntegrity();

  for (int i = 0; i < EACH_BUCKET_SIZE; i++) {
    ASSERT_TRUE(ht.Insert(nullptr, 211, i));
  }

  ht.VerifyIntegrity();

  for (int i = 0; i < EACH_BUCKET_SIZE; i++) {
    ASSERT_TRUE(ht.Insert(nullptr, 6, i));
  }

  ht.VerifyIntegrity();

  for (int i = 0; i < EACH_BUCKET_SIZE; i++) {
    ASSERT_TRUE(ht.Insert(nullptr, 13, i));
  }

  ht.VerifyIntegrity();

  for (int i = 0; i < EACH_BUCKET_SIZE; i++) {
    ASSERT_TRUE(ht.Insert(nullptr, 18, i));
  }

  ht.VerifyIntegrity();

  for (int i = 0; i < EACH_BUCKET_SIZE; i++) {
    ASSERT_TRUE(ht.Insert(nullptr, 1, i));
  }

  ht.VerifyIntegrity();

  ASSERT_EQ(4, ht.GetGlobalDepth());

  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

// steal

}  // namespace bustub
