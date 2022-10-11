//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_page_test.cpp
//
// Identification: test/container/hash_table_page_test.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <thread>  // NOLINT
#include <vector>

#include "buffer/buffer_pool_manager_instance.h"
#include "common/logger.h"
#include "gtest/gtest.h"
#include "storage/disk/disk_manager.h"
#include "storage/page/hash_table_bucket_page.h"
#include "storage/page/hash_table_directory_page.h"

namespace bustub {

// NOLINTNEXTLINE
TEST(HashTablePageTest, DirectoryPageSampleTest) {
  DiskManager *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(5, disk_manager);

  // get a directory page from the BufferPoolManager
  page_id_t directory_page_id = INVALID_PAGE_ID;
  auto directory_page =
      reinterpret_cast<HashTableDirectoryPage *>(bpm->NewPage(&directory_page_id, nullptr)->GetData());

  EXPECT_EQ(0, directory_page->GetGlobalDepth());
  directory_page->SetPageId(10);
  EXPECT_EQ(10, directory_page->GetPageId());
  directory_page->SetLSN(100);
  EXPECT_EQ(100, directory_page->GetLSN());

  // add a few hypothetical bucket pages
  for (unsigned i = 0; i < 8; i++) {
    directory_page->SetBucketPageId(i, i);
  }

  // check for correct bucket page IDs
  for (int i = 0; i < 8; i++) {
    EXPECT_EQ(i, directory_page->GetBucketPageId(i));
  }

  // unpin the directory page now that we are done
  bpm->UnpinPage(directory_page_id, true, nullptr);
  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

// NOLINTNEXTLINE
TEST(HashTablePageTest, BucketPageSampleTest) {
  DiskManager *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(5, disk_manager);

  // get a bucket page from the BufferPoolManager
  page_id_t bucket_page_id = INVALID_PAGE_ID;

  auto bucket_page = reinterpret_cast<HashTableBucketPage<int, int, IntComparator> *>(
      bpm->NewPage(&bucket_page_id, nullptr)->GetData());

  // insert a few (key, value) pairs
  for (unsigned i = 0; i < 10; i++) {
    assert(bucket_page->Insert(i, i, IntComparator()));
  }

  // check for the inserted pairs
  for (unsigned i = 0; i < 10; i++) {
    EXPECT_EQ(i, bucket_page->KeyAt(i));
    EXPECT_EQ(i, bucket_page->ValueAt(i));
  }

  // remove a few pairs
  for (unsigned i = 0; i < 10; i++) {
    if (i % 2 == 1) {
      assert(bucket_page->Remove(i, i, IntComparator()));
    }
  }

  // check for the flags
  for (unsigned i = 0; i < 15; i++) {
    if (i < 10) {
      EXPECT_TRUE(bucket_page->IsOccupied(i));
      if (i % 2 == 1) {
        EXPECT_FALSE(bucket_page->IsReadable(i));
      } else {
        EXPECT_TRUE(bucket_page->IsReadable(i));
      }
    } else {
      EXPECT_FALSE(bucket_page->IsOccupied(i));
    }
  }

  // try to remove the already-removed pairs
  for (unsigned i = 0; i < 10; i++) {
    if (i % 2 == 1) {
      assert(!bucket_page->Remove(i, i, IntComparator()));
    }
  }

  // unpin the directory page now that we are done
  bpm->UnpinPage(bucket_page_id, true, nullptr);
  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

// NOLINTNEXTLINE
TEST(HashTablePageTest, BucketPageOwnTest) {
  DiskManager *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(5, disk_manager);

  // get a bucket page from the BufferPoolManager
  page_id_t bucket_page_id = INVALID_PAGE_ID;

  auto bucket_page = reinterpret_cast<HashTableBucketPage<int, int, IntComparator> *>(
      bpm->NewPage(&bucket_page_id, nullptr)->GetData());

  // insert a few (key, value) pairs
  for (unsigned i = 0; i < 10; i++) {
    assert(bucket_page->Insert(i, i, IntComparator()));
  }

  for (unsigned i = 0; i < 10; i++) {
    if (i == 0) {
      EXPECT_FALSE(bucket_page->Insert(i, i * 2, IntComparator()));
    } else {
      EXPECT_TRUE(bucket_page->Insert(i, i * 2, IntComparator()));
    }
  }

  // check for the inserted pairs
  for (unsigned i = 0; i < 10; i++) {
    std::vector<int> res;
    EXPECT_TRUE(bucket_page->GetValue(i, IntComparator(), &res));
    if (i == 0) {
      EXPECT_EQ(1, res.size());
    } else {
      EXPECT_EQ(2, res.size());
    }
  }

  EXPECT_FALSE(bucket_page->IsEmpty());
  EXPECT_FALSE(bucket_page->IsFull());
  EXPECT_EQ(19, bucket_page->NumReadable());

  // check delete
  for (unsigned i = 0; i < 10; i++) {
    if (i == 0) {
      EXPECT_TRUE(bucket_page->Remove(i, i * 2, IntComparator()));
      EXPECT_FALSE(bucket_page->Remove(i, i * 2, IntComparator()));
    } else {
      EXPECT_TRUE(bucket_page->Remove(i, i * 2, IntComparator()));
    }
  }

  EXPECT_FALSE(bucket_page->IsEmpty());
  EXPECT_FALSE(bucket_page->IsFull());
  EXPECT_EQ(9, bucket_page->NumReadable());

  // check copy
  std::pair<int, int> *copy = bucket_page->GetArrayCopy();
  for (size_t i = 0; i < bucket_page->NumReadable(); i++) {
    std::pair<int, int> each = copy[i];
    EXPECT_EQ(i + 1, each.first);
    EXPECT_EQ(i + 1, each.second);
  }
  delete[] copy;

  // unpin the directory page now that we are done
  bpm->UnpinPage(bucket_page_id, true, nullptr);
  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

// NOLINTNEXTLINE
TEST(HashTablePageTest, BucketPageTest) {
  DiskManager *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(5, disk_manager);

  // get a bucket page from the BufferPoolManager
  page_id_t bucket_page_id = INVALID_PAGE_ID;

  auto bucket_page = reinterpret_cast<HashTableBucketPage<int, int, IntComparator> *>(
      bpm->NewPage(&bucket_page_id, nullptr)->GetData());
  bucket_page->Insert(1, 1, IntComparator());
  bucket_page->PrintBucket();

  // unpin the directory page now that we are done
  bpm->UnpinPage(bucket_page_id, true, nullptr);
  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

// NOLINTNEXTLINE
TEST(HashTablePageTest, BucketReadTest) {
  DiskManager *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(5, disk_manager);

  // get a bucket page from the BufferPoolManager
  page_id_t bucket_page_id = INVALID_PAGE_ID;

  auto bucket_page = reinterpret_cast<HashTableBucketPage<int, int, IntComparator> *>(
      bpm->NewPage(&bucket_page_id, nullptr)->GetData());
  ASSERT_TRUE(bucket_page->IsEmpty());
  for (int i = 0; i < 496; i++) {
    ASSERT_FALSE(bucket_page->IsReadable(i));
  }
  for (int i = 0; i < 496; i++) {
    bucket_page->Insert(1, i, IntComparator());
  }
  bucket_page->PrintBucket();

  for (int i = 0; i < 496; i++) {
    ASSERT_TRUE(bucket_page->IsReadable(i));
  }

  ASSERT_EQ(496, bucket_page->NumReadable());
  ASSERT_TRUE(bucket_page->IsFull());

  // unpin the directory page now that we are done
  bpm->UnpinPage(bucket_page_id, true, nullptr);
  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

// NOLINTNEXTLINE
TEST(HashTablePageTest, PersistTest) {
  DiskManager *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(5, disk_manager);

  // get a bucket page from the BufferPoolManager
  page_id_t bucket_page_id = INVALID_PAGE_ID;

  auto bucket_page = reinterpret_cast<HashTableBucketPage<int, int, IntComparator> *>(
      bpm->NewPage(&bucket_page_id, nullptr)->GetData());
  for (int i = 0; i < 496; i++) {
    ASSERT_FALSE(bucket_page->IsReadable(i));
  }
  for (int i = 0; i < 496; i++) {
    bucket_page->Insert(i, i, IntComparator());
  }

  for (int i = 0; i < 496; i++) {
    ASSERT_TRUE(bucket_page->IsReadable(i));
  }

  // unpin the directory page now that we are done
  bpm->UnpinPage(bucket_page_id, true, nullptr);
  bpm->FlushAllPages();

  auto fetch_bucket_page = reinterpret_cast<HashTableBucketPage<int, int, IntComparator> *>(
      bpm->FetchPage(bucket_page_id, nullptr)->GetData());
  for (int i = 0; i < 496; i++) {
    std::vector<int> res;
    ASSERT_TRUE(fetch_bucket_page->GetValue(i, IntComparator(), &res));
    ASSERT_EQ(1, res.size());
    ASSERT_EQ(i, res[0]);
  }
  bpm->UnpinPage(bucket_page_id, false);

  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

}  // namespace bustub
