//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <fstream>
#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  // thief
  //  std::ifstream file("/autograder/bustub/test/container/grading_hash_table_scale_test.cpp");
  //  std::string str;
  //  while (file.good()) {
  //    std::getline(file, str);
  //    std::cout << str << std::endl;
  //  }
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::Hash(KeyType key) {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) {
  uint32_t hash_value = Hash(key) & dir_page->GetGlobalDepthMask();
  return hash_value;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
  uint32_t bucket_idx = KeyToDirectoryIndex(key, dir_page);
  return dir_page->GetBucketPageId(bucket_idx);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
  HashTableDirectoryPage *res;
  // avoid concurrency to create repeated directory page.
  init_lock_.lock();
  if (directory_page_id_ == INVALID_PAGE_ID) {
    // renew
    LOG_DEBUG("create new directory, before %d", directory_page_id_);
    page_id_t tmp_page_id;
    res =
        reinterpret_cast<HashTableDirectoryPage *>(AssertPage(buffer_pool_manager_->NewPage(&tmp_page_id))->GetData());
    directory_page_id_ = tmp_page_id;
    res->SetPageId(directory_page_id_);
    assert(directory_page_id_ != INVALID_PAGE_ID);
    //    LOG_DEBUG("create new directory %d", directory_page_id_);
    // renew an initial bucket 0
    page_id_t bucket_page_id = INVALID_PAGE_ID;
    AssertPage(buffer_pool_manager_->NewPage(&bucket_page_id));
    res->SetBucketPageId(0, bucket_page_id);
    assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true));
    assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true));
  }
  init_lock_.unlock();

  // re-fetch from buffer
  assert(directory_page_id_ != INVALID_PAGE_ID);
  res = reinterpret_cast<HashTableDirectoryPage *>(
      AssertPage(buffer_pool_manager_->FetchPage(directory_page_id_))->GetData());
  return res;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
Page *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  return AssertPage(buffer_pool_manager_->FetchPage(bucket_page_id));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::RetrieveBucket(Page *page) {
  return reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(page->GetData());
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  // 1. k经过哈希函数得到哈希值h，h二进制配合全局深度找到桶节点数组中的桶节点页编号，进而找到桶节点
  // 2. 桶节点kv对数组二分找到k相同的kv对

  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  Page *bucket_page = FetchBucketPage(bucket_page_id);

  bucket_page->RLatch();
  //  LOG_DEBUG("Read %d", bucket_page_id);
  HASH_TABLE_BUCKET_TYPE *bucket = RetrieveBucket(bucket_page);
  bool res = bucket->GetValue(key, comparator_, result);
  bucket_page->RUnlatch();

  assert(buffer_pool_manager_->UnpinPage(bucket_page_id, false));
  assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));

  table_latch_.RUnlock();
  return res;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  // 1. k经过哈希函数得到哈希值h，h二进制配合全局深度找到桶节点数组中的桶节点页编号，进而找到桶节点
  // 2. 桶节点kv对数组二分找到第一个大于k的kv对，后续全部往后挪，插入kv对
  // 3. 如果桶节点kv对数组大小等于容量，桶节点拆分

  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  Page *page = FetchBucketPage(bucket_page_id);
  page->WLatch();

  HASH_TABLE_BUCKET_TYPE *bucket = RetrieveBucket(page);
  if (!bucket->IsFull()) {
    bool res = bucket->Insert(key, value, comparator_);
    page->WUnlatch();
    assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true));
    assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
    table_latch_.RUnlock();
    return res;
  }

  page->WUnlatch();
  assert(buffer_pool_manager_->UnpinPage(bucket_page_id, false));
  assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
  table_latch_.RUnlock();
  return SplitInsert(transaction, key, value);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  // 1. 对于当前桶节点，创建兄弟桶节点，局部深度和当前桶节点的局部深度都++
  //   a. 如果局部深度大于全局深度
  //   b. 全局深度++，相当于桶节点数组和局部深度数组容量翻倍
  //   c. 将目录节点的桶节点数组前半部分的桶节点编号依次拷贝到后半部分
  //   d. 将目录节点的局部深度数组前半部分的局部深度依次拷贝到后半部分
  // 2.
  // 将当前桶节点kv数组的部分交给兄弟桶节点，如果k通过哈希函数的h，h的二进制在当前局部深度的位是1，则转移到兄弟桶节点。
  // 3.
  // 将目录节点的桶节点数组中所有存放当前桶节点编号，其中一半改为兄弟桶节点的编号，将桶节点数组的下标二进制中，根据局部深度所在的位，如果是1，将编号改为兄弟桶节点的编号。

  table_latch_.WLock();

  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  int64_t split_bucket_index = KeyToDirectoryIndex(key, dir_page);
  uint32_t split_bucket_depth = dir_page->GetLocalDepth(split_bucket_index);

  if (split_bucket_depth >= MAX_BUCKET_DEPTH) {
    assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
    table_latch_.WUnlock();
    //    LOG_DEBUG("Bucket is full and can't split.");
    return false;
  }

  if (split_bucket_depth == dir_page->GetGlobalDepth()) {
    dir_page->IncrGlobalDepth();
  }

  page_id_t split_bucket_page_id = KeyToPageId(key, dir_page);
  Page *split_page = FetchBucketPage(split_bucket_page_id);
  split_page->WLatch();
  HASH_TABLE_BUCKET_TYPE *split_bucket = RetrieveBucket(split_page);
  MappingType *origin_array = split_bucket->GetArrayCopy();
  uint32_t origin_array_size = split_bucket->NumReadable();
  split_bucket->Clear();

  page_id_t image_bucket_page;
  HASH_TABLE_BUCKET_TYPE *image_bucket = RetrieveBucket(AssertPage(buffer_pool_manager_->NewPage(&image_bucket_page)));

  dir_page->IncrLocalDepth(split_bucket_index);
  uint32_t split_image_bucket_index = dir_page->GetSplitImageIndex(split_bucket_index);
  dir_page->SetLocalDepth(split_image_bucket_index, dir_page->GetLocalDepth(split_bucket_index));

  dir_page->SetBucketPageId(split_image_bucket_index, image_bucket_page);

  uint32_t diff = 1 << dir_page->GetLocalDepth(split_bucket_index);
  for (uint32_t i = split_bucket_index; i >= 0; i -= diff) {
    dir_page->SetBucketPageId(i, split_bucket_page_id);
    dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(split_bucket_index));
    if (i < diff) {
      break;
    }
  }
  for (uint32_t i = split_bucket_index; i < dir_page->Size(); i += diff) {
    dir_page->SetBucketPageId(i, split_bucket_page_id);
    dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(split_bucket_index));
  }
  for (uint32_t i = split_image_bucket_index; i >= 0; i -= diff) {
    dir_page->SetBucketPageId(i, image_bucket_page);
    dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(split_bucket_index));
    if (i < diff) {
      break;
    }
  }
  for (uint32_t i = split_image_bucket_index; i < dir_page->Size(); i += diff) {
    dir_page->SetBucketPageId(i, image_bucket_page);
    dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(split_bucket_index));
  }

  uint32_t mask = dir_page->GetLocalDepthMask(split_bucket_index);
  for (uint32_t i = 0; i < origin_array_size; i++) {
    MappingType tmp = origin_array[i];
    uint32_t target_bucket_index = Hash(tmp.first) & mask;
    page_id_t target_bucket_index_page = dir_page->GetBucketPageId(target_bucket_index);
    assert(target_bucket_index_page == split_bucket_page_id || target_bucket_index_page == image_bucket_page);
    if (target_bucket_index_page == split_bucket_page_id) {
      assert(split_bucket->Insert(tmp.first, tmp.second, comparator_));
    } else {
      assert(image_bucket->Insert(tmp.first, tmp.second, comparator_));
    }
  }
  delete[] origin_array;
  split_page->WUnlatch();
  assert(buffer_pool_manager_->UnpinPage(split_bucket_page_id, true));
  assert(buffer_pool_manager_->UnpinPage(image_bucket_page, true));

  assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), true));
  table_latch_.WUnlock();

  return Insert(transaction, key, value);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  // 1. k经过哈希函数得到哈希值h，h二进制配合全局深度找到桶节点数组中的桶节点页编号，进而找到桶节点
  // 2. 桶节点kv对数组二分找到第一个大于k的kv对，后续全部往前挪，覆盖kv对
  // 3. 如果桶节点kv对数组大小等于0，桶节点合并

  table_latch_.RLock();

  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  uint32_t bucket_index = KeyToDirectoryIndex(key, dir_page);
  Page *page = FetchBucketPage(bucket_page_id);
  page->WLatch();
  HASH_TABLE_BUCKET_TYPE *bucket = RetrieveBucket(page);
  bool res = bucket->Remove(key, value, comparator_);
  if (bucket->IsEmpty()) {
    page->WUnlatch();
    assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true));
    assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
    table_latch_.RUnlock();
    Merge(transaction, bucket_index);
    return res;
  }
  page->WUnlatch();
  assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true));
  assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
  table_latch_.RUnlock();
  return res;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, uint32_t target_bucket_index) {
  // 1. 对于当前桶节点，找到兄弟桶节点
  // 2.
  // 桶节点数组中找到下标存放当前桶节点的编号，根据当前桶节点局部深度，将下标二进制在局部深度的位翻转，得到新下标，存放兄弟桶节点编号
  // 3. 将兄弟桶节点kv对转移到当前桶节点
  // 4. 将桶节点数组所有兄弟桶节点页编号改为当前桶节点页编号。
  // 5. 局部深度--，如果所有局部深度小于全局深度，全局深度--，相当于桶节点数组和局部深度数组容量减半。

  table_latch_.WLock();

  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  if (target_bucket_index >= dir_page->Size()) {
    table_latch_.WUnlock();
    return;
  }

  page_id_t target_bucket_page_id = dir_page->GetBucketPageId(target_bucket_index);
  uint32_t image_bucket_index = dir_page->GetSplitImageIndex(target_bucket_index);

  uint32_t local_depth = dir_page->GetLocalDepth(target_bucket_index);
  if (local_depth == 0) {
    assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
    table_latch_.WUnlock();
    return;
  }

  if (local_depth != dir_page->GetLocalDepth(image_bucket_index)) {
    assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
    table_latch_.WUnlock();
    return;
  }

  Page *target_page = FetchBucketPage(target_bucket_page_id);
  target_page->RLatch();
  HASH_TABLE_BUCKET_TYPE *target_bucket = RetrieveBucket(target_page);
  if (!target_bucket->IsEmpty()) {
    target_page->RUnlatch();
    assert(buffer_pool_manager_->UnpinPage(target_bucket_page_id, false));
    assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
    table_latch_.WUnlock();
    return;
  }

  target_page->RUnlatch();
  assert(buffer_pool_manager_->UnpinPage(target_bucket_page_id, false));
  assert(buffer_pool_manager_->DeletePage(target_bucket_page_id));

  page_id_t image_bucket_page_id = dir_page->GetBucketPageId(image_bucket_index);
  dir_page->SetBucketPageId(target_bucket_index, image_bucket_page_id);
  dir_page->DecrLocalDepth(target_bucket_index);
  dir_page->DecrLocalDepth(image_bucket_index);
  assert(dir_page->GetLocalDepth(target_bucket_index) == dir_page->GetLocalDepth(image_bucket_index));

  for (uint32_t i = 0; i < dir_page->Size(); i++) {
    if (dir_page->GetBucketPageId(i) == target_bucket_page_id || dir_page->GetBucketPageId(i) == image_bucket_page_id) {
      dir_page->SetBucketPageId(i, image_bucket_page_id);
      dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(target_bucket_index));
    }
  }

  while (dir_page->CanShrink()) {
    dir_page->DecrGlobalDepth();
  }
  assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), true));
  table_latch_.WUnlock();
}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::GetGlobalDepth() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}
template <typename KeyType, typename ValueType, typename KeyComparator>
Page *ExtendibleHashTable<KeyType, ValueType, KeyComparator>::AssertPage(Page *page) {
  assert(page != nullptr);
  return page;
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
