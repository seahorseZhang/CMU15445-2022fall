//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager_rc_test.cpp
//
// Identification: test/concurrency/lock_manager_rc_test.cpp
//
// Tests for LockManager under READ_COMMITTED isolation
//
//===----------------------------------------------------------------------===//

#include "gtest/gtest.h"

#include "concurrency/lock_manager.h"
#include "concurrency/transaction.h"
#include "common/rid.h"
#include <thread>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>

namespace bustub {

// 验证在 READ_COMMITTED 下：事务释放共享行锁后，另一个事务可以获取该行的独占锁
TEST(LockManagerTest, ReadCommittedRowUnlockAllowsExclusive) {
  LockManager lm;
  // 两个事务，隔离级别为 READ_COMMITTED
  Transaction t1(0, IsolationLevel::READ_COMMITTED);
  Transaction t2(1, IsolationLevel::READ_COMMITTED);

  const table_oid_t oid = 42;
  const RID rid(0, 0);

  // t1 先获取表意向共享锁，然后对行加共享锁
  EXPECT_NO_THROW({ ASSERT_TRUE(lm.LockTable(&t1, LockManager::LockMode::INTENTION_SHARED, oid)); });
  ASSERT_TRUE(lm.LockRow(&t1, LockManager::LockMode::SHARED, oid, rid));

  // t1 释放行锁（READ_COMMITTED 下释放 S 不应使事务进入 SHRINKING）
  ASSERT_TRUE(lm.UnlockRow(&t1, oid, rid));

  // t2 获取表意向独占锁，然后尝试获取行独占锁
  EXPECT_NO_THROW({ ASSERT_TRUE(lm.LockTable(&t2, LockManager::LockMode::INTENTION_EXCLUSIVE, oid)); });
  // 因为 t1 已经释放行共享锁，t2 应该能够获取独占行锁
  ASSERT_TRUE(lm.LockRow(&t2, LockManager::LockMode::EXCLUSIVE, oid, rid));

  // 清理：释放 t2 的行锁与表锁
  ASSERT_TRUE(lm.UnlockRow(&t2, oid, rid));
  ASSERT_TRUE(lm.UnlockTable(&t2, oid));
  // 释放 t1 的表锁
  ASSERT_TRUE(lm.UnlockTable(&t1, oid));
}

// 验证在 READ_COMMITTED 下：如果事务在 SHRINKING 状态则禁止获取新的行锁
TEST(LockManagerTest, ReadCommittedShrinkPreventsRowLock) {
  LockManager lm;
  Transaction t1(0, IsolationLevel::READ_COMMITTED);

  const table_oid_t oid = 43;
  const RID rid(1, 1);

  // t1 获取表的独占锁，然后释放（释放 X 会将事务置为 SHRINKING）
  ASSERT_TRUE(lm.LockTable(&t1, LockManager::LockMode::EXCLUSIVE, oid));
  // 获取并释放一个行独占锁以触发状态变化
  ASSERT_TRUE(lm.LockRow(&t1, LockManager::LockMode::EXCLUSIVE, oid, rid));
  ASSERT_TRUE(lm.UnlockRow(&t1, oid, rid));
  ASSERT_TRUE(lm.UnlockTable(&t1, oid));

  // 事务现在应处于 SHRINKING 状态（对于 READ_COMMITTED，解锁 X 会触发 SHRINKING）
  ASSERT_EQ(t1.GetState(), TransactionState::SHRINKING);

  // 之后尝试获取新的行锁（例如 EXCLUSIVE）应导致 abort（抛出或返回 false）
  // 这里我们期望抛出 TransactionAbortException
  EXPECT_THROW(lm.LockRow(&t1, LockManager::LockMode::EXCLUSIVE, oid, RID(2, 2)), TransactionAbortException);
}

// 验证在 READ_COMMITTED 下：禁止脏读（事务在另一个事务未提交前不能读取其独占写入）
TEST(LockManagerTest, ReadCommittedPreventsDirtyRead) {
  LockManager lm;
  Transaction t1(0, IsolationLevel::READ_COMMITTED);
  Transaction t2(1, IsolationLevel::READ_COMMITTED);

  const table_oid_t oid = 50;
  const RID rid(2, 2);

  std::atomic<bool> t2_acquired{false};

  // t1 获取写锁并保持
  ASSERT_TRUE(lm.LockTable(&t1, LockManager::LockMode::INTENTION_EXCLUSIVE, oid));
  ASSERT_TRUE(lm.LockRow(&t1, LockManager::LockMode::EXCLUSIVE, oid, rid));

  // t2 在独立线程尝试获取共享锁（应被阻塞，直到 t1 释放）
  std::thread th([&]() {
    // 需要先获取表级意向共享锁
    ASSERT_TRUE(lm.LockTable(&t2, LockManager::LockMode::INTENTION_SHARED, oid));
    // 下面的 LockRow 会被阻塞直到 t1 释放写锁
    ASSERT_TRUE(lm.LockRow(&t2, LockManager::LockMode::SHARED, oid, rid));
    t2_acquired.store(true);
    ASSERT_TRUE(lm.UnlockRow(&t2, oid, rid));
    ASSERT_TRUE(lm.UnlockTable(&t2, oid));
  });

  // 等待一段时间，确保 t2 无法立即获得锁
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  EXPECT_EQ(false, t2_acquired.load());

  // 释放 t1 的行锁，使 t2 能够获取
  ASSERT_TRUE(lm.UnlockRow(&t1, oid, rid));
  ASSERT_TRUE(lm.UnlockTable(&t1, oid));

  th.join();
  EXPECT_EQ(true, t2_acquired.load());
}

// 验证在 READ_COMMITTED 下：允许不可重复读（事务在再次读取时可能看到其他事务已提交的更改）
TEST(LockManagerTest, ReadCommittedAllowsNonRepeatableRead) {
  LockManager lm;
  Transaction t1(0, IsolationLevel::READ_COMMITTED);
  Transaction t2(1, IsolationLevel::READ_COMMITTED);

  const table_oid_t oid = 51;
  const RID rid(3, 3);

  int value = 0;  // 模拟被读/改写的数据

  std::mutex mu;
  std::condition_variable cv;
  bool t2_first_read_done = false;
  bool t1_updated = false;
  int first_read = -1;
  int second_read = -1;

  // t2: 先读取一次（获取并释放共享锁），等待 t1 提交后再读第二次
  std::thread th2([&]() {
    ASSERT_TRUE(lm.LockTable(&t2, LockManager::LockMode::INTENTION_SHARED, oid));
    ASSERT_TRUE(lm.LockRow(&t2, LockManager::LockMode::SHARED, oid, rid));
    first_read = value;
    ASSERT_TRUE(lm.UnlockRow(&t2, oid, rid));
    ASSERT_TRUE(lm.UnlockTable(&t2, oid));

    {
      std::lock_guard<std::mutex> lg(mu);
      t2_first_read_done = true;
    }
    cv.notify_one();

    // 等待 t1 完成更新并提交
    {
      std::unique_lock<std::mutex> ul(mu);
      cv.wait(ul, [&] { return t1_updated; });
    }

    // 重新读取（应能看到 t1 的提交更改）
    ASSERT_TRUE(lm.LockTable(&t2, LockManager::LockMode::INTENTION_SHARED, oid));
    ASSERT_TRUE(lm.LockRow(&t2, LockManager::LockMode::SHARED, oid, rid));
    second_read = value;
    ASSERT_TRUE(lm.UnlockRow(&t2, oid, rid));
    ASSERT_TRUE(lm.UnlockTable(&t2, oid));
  });

  // t1: 等待 t2 完成第一次读取后，获取写锁并更新，然后释放（模拟提交）
  std::thread th1([&]() {
    {
      std::unique_lock<std::mutex> ul(mu);
      cv.wait(ul, [&] { return t2_first_read_done; });
    }
    ASSERT_TRUE(lm.LockTable(&t1, LockManager::LockMode::INTENTION_EXCLUSIVE, oid));
    ASSERT_TRUE(lm.LockRow(&t1, LockManager::LockMode::EXCLUSIVE, oid, rid));
    // 执行“写入”
    value = 12345;
    ASSERT_TRUE(lm.UnlockRow(&t1, oid, rid));
    ASSERT_TRUE(lm.UnlockTable(&t1, oid));

    {
      std::lock_guard<std::mutex> lg(mu);
      t1_updated = true;
    }
    cv.notify_one();
  });

  th1.join();
  th2.join();

  // 在 READ_COMMITTED 下，第二次读取应能看到 t1 的更改，从而导致不可重复读
  EXPECT_NE(first_read, second_read);
}

}  // namespace bustub
