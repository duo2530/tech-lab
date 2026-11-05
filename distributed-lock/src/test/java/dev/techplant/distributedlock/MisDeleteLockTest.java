package dev.techplant.distributedlock;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 复现：业务耗时 > 锁过期时间时，锁被自动释放，del 误删别人的锁。
 * <p>
 * 场景：业务耗时 7s，锁 TTL 3s，锁的 value 为 "线程名-UUID" 以便区分持有者。
 * <ol>
 *   <li>index1 业务没执行完，3s 后锁自动过期</li>
 *   <li>index3 / index2 相继拿到锁，同样 3s 后过期</li>
 *   <li>index1 执行完调用 del，此时删掉的是别人的锁</li>
 *   <li>index4 立刻拿到锁，与仍在执行业务的线程并发，互斥失效</li>
 * </ol>
 * 前置条件：本机 Redis(application.yaml 中配置) 已启动。运行约 20s。
 * 本测试断言"问题确实发生"，修复后（校验 value + Lua 原子删除）该断言应失败。
 * <p>
 * 某次运行结果如下：
 * <p>
 * ===== 时间线 =====
 * 14:57:41.880539300 index1 获取到锁 value=index1-7dcd9d32-22c2-4fe7-a8e9-5b5810506b0a
 * 14:57:44.852268500 index2 获取到锁 value=index2-372c8887-37ac-4710-aace-220535d76ff8
 * 14:57:47.911139300 index3 获取到锁 value=index3-ca5650b5-bee0-4feb-b1ba-d944b5368a13
 * 14:57:48.890357100 index1 业务完成，准备 del。当前 lock 的 value=index3-ca5650b5-bee0-4feb-b1ba-d944b5368a13，是否是自己的锁=false
 * 14:57:49.390784 index4 获取到锁 value=index4-4abe7f4d-387c-41cb-968b-e0ce3eb03b7c
 * 14:57:51.866548700 index2 业务完成，准备 del。当前 lock 的 value=index4-4abe7f4d-387c-41cb-968b-e0ce3eb03b7c，是否是自己的锁=false
 * 14:57:54.924502900 index3 业务完成，准备 del。当前 lock 的 value=null，是否是自己的锁=false
 * 14:57:56.405215100 index4 业务完成，准备 del。当前 lock 的 value=null，是否是自己的锁=false
 * 误删他人锁次数: 2
 * 同时持有"锁"执行业务的最大线程数: 3
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
class MisDeleteLockTest {

    private static final String LOCK_KEY = "lock:misdelete:demo";
    private static final long LOCK_TTL_SECONDS = 3;
    private static final long BUSINESS_MILLIS = 7000;

    @Autowired
    private StringRedisTemplate redisTemplate;

    private final List<String> timeline = new CopyOnWriteArrayList<>();
    private final AtomicInteger misDeleteCount = new AtomicInteger();
    private final AtomicInteger skippedDeleteCount = new AtomicInteger();
    private final AtomicInteger running = new AtomicInteger();
    private final AtomicInteger maxConcurrent = new AtomicInteger();

    @Test
    void reproduceMisDelete() throws Exception {
        redisTemplate.delete(LOCK_KEY);

        // 各线程相对起点的启动延迟(ms)：index1/2/3 间隔 1s，index4 在 7.5s 时出现
        String[] names = {"index1", "index2", "index3", "index4"};
        long[] startDelays = {0, 1000, 2000, 7500};

        CountDownLatch done = new CountDownLatch(names.length);
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < names.length; i++) {
            String name = names[i];
            long delay = startDelays[i];
            Thread t = new Thread(() -> {
                try {
                    Thread.sleep(delay);
                    runBusiness(name);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            }, name);
            threads.add(t);
            t.start();
        }

        assertTrue(done.await(60, TimeUnit.SECONDS), "测试超时");

        System.out.println("\n===== 时间线 =====");
        timeline.forEach(System.out::println);
        System.out.println("误删他人锁次数: " + misDeleteCount.get());
        System.out.println("同时持有\"锁\"执行业务的最大线程数: " + maxConcurrent.get());

        assertTrue(misDeleteCount.get() > 0, "应至少发生一次误删他人的锁");
        assertTrue(maxConcurrent.get() > 1, "应出现多个线程同时执行业务，说明互斥失效");
    }

    private void runBusiness(String name) throws InterruptedException {
        String myValue = name + "-" + UUID.randomUUID();
        // 自旋抢锁
        while (!Boolean.TRUE.equals(redisTemplate.opsForValue()
                .setIfAbsent(LOCK_KEY, myValue, LOCK_TTL_SECONDS, TimeUnit.SECONDS))) {
            Thread.sleep(100);
        }
        log("%s 获取到锁 value=%s", name, myValue);

        int now = running.incrementAndGet();
        maxConcurrent.accumulateAndGet(now, Math::max);
        try {
            Thread.sleep(BUSINESS_MILLIS); // 业务耗时 7s > TTL 3s
        } finally {
            running.decrementAndGet();
        }

        String current = redisTemplate.opsForValue().get(LOCK_KEY);
        boolean mine = myValue.equals(current);
        if (current != null && !mine) {
            misDeleteCount.incrementAndGet();
        }
        log("%s 业务完成，准备 del。当前 lock 的 value=%s，是否是自己的锁=%s", name, current, mine);
        redisTemplate.delete(LOCK_KEY); // 无条件 del：问题所在
    }

    @Test
    void solveMisDelete() throws Exception {
        redisTemplate.delete(LOCK_KEY);

        // 各线程相对起点的启动延迟(ms)：index1/2/3 间隔 1s，index4 在 7.5s 时出现
        String[] names = {"index1", "index2", "index3", "index4"};
        long[] startDelays = {0, 1000, 2000, 7500};

        CountDownLatch done = new CountDownLatch(names.length);
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < names.length; i++) {
            String name = names[i];
            long delay = startDelays[i];
            Thread t = new Thread(() -> {
                try {
                    Thread.sleep(delay);
                    runBusiness2(name);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            }, name);
            threads.add(t);
            t.start();
        }

        assertTrue(done.await(60, TimeUnit.SECONDS), "测试超时");

        System.out.println("\n===== 时间线 =====");
        timeline.forEach(System.out::println);
        System.out.println("误删他人锁次数: " + misDeleteCount.get());
        System.out.println("因锁已易主而跳过 del 的次数: " + skippedDeleteCount.get());
        System.out.println("同时持有\"锁\"执行业务的最大线程数: " + maxConcurrent.get());

        assertEquals(0, misDeleteCount.get(), "加 UUID 校验后，不应再误删他人的锁");
        assertTrue(skippedDeleteCount.get() > 0, "应有线程发现锁已不是自己的而放弃 del");
    }

    /**
     * 解决方案：加锁 value 用 UUID，释放前先比较 value 是否是自己的，是才 del。
     * <p>
     * 局限：
     * 1. 只防误删，不保证互斥。业务 7s > TTL 3s，锁仍会提前过期，多个线程仍会同时执行业务。
     * 2. "get 比较 + del"不是原子操作，get 之后 del 之前锁恰好过期并被他人获取，仍会误删。
     * 彻底解决需用 Lua 脚本把比较和删除合成一步。
     */
    private void runBusiness2(String name) throws InterruptedException {
        String myValue = name + "-" + UUID.randomUUID();
        // 自旋抢锁
        while (!Boolean.TRUE.equals(redisTemplate.opsForValue()
                .setIfAbsent(LOCK_KEY, myValue, LOCK_TTL_SECONDS, TimeUnit.SECONDS))) {
            Thread.sleep(100);
        }
        log("%s 获取到锁 value=%s", name, myValue);

        int now = running.incrementAndGet();
        maxConcurrent.accumulateAndGet(now, Math::max);

        try {
            Thread watchDog = new Thread(() -> {
                while (true) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    String current = redisTemplate.opsForValue().get(LOCK_KEY);
                    boolean mine = myValue.equals(current);
                    if (mine) {
                        log("看门狗给 %s 的锁 value=%s 续期", name, myValue);
                        redisTemplate.opsForValue().setIfAbsent(LOCK_KEY, myValue, LOCK_TTL_SECONDS, TimeUnit.SECONDS);
                    } else {
                        break;
                    }
                }
            });
            watchDog.start();
            
            Thread.sleep(BUSINESS_MILLIS); // 业务耗时 7s > TTL 3s

        } finally {
            running.decrementAndGet();
        }

        String current = redisTemplate.opsForValue().get(LOCK_KEY);
        boolean mine = myValue.equals(current);
        log("%s 业务完成，准备 del。当前 lock 的 value=%s，是否是自己的锁=%s", name, current, mine);
        if (!mine) {
            // 锁已过期或已易主：不能删
            skippedDeleteCount.incrementAndGet();
            log("%s 锁已不是自己的，跳过 del", name);
            return;
        }
        redisTemplate.delete(LOCK_KEY); // 校验过是自己的 UUID 才删

    }

    private void log(String fmt, Object... args) {
        timeline.add(LocalTime.now() + " " + fmt.formatted(args));
    }
}
