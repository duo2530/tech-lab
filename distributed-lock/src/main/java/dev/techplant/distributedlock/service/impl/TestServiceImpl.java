package dev.techplant.distributedlock.service.impl;

import dev.techplant.distributedlock.service.TestService;
import io.micrometer.common.util.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

@Service
public class TestServiceImpl implements TestService {

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Override
    public void testLock() {
        // 查询Redis中的num值
        String value = (String)this.redisTemplate.opsForValue().get("num");
        // 没有该值return
        if (StringUtils.isBlank(value)){
            return ;
        }
        // 有值就转成成int
        int num = Integer.parseInt(value);
        // 把Redis中的num值+1
        this.redisTemplate.opsForValue().set("num", String.valueOf(++num));
    }

    /**
     * 采用SpringDataRedis实现分布式锁
     * 原理：执行业务方法前先尝试获取锁（setnx存入key val），如果获取锁成功再执行业务代码，业务执行完毕后将锁释放(del key)
     */
    @Override
    public void testDistributedLock() {
        //0.先尝试获取锁 setnx key val
        Boolean flag = redisTemplate.opsForValue().setIfAbsent("lock", "lock",3, TimeUnit.SECONDS);
        if(flag){
            //获取锁成功，执行业务代码
            //1.先从redis中通过key num获取值  key提前手动设置 num 初始值：0
            String value = redisTemplate.opsForValue().get("num");
            //2.如果值为空则非法直接返回即可
            if (StringUtils.isBlank(value)) {
                return;
            }
            //3.对num值进行自增加一
            int num = Integer.parseInt(value);
            redisTemplate.opsForValue().set("num", String.valueOf(++num));

            //4.将锁释放
            redisTemplate.delete("lock");

        }else{
            try {
                Thread.sleep(100);
                this.testLock();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}