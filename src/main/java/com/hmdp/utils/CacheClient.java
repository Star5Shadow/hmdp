package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.*;

@Slf4j
@Component
public class CacheClient {
    private final StringRedisTemplate stringRedisTemplate;

    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }
    public void set(String key, Object value, Long time, TimeUnit unit){
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value),time,unit);
    }
    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit){
        // 设置逻辑过期
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        // 写入Redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    // 缓存穿透
    public <R,ID> R queryWithPassThrough(String keyPrefix, ID id, Class<R> type,
                                         Function<ID,R> dbFallback,Long time, TimeUnit unit){
        String key = keyPrefix + id;
        // 1.从redis查询缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        // 2.判断是否存在
        if(StrUtil.isNotBlank(json)){
            // 3.存在，直接返回
            return JSONUtil.toBean(json, type);
        }
        // 判断命中是否是空值
        if(json != null){
            return null;
        }
        // 4.不存在，根据id查询数据库
        R r = dbFallback.apply(id);
        // 5.不存在，返回错误
        if(r==null){
            // 将空值写入redis
            stringRedisTemplate.opsForValue().set(key,"", CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        // 6.存在写入redis
        this.set(key,r,time,unit);
        return r;
    }
    // 自定义线程池
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    // 逻辑过期解决缓存击穿
    public <R,ID> R queryWithLogicalExpire(String keyPrefix,ID id,Class<R> type,
                                           Function<ID,R> dbFallback,Long time, TimeUnit unit){
        String key = keyPrefix + id;
        // 1.从redis查询缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        // 2.判断是否存在
        if(StrUtil.isBlank(json)){
            // 3.不存在直接返回
            return null;
        }
        // 4.命中，需要先把Json反序列化为对象
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        R r = JSONUtil.toBean((JSONObject)redisData.getData(), type);
        LocalDateTime expireTime = redisData.getExpireTime();
        // 5.判断是否过期
        if(expireTime.isAfter(LocalDateTime.now())){
            // 5.1未过期直接返回
            return r;
        }
        // 5.2已过期，则缓存重建
        // 6.缓存重建
        // 6.1获取互斥锁
        String lockKey = LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(lockKey);
        // 6.2判断是否获取成功
        if(isLock){
            // 二次检验是否过期
            json = stringRedisTemplate.opsForValue().get(key);
            if(StrUtil.isBlank(json)){
                return null;
            }
            redisData = JSONUtil.toBean(json, RedisData.class);
            r = JSONUtil.toBean((JSONObject)redisData.getData(), type);
            expireTime = redisData.getExpireTime();
            if(expireTime.isAfter(LocalDateTime.now())){
                return r;
            }
            // 6.3开启独立线程重建缓存
            CACHE_REBUILD_EXECUTOR.submit(()->{
                try {
                    // 查询数据库
                    R r1 = dbFallback.apply(id);
                    this.setWithLogicalExpire(key,r1,time,unit);
                } catch (Exception e){
                    throw new RuntimeException(e);
                }finally {
                    //释放锁
                    unLock(lockKey);
                }
            });
        }
        // 6.4返回商铺信息
        // 6.存在写入redis
        return r;
    }
    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key,"1",LOCK_SHOP_TTL,TimeUnit.MINUTES);
        return BooleanUtil.isTrue(flag);
    }

    private void unLock(String key){
        stringRedisTemplate.delete(key);
    }

}
