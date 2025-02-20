package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.entity.Blog;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisData;
import com.hmdp.utils.SystemConstants;
import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private CacheClient cacheClient;
    @Override
    public Result queryById(Long id) {
        // 解决缓存穿透
        // Shop shop = cacheClient.queryWithPassThrough(CACHE_SHOP_KEY,id,
                 // Shop.class,this::getById,CACHE_SHOP_TTL,TimeUnit.MINUTES);

        // 互斥锁解决缓存击穿
        Shop shop = queryWithMutex(id);

        // 逻辑过期解决缓存击穿
        // Shop shop = cacheClient.queryWithLogicalExpire(CACHE_SHOP_KEY,id,
                // Shop.class,this::getById,CACHE_SHOP_TTL,TimeUnit.MINUTES);

        if(shop==null){
            return Result.fail("店铺不存在");
        }
        // 互斥锁解决缓存击穿
        return Result.ok(shop);
    }

    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key,"1",LOCK_SHOP_TTL,TimeUnit.MINUTES);
        return BooleanUtil.isTrue(flag);
    }

    private void unLock(String key){
        stringRedisTemplate.delete(key);
    }
//
//    // 缓存穿透
//    public Shop queryWithPassThrough(Long id){
//        String key = CACHE_SHOP_KEY + id;
//        // 1.从redis查询商城缓存
//        String shopJson = stringRedisTemplate.opsForValue().get(key);
//        // 2.判断是否存在
//        if(StrUtil.isNotBlank(shopJson)){
//            // 3.存在，直接返回
//            return JSONUtil.toBean(shopJson, Shop.class);
//        }
//        // 判断命中是否是空值
//        if(shopJson != null){
//            return null;
//        }
//        // 4.不存在，根据id查询数据库
//        Shop shop = getById(id);
//        // 5.不存在，返回错误
//        if(shop==null){
//            // 将空值写入redis
//            stringRedisTemplate.opsForValue().set(key,"", CACHE_NULL_TTL, TimeUnit.MINUTES);
//            return null;
//        }
//        // 6.存在写入redis
//        stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
//        return shop;
//    }
    // 互斥锁实现缓存击穿
    public Shop queryWithMutex(Long id){
        String key = CACHE_SHOP_KEY + id;
        // 1.从redis查询商城缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        // 2.判断是否存在
        if(StrUtil.isNotBlank(shopJson)){
            // 3.存在，直接返回
            return JSONUtil.toBean(shopJson, Shop.class);
        }
        // 判断命中是否是空值
        if(shopJson != null){
            return null;
        }
        // 4.实现缓存重建
        // 4.1实现互斥锁
        String lockKey = LOCK_SHOP_KEY + id;
        Shop shop = null;
        try {
            boolean isLock = tryLock(lockKey);
            // 4.2判断获取是否成功
            if (!isLock) {
                // 4.3失败，则休眠并重试
                Thread.sleep(50);
                return queryWithMutex(id);
            }
            // 4.4成功，根据id查询数据库
            // 再次检查redis中是否存在
            shopJson = stringRedisTemplate.opsForValue().get(key);
            if(StrUtil.isNotBlank(shopJson)){
                return JSONUtil.toBean(shopJson, Shop.class);
            }
            if(shopJson != null){
                return null;
            }
            shop = getById(id);
            // 模拟重建延迟
            //Thread.sleep(200);
            // 5.不存在，返回错误
            if (shop == null) {
                // 将空值写入redis
                stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
                return null;
            }
            // 6.存在写入redis
            stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e){
            throw new RuntimeException();
        }finally {
            // 7.释放互斥锁
            unLock(lockKey);
        }
        // 8. 返回
        return shop;
    }

//    // 自定义线程池
//    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);
//
//    // 逻辑过期解决缓存击穿
//    public Shop queryWithLogicalExpire(Long id){
//        String key = CACHE_SHOP_KEY + id;
//        // 1.从redis查询商城缓存
//        String shopJson = stringRedisTemplate.opsForValue().get(key);
//        // 2.判断是否存在
//        if(StrUtil.isBlank(shopJson)){
//            // 3.不存在直接返回
//            return null;
//        }
//        // 4.命中，需要先把Json反序列化为对象
//        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
//        Shop shop = JSONUtil.toBean((JSONObject)redisData.getData(), Shop.class);
//        LocalDateTime expireTime = redisData.getExpireTime();
//        // 5.判断是否过期
//        if(expireTime.isAfter(LocalDateTime.now())){
//            // 5.1未过期直接返回
//            return shop;
//        }
//        // 5.2已过期，则缓存重建
//        // 6.缓存重建
//        // 6.1获取互斥锁
//        String lockKey = LOCK_SHOP_KEY+id;
//        boolean isLock = tryLock(lockKey);
//        // 6.2判断是否获取成功
//        if(isLock){
//            // 二次检验是否过期
//            shopJson = stringRedisTemplate.opsForValue().get(key);
//            if(StrUtil.isBlank(shopJson)){
//                return null;
//            }
//            redisData = JSONUtil.toBean(shopJson, RedisData.class);
//            shop = JSONUtil.toBean((JSONObject)redisData.getData(), Shop.class);
//            expireTime = redisData.getExpireTime();
//            if(expireTime.isAfter(LocalDateTime.now())){
//                return shop;
//            }
//            // 6.3成功，开启独立线程重建缓存
//            CACHE_REBUILD_EXECUTOR.submit(()->{
//                try {// 重建缓存
//                    this.saveShop2Redis(id, LOCK_SHOP_TTL);
//                } catch (Exception e){
//                    throw new RuntimeException(e);
//                }finally {
//                    //释放锁
//                    unLock(lockKey);
//                }
//            });
//        }
//        // 6.4返回商铺信息
//        // 6.存在写入redis
//        return shop;
//    }
    public void saveShop2Redis(Long id,Long expireSeconds) throws InterruptedException {
        // 1.查询店铺数据
        Shop shop = getById(id);
        // 模拟延迟
        Thread.sleep(200);
        // 2.封装逻辑过期时间
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        // 3. 写入Redis
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY+id,JSONUtil.toJsonStr(redisData));
    }
    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if(id == null){
            return Result.fail("店铺id不能为空");
        }
        // 1.更新数据库
        updateById(shop);
        // 2.删除缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY + id);
        return Result.ok();
    }

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        // 1.是否需要根据坐标查询
        if(x==null||y==null){
            // 不需要坐标查询，按数据库查询
            Page<Shop> page = query().eq("type_id", typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            return Result.ok(page.getRecords());
        }
        String key = SHOP_GEO_KEY+typeId;
        // 2.计算分页参数
        int from = (current - 1)*SystemConstants.DEFAULT_PAGE_SIZE;
        int end = current*SystemConstants.DEFAULT_PAGE_SIZE;
        // 3.查询redis,按照距离排序,分页,结果：shopId,distance
        GeoResults<RedisGeoCommands.GeoLocation<String>> radius
                = stringRedisTemplate.opsForGeo().radius(key,new Circle(x,y,5000),
                RedisGeoCommands.GeoRadiusCommandArgs.newGeoRadiusArgs().includeDistance().limit(end));
        // 4.解析出id
        if(radius == null){
            return Result.ok(Collections.EMPTY_LIST);
        }
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = radius.getContent();
        if(list.size()<=from){
            //最后一页了
            return Result.ok(Collections.EMPTY_LIST);
        }
        List<Long> ids = new ArrayList<>(list.size());
        Map<String,Distance> distanceMap = new HashMap<>(list.size());
        // 4.1截取from-end的部分
        list.stream().skip(from).forEach(result -> {
            // 4.2获取id
            String shopIdStr = result.getContent().getName();
            ids.add(Long.valueOf(shopIdStr));
            // 4.3获取距离
            Distance distance = result.getDistance();
            distanceMap.put(shopIdStr,distance);
        });
        // 5.根据id查询shop
        String idStr = StrUtil.join(",", ids);
        List<Shop> shops = query().in("id", ids).last("ORDER BY FIELD(id," + idStr + ")").list();
        for (Shop shop : shops) {
            shop.setDistance(distanceMap.get(shop.getId().toString()).getValue());
        }
        // 6.返回
        return Result.ok(shops);
    }
}
