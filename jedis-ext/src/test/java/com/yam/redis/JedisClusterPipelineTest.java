/**
 * Copyright: Copyright (c) 2015
 *
 * @author youaremoon
 * @date 2016年6月25日
 * @version V1.0
 */
package com.yam.redis;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

/**
 * @author youaremoon
 * @Description: TODO
 * @date 2016年6月25日 上午1:01:21
 */
public class JedisClusterPipelineTest {

    @Test
    public void test() {
        Set<HostAndPort> nodes = new HashSet<>();
        nodes.add(new HostAndPort("127.0.0.1", 7000));
        nodes.add(new HostAndPort("127.0.0.1", 7001));
        nodes.add(new HostAndPort("127.0.0.1", 7002));
        nodes.add(new HostAndPort("127.0.0.1", 7003));
        nodes.add(new HostAndPort("127.0.0.1", 7004));
        nodes.add(new HostAndPort("127.0.0.1", 7005));

        /*
        构建 JedisCluster 客户端，传入一系列的地址，依次进行服务(slot)发现操作。如果没有异常，将近根据第一个进行发现操作（注意这里是set，
        顺序可能与预期不一致）。
         */
        JedisCluster jc = new JedisCluster(nodes);

        long s = System.currentTimeMillis();

        JedisClusterPipeline jcp = JedisClusterPipeline.pipelined(jc);
        jcp.refreshCluster();
        List<Object> batchResult = null;
        try {
            // batch write
            for (int i = 0; i < 10000; i++) {
                jcp.set("k:" + i, "v1" + i);
                jcp.expire("k:" + i, 1000);
            }
            jcp.sync();

            // batch read
            for (int i = 0; i < 10000; i++) {
                jcp.get("k" + i);
            }
            batchResult = jcp.syncAndReturnAll();
        } finally {
            jcp.close();
        }

        // output time 
        System.out.println("耗时=" + (System.currentTimeMillis() - s));

        System.out.println(batchResult.size());

        // 实际业务代码中，close要在finally中调，这里之所以没这么写，是因为懒
        try {
            jc.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
