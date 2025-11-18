package com.example.demo;

import cn.hutool.json.JSONUtil;
import java.util.List;
import java.util.Scanner;

/**
 *  RedisVectorUtilExample
 *
 *  @author tangzq
 */
public class RedisVectorUtilExample {

    // 服务列表
    private static final List<Service> SERVICES = Service.getSERVICES();
    // 键名
    private static final String VECTOR_INDEX_KEY = "services";

    public static void main(String[] args) {
        try {

            // init(); // 初始化

            search("台风");

            Scanner scanner = new Scanner(System.in);

            while (true) {

                System.out.println("输入关键字：");
                String input = scanner.nextLine();
                if (input.equals("quit")) {
                    break;
                }

                search(input);
            }

        } finally {
            RedisVectorUtil.closeJedisPool();
        }
    }

    public static void init() {
        System.out.println("===  初始化 ===");

        for (Service service : SERVICES) {
            try {
                //  服务文本描述向量化
                String serviceText = service.toVecText();
                List<Float> embedding = EmbeddingUtil.getEmbedding(serviceText);

                // 转换为float数组
                float[] vector = new float[embedding.size()];
                for (int i = 0; i < embedding.size(); i++) {
                    vector[i] = embedding.get(i);
                }

                // 添加服务属性信息
                String attributes = JSONUtil.toJsonStr(service);

                // 存储向量索引
                Long result = RedisVectorUtil.vAdd(VECTOR_INDEX_KEY,
                        embedding.size(),
                        vector,
                        service.toElementId(),
                        null,
                        null,
                        false,
                        null,
                        attributes,
                        null);

                if (result == 1L) {
                    System.out.println("添加成功: " + service.name);
                } else {
                    System.out.println("添加失败: " + service.name);
                }

            } catch (Exception e) {
                System.err.println("添加服务异常: " + service.name + ", 错误: " + e.getMessage());
            }
        }

        System.out.println("添加完成\n");
    }

    public static void search(String userQuery) {
        System.out.println("=== 搜索服务 ===");

        try {
            //  向量化
            List<Float> queryEmbedding = EmbeddingUtil.getEmbedding(userQuery);
            float[] queryVector = new float[queryEmbedding.size()];
            for (int i = 0; i < queryEmbedding.size(); i++) {
                queryVector[i] = queryEmbedding.get(i);
            }

            StringBuilder vectorValues = new StringBuilder();
            vectorValues.append(queryVector.length);
            for (float value : queryVector) {
                vectorValues.append(" ").append(value);
            }

            // 相似搜索
            List<String> results = RedisVectorUtil.vSim(VECTOR_INDEX_KEY,
                    "VALUES",
                    vectorValues.toString(),
                    null,
                    true,
                    true,
                    5,
                    0.25f,
                    null,
                    null,
                    false);

            System.out.println("查询: \"" + userQuery + "\" 的搜索结果:");

            for (int i = 0; i < results.size(); i += 3) {
                if (i + 2 < results.size()) {
                    String elementId = results.get(i);
                    String score = results.get(i + 1);
                    String attributes = results.get(i + 2);

                    System.out.println((i / 3 + 1) + ". 服务ID: " + elementId);
                    System.out.println("   相似度: " + score);
                    System.out.println("   属性: " + attributes);
                    System.out.println();
                }
            }

        } catch (Exception e) {
            System.err.println("错误: " + e.getMessage());
        }
    }

}
