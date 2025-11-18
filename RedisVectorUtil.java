package com.example.demo;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.commands.ProtocolCommand;
import redis.clients.jedis.exceptions.JedisException;

/**
 * Redis向量工具类
 * 提供基于Redis的向量存储和相似度搜索功能
 *
 * @author tangzq
 */
public class RedisVectorUtil {

    private static final JedisPool jedisPool;
    private static final Charset charset = StandardCharsets.UTF_8;

    static {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(30);
        poolConfig.setMaxIdle(15);
        poolConfig.setMinIdle(5);
        poolConfig.setTestOnBorrow(true);
        poolConfig.setTestWhileIdle(true);

        String redisHost = "127.0.0.1";
        int redisPort = 16379;
        int timeout = 3000;
        String redisPassword = null;
        int database = 2;

        jedisPool = new JedisPool(poolConfig, redisHost, redisPort, timeout, redisPassword, database);
    }

    /**
     * 执行原始Redis命令
     *
     * @param command 命令名称
     * @param args 命令参数字节数组
     * @param <T> 返回类型
     * @return 命令执行结果
     */
    @SuppressWarnings("unchecked")
    private static <T> T executeRawCommand(String command, byte[][] args) {
        try (Jedis jedis = jedisPool.getResource()) {
            ProtocolCommand customCommand = new ProtocolCommand() {
                @Override
                public byte[] getRaw() {
                    return command.getBytes(charset);
                }
            };

            Object rawResult = jedis.sendCommand(customCommand, args);
            return (T) rawResult;

        } catch (JedisException e) {
            throw new RuntimeException("执行 Redis 命令失败：command=" + command, e);
        }
    }

    /**
     * 向向量索引中添加向量元素
     *
     * @param key 向量索引的键名
     * @param dim 向量维度
     * @param vector 向量数据数组
     * @param element 元素标识符
     * @return 成功添加返回1，失败返回0
     */
    public static Long vAdd(String key, int dim, float[] vector, String element) {
        return vAdd(key, dim, vector, element, null, null, null, null, null, null);
    }

    /**
     * 向向量索引中添加向量元素
     *
     * @param key 向量索引的键名
     * @param dim 向量维度
     * @param vector 向量数据数组
     * @param element 元素标识符
     * @param reduceDim 降维后的维度大小
     * @param quantType 量化类型：NOQUANT/Q8/BIN
     * @param cas 是否进行CAS检查
     * @param ef 搜索时的ef参数
     * @param attributes 元素属性信息
     * @param m HNSW算法的M参数
     * @return 成功添加返回1，失败返回0
     */
    public static Long vAdd(String key, int dim, float[] vector, String element, Integer reduceDim, String quantType,
            Boolean cas, Integer ef, String attributes, Integer m) {
        if (key == null || key.isEmpty() || vector == null || vector.length != dim || element == null) {
            throw new IllegalArgumentException(
                    "VADD 必填参数非法：key/vector/element 不可为空，vector长度需等于dim（当前dim=" + dim + ", vector长度=" + (
                            vector == null ? 0 : vector.length) + "）");
        }

        List<byte[]> argsList = new ArrayList<>();
        argsList.add(key.getBytes(charset));

        if (reduceDim != null && reduceDim > 0) {
            argsList.add("REDUCE".getBytes(charset));
            argsList.add(String.valueOf(reduceDim).getBytes(charset));
        }

        argsList.add("VALUES".getBytes(charset));
        argsList.add(String.valueOf(dim).getBytes(charset));

        for (float f : vector) {
            argsList.add(String.valueOf(f).getBytes(charset));
        }

        argsList.add(element.getBytes(charset));

        if ("NOQUANT".equals(quantType) || "Q8".equals(quantType) || "BIN".equals(quantType)) {
            argsList.add(quantType.getBytes(charset));
        }
        if (cas != null && cas) {
            argsList.add("CAS".getBytes(charset));
        }
        if (ef != null && ef > 0) {
            argsList.add("EF".getBytes(charset));
            argsList.add(String.valueOf(ef).getBytes(charset));
        }
        if (attributes != null && !attributes.isEmpty()) {
            argsList.add("SETATTR".getBytes(charset));
            argsList.add(attributes.getBytes(charset));
        }
        if (m != null && m > 0) {
            argsList.add("M".getBytes(charset));
            argsList.add(String.valueOf(m).getBytes(charset));
        }

        byte[][] args = argsList.toArray(new byte[0][]);
        Object result = executeRawCommand("VADD", args);
        return result == null ? 0L : Long.parseLong(result.toString());
    }

    /**
     * 根据元素标识符进行向量相似度搜索
     *
     * @param key 向量索引的键名
     * @param element 要搜索的元素标识符
     * @return 相似元素标识符列表
     */
    public static List<String> vSimByElement(String key, String element) {
        return vSim(key, "ELE", element, null, false, false, 10, null, null, null, null);
    }

    /**
     * 向量相似度搜索
     *
     * @param key 向量索引的键名
     * @param vectorType 向量类型：ELE/VALUES/FP32
     * @param vectorOrElement 向量数据或元素标识符
     * @param filter 过滤条件
     * @param withScores 是否返回相似度分数
     * @param withAttribs 是否返回属性信息
     * @param count 返回结果数量
     * @param epsilon 搜索精度参数
     * @param ef 搜索时的ef参数
     * @param filterEf 过滤时的ef参数
     * @param truth 是否返回真实距离
     * @return 相似元素标识符列表，可能包含分数和属性信息
     */
    public static List<String> vSim(String key, String vectorType, Object vectorOrElement, String filter,
            boolean withScores, boolean withAttribs, Integer count, Float epsilon, Integer ef, Integer filterEf,
            Boolean truth) {
        if (key == null || key.isEmpty() || vectorType == null || vectorOrElement == null) {
            throw new IllegalArgumentException("VSIM 必填参数非法：key/vectorType/vectorOrElement 不可为空");
        }
        if (!"ELE".equals(vectorType) && !"VALUES".equals(vectorType) && !"FP32".equals(vectorType)) {
            throw new IllegalArgumentException("VSIM vectorType 非法：仅支持 ELE/VALUES/FP32（当前值=" + vectorType + "）");
        }

        List<byte[]> argsList = new ArrayList<>();
        argsList.add(key.getBytes(charset));
        argsList.add(vectorType.getBytes(charset));

        if ("VALUES".equals(vectorType)) {
            if (!(vectorOrElement instanceof String)) {
                throw new IllegalArgumentException(
                        "VSIM vectorType=VALUES 时，vectorOrElement 必须为字符串（格式：\"dim val1 val2 ...\"）");
            }
            String valuesStr = (String) vectorOrElement;
            String[] valuesParts = valuesStr.trim().split("\\s+");
            if (valuesParts.length < 1) {
                throw new IllegalArgumentException("VSIM VALUES 格式错误：至少包含维度（当前值=" + valuesStr + "）");
            }

            int dim;
            try {
                dim = Integer.parseInt(valuesParts[0]);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("VSIM VALUES 维度必须为整数（当前值=" + valuesParts[0] + "）", e);
            }

            if (valuesParts.length - 1 != dim) {
                throw new IllegalArgumentException(
                        "VSIM VALUES 向量值数量与维度不匹配（维度=" + dim + "，值数量=" + (valuesParts.length - 1) + "）");
            }

            for (String part : valuesParts) {
                argsList.add(part.getBytes(charset));
            }

        } else if ("FP32".equals(vectorType)) {
            if (!(vectorOrElement instanceof byte[])) {
                throw new IllegalArgumentException("VSIM vectorType=FP32 时，vectorOrElement 必须为 byte[] 二进制流");
            }
            argsList.add((byte[]) vectorOrElement);

        } else if ("ELE".equals(vectorType)) {
            if (!(vectorOrElement instanceof String)) {
                throw new IllegalArgumentException("VSIM vectorType=ELE 时，vectorOrElement 必须为元素标识字符串");
            }
            argsList.add(((String) vectorOrElement).getBytes(charset));
        }

        if (withScores) {
            argsList.add("WITHSCORES".getBytes(charset));
        }
        if (withAttribs) {
            argsList.add("WITHATTRIBS".getBytes(charset));
        }
        if (count != null && count > 0) {
            argsList.add("COUNT".getBytes(charset));
            argsList.add(String.valueOf(count).getBytes(charset));
        }
        if (epsilon != null && epsilon >= 0 && epsilon <= 1) {
            argsList.add("EPSILON".getBytes(charset));
            argsList.add(String.valueOf(epsilon).getBytes(charset));
        }
        if (ef != null && ef > 0) {
            argsList.add("EF".getBytes(charset));
            argsList.add(String.valueOf(ef).getBytes(charset));
        }
        if (filter != null && !filter.isEmpty()) {
            argsList.add("FILTER".getBytes(charset));
            argsList.add(filter.getBytes(charset));
        }
        if (filterEf != null && filterEf > 0) {
            argsList.add("FILTER-EF".getBytes(charset));
            argsList.add(String.valueOf(filterEf).getBytes(charset));
        }
        if (truth != null && truth) {
            argsList.add("TRUTH".getBytes(charset));
        }

        byte[][] args = argsList.toArray(new byte[0][]);
        List<byte[]> rawResult = executeRawCommand("VSIM", args);
        List<String> resultList = new ArrayList<>();
        if (rawResult != null && !rawResult.isEmpty()) {
            for (byte[] bytes : rawResult) {
                resultList.add(new String(bytes, charset));
            }
        }
        return resultList;
    }

    /**
     * 向量相似度搜索（字符串参数）
     *
     * @param key 向量索引的键名
     * @param vectorType 向量类型：ELE/VALUES/FP32
     * @param vectorOrElement 向量数据或元素标识符字符串
     * @param filter 过滤条件
     * @param withScores 是否返回相似度分数
     * @param withAttribs 是否返回属性信息
     * @param count 返回结果数量
     * @param epsilon 搜索精度参数
     * @param ef 搜索时的ef参数
     * @param filterEf 过滤时的ef参数
     * @param truth 是否返回真实距离
     * @return 相似元素标识符列表，可能包含分数和属性信息
     */
    public static List<String> vSim(String key, String vectorType, String vectorOrElement, String filter,
            boolean withScores, boolean withAttribs, Integer count, Float epsilon, Integer ef, Integer filterEf,
            Boolean truth) {
        return vSim(key,
                vectorType,
                (Object) vectorOrElement,
                filter,
                withScores,
                withAttribs,
                count,
                epsilon,
                ef,
                filterEf,
                truth);
    }

    /**
     * 获取向量索引的维度
     *
     * @param key 向量索引的键名
     * @return 向量维度，如果索引不存在返回0
     */
    public static Integer vDim(String key) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("VDIM key 不可为空");
        }
        byte[][] args = new byte[][] { key.getBytes(charset) };
        Object result = executeRawCommand("VDIM", args);
        return result == null ? 0 : Integer.parseInt(result.toString());
    }

    /**
     * 获取向量索引中的元素数量
     *
     * @param key 向量索引的键名
     * @return 元素数量，如果索引不存在返回0
     */
    public static Long vCard(String key) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("VCARD key 不可为空");
        }
        byte[][] args = new byte[][] { key.getBytes(charset) };
        Object result = executeRawCommand("VCARD", args);
        return result == null ? 0L : Long.parseLong(result.toString());
    }

    /**
     * 从向量索引中删除指定元素
     *
     * @param key 向量索引的键名
     * @param element 要删除的元素标识符
     * @return 成功删除返回1，元素不存在返回0
     */
    public static Long vRem(String key, String element) {
        if (key == null || key.isEmpty() || element == null) {
            throw new IllegalArgumentException("VREM key/element 不可为空");
        }
        byte[][] args = new byte[][] { key.getBytes(charset), element.getBytes(charset) };
        Object result = executeRawCommand("VREM", args);
        return result == null ? 0L : Long.parseLong(result.toString());
    }

    /**
     * 检查元素是否存在于向量索引中
     *
     * @param key 向量索引的键名
     * @param element 要检查的元素标识符
     * @return 存在返回true，不存在返回false
     */
    public static Boolean vIsMember(String key, String element) {
        if (key == null || key.isEmpty() || element == null) {
            throw new IllegalArgumentException("VISMEMBER key/element 不可为空");
        }
        byte[][] args = new byte[][] { key.getBytes(charset), element.getBytes(charset) };
        Object result = executeRawCommand("VISMEMBER", args);
        return result != null && "1".equals(result.toString());
    }

    /**
     * 获取指定元素的向量嵌入
     *
     * @param key 向量索引的键名
     * @param element 元素标识符
     * @param raw 是否返回原始字节数据
     * @return 向量嵌入数据列表
     */
    public static List<String> vEmb(String key, String element, boolean raw) {
        if (key == null || key.isEmpty() || element == null) {
            throw new IllegalArgumentException("VEMB key/element 不可为空");
        }

        List<byte[]> argsList = new ArrayList<>();
        argsList.add(key.getBytes(charset));
        argsList.add(element.getBytes(charset));
        if (raw) {
            argsList.add("RAW".getBytes(charset));
        }

        byte[][] args = argsList.toArray(new byte[0][]);
        List<byte[]> rawResult = executeRawCommand("VEMB", args);
        List<String> resultList = new ArrayList<>();
        if (rawResult != null && !rawResult.isEmpty()) {
            for (byte[] bytes : rawResult) {
                resultList.add(new String(bytes, charset));
            }
        }
        return resultList;
    }

    /**
     * 设置元素的属性信息
     *
     * @param key 向量索引的键名
     * @param element 元素标识符
     * @param attributes 属性信息字符串
     * @return 成功设置返回1，失败返回0
     */
    public static Long vSetAttr(String key, String element, String attributes) {
        if (key == null || key.isEmpty() || element == null) {
            throw new IllegalArgumentException("VSETATTR key/element 不可为空");
        }
        String attr = attributes == null ? "" : attributes;

        byte[][] args = new byte[][] { key.getBytes(charset), element.getBytes(charset), attr.getBytes(charset) };
        Object result = executeRawCommand("VSETATTR", args);
        return result == null ? 0L : Long.parseLong(result.toString());
    }

    /**
     * 获取元素的属性信息
     *
     * @param key 向量索引的键名
     * @param element 元素标识符
     * @return 元素的属性信息字符串，如果不存在返回null
     */
    public static String vGetAttr(String key, String element) {
        if (key == null || key.isEmpty() || element == null) {
            throw new IllegalArgumentException("VGETATTR key/element 不可为空");
        }

        byte[][] args = new byte[][] { key.getBytes(charset), element.getBytes(charset) };
        Object result = executeRawCommand("VGETATTR", args);
        if (result instanceof byte[]) {
            return new String((byte[]) result, charset);
        }
        return result == null ? null : result.toString();
    }

    /**
     * 按范围获取向量索引中的元素
     *
     * @param key 向量索引的键名
     * @param start 起始元素标识符
     * @param end 结束元素标识符
     * @param count 返回的最大数量
     * @return 元素标识符列表
     */
    public static List<String> vRange(String key, String start, String end, int count) {
        if (key == null || key.isEmpty() || start == null || end == null) {
            throw new IllegalArgumentException("VRANGE key/start/end 不可为空");
        }

        byte[][] args = new byte[][] { key.getBytes(charset), start.getBytes(charset), end.getBytes(charset),
                String.valueOf(count).getBytes(charset) };
        List<byte[]> rawResult = executeRawCommand("VRANGE", args);
        List<String> resultList = new ArrayList<>();
        if (rawResult != null && !rawResult.isEmpty()) {
            for (byte[] bytes : rawResult) {
                resultList.add(new String(bytes, charset));
            }
        }
        return resultList;
    }

    /**
     * 随机获取向量索引中的元素
     *
     * @param key 向量索引的键名
     * @param count 要获取的元素数量，为0时返回单个随机元素
     * @return 随机元素标识符列表
     */
    public static List<String> vRandMember(String key, int count) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("VRANDMEMBER key 不可为空");
        }

        List<byte[]> argsList = new ArrayList<>();
        argsList.add(key.getBytes(charset));
        if (count != 0) {
            argsList.add(String.valueOf(count).getBytes(charset));
        }

        byte[][] args = argsList.toArray(new byte[0][]);
        Object rawResult = executeRawCommand("VRANDMEMBER", args);
        List<String> resultList = new ArrayList<>();

        if (rawResult instanceof byte[]) {
            resultList.add(new String((byte[]) rawResult, charset));
        } else if (rawResult instanceof List) {
            for (Object obj : (List<?>) rawResult) {
                if (obj instanceof byte[]) {
                    resultList.add(new String((byte[]) obj, charset));
                }
            }
        }
        return resultList;
    }

    /**
     * 获取向量索引的详细信息
     *
     * @param key 向量索引的键名
     * @return 索引信息列表，包含维度、元素数量等统计信息
     */
    public static List<String> vInfo(String key) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("VINFO key 不可为空");
        }

        byte[][] args = new byte[][] { key.getBytes(charset) };
        Object rawResult = executeRawCommand("VINFO", args);
        List<String> resultList = new ArrayList<>();

        if (rawResult instanceof List) {
            for (Object item : (List<?>) rawResult) {
                if (item instanceof byte[]) {
                    resultList.add(new String((byte[]) item, charset));
                } else if (item instanceof Long) {
                    resultList.add(String.valueOf(item));
                } else {
                    resultList.add(item == null ? "null" : item.toString());
                }
            }
        }
        return resultList;
    }

    /**
     * 关闭Redis连接池
     */
    public static void closeJedisPool() {
        if (jedisPool != null && !jedisPool.isClosed()) {
            jedisPool.close();
        }
    }
}
