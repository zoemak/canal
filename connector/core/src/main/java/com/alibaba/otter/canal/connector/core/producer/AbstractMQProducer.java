package com.alibaba.otter.canal.connector.core.producer;

import com.alibaba.otter.canal.common.utils.NamedThreadFactory;
import com.alibaba.otter.canal.connector.core.config.CanalConstants;
import com.alibaba.otter.canal.connector.core.config.MQProperties;
import com.alibaba.otter.canal.connector.core.spi.CanalMQProducer;
import org.apache.commons.lang.StringUtils;

import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * MQ producer 抽象类
 *
 * @author rewerma 2020-01-27
 * @version 1.0.0
 */
public abstract class AbstractMQProducer implements CanalMQProducer {

    protected MQProperties       mqProperties;

    protected ThreadPoolExecutor sendExecutor;
    protected ThreadPoolExecutor buildExecutor;

    @Override
    public void init(Properties properties) {
        // parse canal mq properties
        loadCanalMqProperties(properties);

        int parallelBuildThreadSize = mqProperties.getParallelBuildThreadSize();
        buildExecutor = new ThreadPoolExecutor(parallelBuildThreadSize,
                parallelBuildThreadSize,
                0,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<Runnable>(parallelBuildThreadSize * 2),
                new NamedThreadFactory("MQ-Parallel-Builder"),
                new ThreadPoolExecutor.CallerRunsPolicy());

        int parallelSendThreadSize = mqProperties.getParallelSendThreadSize();
        sendExecutor = new ThreadPoolExecutor(parallelSendThreadSize,
                parallelSendThreadSize,
                0,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<Runnable>(parallelSendThreadSize * 2),
                new NamedThreadFactory("MQ-Parallel-Sender"),
                new ThreadPoolExecutor.CallerRunsPolicy());
    }

    @Override
    public MQProperties getMqProperties() {
        return this.mqProperties;
    }

    @Override
    public void stop() {
        if (buildExecutor != null) {
            buildExecutor.shutdownNow();
        }

        if (sendExecutor != null) {
            sendExecutor.shutdownNow();
        }
    }

    /**
     * 初始化配置
     * <p>
     * canal.mq.flat.message = true <br/>
     * canal.mq.database.hash = true <br/>
     * canal.mq.filter.transaction.entry = true <br/>
     * canal.mq.parallel.build.thread.size = 8 <br/>
     * canal.mq.parallel.send.thread.size = 8 <br/>
     * canal.mq.batch.size = 50 <br/>
     * canal.mq.timeout = 100 <br/>
     * canal.mq.access.channel = local <br/>
     * </p>
     *
     * @param properties 总配置对象
     */
    private void loadCanalMqProperties(Properties properties) {
        String flatMessage = getProperty(properties, CanalConstants.CANAL_MQ_FLAT_MESSAGE);
        if (!StringUtils.isEmpty(flatMessage)) {
            mqProperties.setFlatMessage(Boolean.parseBoolean(flatMessage));
        }

        String databaseHash = getProperty(properties, CanalConstants.CANAL_MQ_DATABASE_HASH);
        if (!StringUtils.isEmpty(databaseHash)) {
            mqProperties.setDatabaseHash(Boolean.parseBoolean(databaseHash));
        }
        String filterTranEntry = getProperty(properties, CanalConstants.CANAL_FILTER_TRANSACTION_ENTRY);
        if (!StringUtils.isEmpty(filterTranEntry)) {
            mqProperties.setFilterTransactionEntry(Boolean.parseBoolean(filterTranEntry));
        }
        String parallelBuildThreadSize = getProperty(properties, CanalConstants.CANAL_MQ_BUILD_THREAD_SIZE);
        if (!StringUtils.isEmpty(parallelBuildThreadSize)) {
            mqProperties.setParallelBuildThreadSize(Integer.parseInt(parallelBuildThreadSize));
        }
        String parallelSendThreadSize = getProperty(properties, CanalConstants.CANAL_MQ_SEND_THREAD_SIZE);
        if (!StringUtils.isEmpty(parallelSendThreadSize)) {
            mqProperties.setParallelSendThreadSize(Integer.parseInt(parallelSendThreadSize));
        }
        String batchSize = getProperty(properties, CanalConstants.CANAL_MQ_CANAL_BATCH_SIZE);
        if (!StringUtils.isEmpty(batchSize)) {
            mqProperties.setBatchSize(Integer.parseInt(batchSize));
        }
        String timeOut = getProperty(properties, CanalConstants.CANAL_MQ_CANAL_GET_TIMEOUT);
        if (!StringUtils.isEmpty(timeOut)) {
            mqProperties.setFetchTimeout(Integer.parseInt(timeOut));
        }
        String accessChannel = getProperty(properties, CanalConstants.CANAL_MQ_ACCESS_CHANNEL);
        if (!StringUtils.isEmpty(accessChannel)) {
            mqProperties.setAccessChannel(accessChannel);
        }
        String aliyunAccessKey = getProperty(properties, CanalConstants.CANAL_ALIYUN_ACCESS_KEY);
        if (!StringUtils.isEmpty(aliyunAccessKey)) {
            mqProperties.setAliyunAccessKey(aliyunAccessKey);
        }
        String aliyunSecretKey = getProperty(properties, CanalConstants.CANAL_ALIYUN_SECRET_KEY);
        if (!StringUtils.isEmpty(aliyunSecretKey)) {
            mqProperties.setAliyunAccessKey(aliyunSecretKey);
        }
        String aliyunUid = getProperty(properties, CanalConstants.CANAL_ALIYUN_UID);
        if (!StringUtils.isEmpty(aliyunUid)) {
            mqProperties.setAliyunUid(Integer.parseInt(aliyunUid));
        }
    }

    /**
     * 兼容下<=1.1.4的mq配置项
     */
    protected void doMoreCompatibleConvert(String oldKey, String newKey, Properties properties) {
        String value = getProperty(properties, oldKey);
        if (StringUtils.isNotEmpty(value)) {
            properties.setProperty(newKey, value);
        }
    }

    public String getProperty(Properties properties, String key) {
        key = StringUtils.trim(key);
        String value = System.getProperty(key);

        if (value == null) {
            value = System.getenv(key);
        }

        if (value == null) {
            value = properties.getProperty(key);
        }

        value = StringUtils.trim(value);

        log(key, value);

        return value;
    }

    public void log(String key, String value) {
    }
}
