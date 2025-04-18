package com.example.toolweb.chatstream.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * ChatStream流式输出控制器
 */
@Slf4j
@RestController
@RequestMapping("/api/chatstream")
public class ChatStreamController {

    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    /**
     * ChatStream测试接口
     */
    @CrossOrigin("*")
    @GetMapping(value = "/sse", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter streamTest(HttpServletResponse response) {
        log.info("ChatStream连接请求已接收");
        
        // 设置响应头
        response.setHeader("Cache-Control", "no-cache, no-store, max-age=0, must-revalidate");
        response.setHeader("Pragma", "no-cache");
        response.setHeader("X-Accel-Buffering", "no");
        response.setHeader("Access-Control-Allow-Origin", "*");
        
        // 创建SSE发射器
        var emitter = new SseEmitter(180000L); // 3分钟超时
        
        emitter.onCompletion(() -> log.info("ChatStream连接完成"));
        emitter.onTimeout(() -> log.info("ChatStream连接超时"));
        emitter.onError(ex -> log.error("ChatStream连接发生错误: {}", ex.getMessage()));
        
        // 异步处理，避免阻塞主线程
        executor.execute(() -> {
            try {
                log.info("开始发送ChatStream事件");
                // 发送连接成功事件
                emitter.send(SseEmitter.event()
                        .name("connected")
                        .data("连接已建立!"));
                
                Thread.sleep(1000);
                
                // 步骤1：查看现有的BI报表
                // 先发送标题
                emitter.send(SseEmitter.event()
                        .name("step-title")
                        .data(createStepTitle(1, "查看现有的BI报表，匹配符合条件")));
                
                // 延迟100ms后发送内容
                Thread.sleep(1000);
                emitter.send(SseEmitter.event()
                        .name("content")
                        .data(createStep1Content()));
                
                Thread.sleep(1500);
                
                // 步骤2：尝试查找数据集
                // 先发送标题
                emitter.send(SseEmitter.event()
                        .name("step-title")
                        .data(createStepTitle(2, "尝试通过SQL数据集查询，找到合适的数据集")));
                
                // 延迟100ms后发送内容
                Thread.sleep(1000);
                emitter.send(SseEmitter.event()
                        .name("content")
                        .data(createStep2Content()));
                
                Thread.sleep(1500);
                
                // 步骤3：查看报表元数据
                // 先发送标题
                emitter.send(SseEmitter.event()
                        .name("step-title")
                        .data(createStepTitle(3, "查看数据集元数据")));
                
                // 延迟100ms后发送内容
                Thread.sleep(1000);
                emitter.send(SseEmitter.event()
                        .name("content")
                        .data(createStep3Content()));
                
                Thread.sleep(1500);
                
                // 步骤4：生成SQL
                // 先发送标题
                emitter.send(SseEmitter.event()
                        .name("step-title")
                        .data(createStepTitle(4, "根据元数据和需求生成SQL")));
                
                // 延迟100ms后发送内容
                Thread.sleep(1000);
                emitter.send(SseEmitter.event()
                        .name("content")
                        .data(createStep4Content()));
                
                Thread.sleep(1500);
                
                // 步骤5：查询数据结果
                // 先发送标题
                emitter.send(SseEmitter.event()
                        .name("step-title")
                        .data(createStepTitle(5, "根据SQL查询数据")));
                
                // 延迟100ms后发送内容
                Thread.sleep(1000);
                emitter.send(SseEmitter.event()
                        .name("content")
                        .data(createStep5Content()));
                
                Thread.sleep(1500);
                
                // 步骤6：生成图表
                // 先发送标题
                emitter.send(SseEmitter.event()
                        .name("step-title")
                        .data(createStepTitle(6, "今日生产线进度与质量对比图")));
                
                // 延迟100ms后发送内容
                Thread.sleep(1000);
                emitter.send(SseEmitter.event()
                        .name("content")
                        .data(createStep6Content()));
                
                Thread.sleep(1000);
                
                // 发送完成事件
                emitter.send(SseEmitter.event()
                        .name("completed")
                        .data("数据传输完成!"));
                
                // 完成并关闭流
                emitter.complete();
                
            } catch (IOException e) {
                log.error("发送消息发生错误", e);
                emitter.completeWithError(e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("消息发送被中断", e);
                emitter.completeWithError(e);
            }
        });
        
        return emitter;
    }
    
    /**
     * 创建步骤标题数据
     */
    private String createStepTitle(int stepNumber, String description) {
        return "{" +
                "\"type\": \"step-title\"," +
                "\"title\": \"步骤" + stepNumber + "：" + description + "\"," +
                "\"step\": " + stepNumber +
                "}";
    }
    
    private String createStep1Content() {
        return "{" +
                "\"type\": \"text\"," +
                "\"title\": \"步骤1：查看现有的BI报表，匹配符合条件\"," +
                "\"step\": 1," +
                "\"content\": \"为了查看今天的生产进度情况，我首先检查了已有的BI报表库，查找是否有现成的报表可以直接使用。\\n\\n经过筛选和分析，发现现有报表中没有完全匹配的报表可以直接展示今日的生产进度详情。现有的报表大多关注月度或季度的生产汇总数据，缺乏日度维度的实时生产进度跟踪。因此需要进一步查询底层数据集。\"" +
                "}";
    }
    
    private String createStep2Content() {
        return "{" +
                "\"type\": \"text\"," +
                "\"title\": \"步骤2：尝试通过SQL数据集查询，找到合适的数据集\"," +
                "\"step\": 2," +
                "\"content\": \"| 数据集名称 | 描述 | 更新频率 | 是否符合需求 |\\n|------------|------------|------------|--------------|\\n| production_daily_summary | 日生产数据汇总表 | 每小时更新 | ✓ 符合 |\\n| production_hourly_metrics | 生产线小时指标表 | 实时更新 | ✓ 符合 |\\n| production_quality_metrics | 生产质量指标表 | 每小时更新 | ✓ 符合 |\\n| production_monthly_target | 生产月度目标表 | 每月更新 | × 更新频率不足 |\\n| production_equipment_status | 生产设备状态表 | 实时更新 | × 缺少产量数据 |\"" +
                "}";
    }
    
    private String createStep3Content() {
        return "{" +
                "\"type\": \"text\"," +
                "\"title\": \"步骤3：查看数据集元数据\"," +
                "\"step\": 3," +
                "\"content\": \"### 1. production_daily_summary (日生产数据汇总表)\\n\\n| 字段名 | 字段类型 | 字段描述 |\\n|------------|------------|------------|\\n| record_date | DATE | 记录日期 |\\n| production_line_id | VARCHAR | 生产线ID |\\n| production_line_name | VARCHAR | 生产线名称 |\\n| product_category | VARCHAR | 产品类别 |\\n| target_quantity | INT | 目标产量 |\\n| actual_quantity | INT | 实际产量 |\\n| completion_rate | DECIMAL | 完成率 |\\n| update_time | TIMESTAMP | 更新时间 |\\n\\n### 2. production_hourly_metrics (生产线小时指标表)\\n\\n| 字段名 | 字段类型 | 字段描述 |\\n|------------|------------|------------|\\n| record_time | TIMESTAMP | 记录时间 |\\n| production_line_id | VARCHAR | 生产线ID |\\n| hourly_output | INT | 小时产量 |\\n\\n### 3. production_quality_metrics (生产质量指标表)\\n\\n| 字段名 | 字段类型 | 字段描述 |\\n|------------|------------|------------|\\n| record_date | DATE | 记录日期 |\\n| production_line_id | VARCHAR | 生产线ID |\\n| qualified_rate | DECIMAL | 合格率 |\\n| defect_count | INT | 缺陷数量 |\\n\\n通过这三个表的联合查询，可以获取今日各生产线的实时产量、完成率和质量数据。\"" +
                "}";
    }
    
    private String createStep4Content() {
        return "{" +
                "\"type\": \"sql\"," +
                "\"title\": \"步骤4：根据元数据和需求生成SQL\"," +
                "\"step\": 4," +
                "\"content\": \"-- 查询今日各生产线的生产进度情况\\nSELECT\\n    pds.production_line_name,\\n    pds.product_category,\\n    pds.target_quantity,\\n    pds.actual_quantity,\\n    ROUND(pds.actual_quantity / pds.target_quantity * 100, 2) AS completion_percentage,\\n    pqm.qualified_rate * 100 AS quality_rate,\\n    (SELECT SUM(hourly_output) \\n     FROM production_hourly_metrics phm \\n     WHERE phm.record_time >= CONCAT(CURRENT_DATE, ' 00:00:00')\\n     AND phm.record_time <= NOW()\\n     AND phm.production_line_id = pds.production_line_id) AS total_hourly_output\\nFROM\\n    production_daily_summary pds\\nLEFT JOIN\\n    production_quality_metrics pqm\\nON\\n    pds.record_date = pqm.record_date\\n    AND pds.production_line_id = pqm.production_line_id\\nWHERE\\n    pds.record_date = CURRENT_DATE\\nORDER BY\\n    completion_percentage DESC;\"" +
                "}";
    }
    
    private String createStep5Content() {
        return "{" +
                "\"type\": \"text\"," +
                "\"title\": \"步骤5：根据SQL查询数据\"," +
                "\"step\": 5," +
                "\"content\": \"| 生产线名称 | 产品类别 | 目标产量 | 实际产量 | 完成率(%) | 质量合格率(%) | 当日累计产出 |\\n|------------|------------|------------|------------|------------|--------------|--------------|\\n| A线 - 高速组装 | 电子元件 | 12000 | 10560 | 88.00 | 99.12 | 10560 |\\n| C线 - 精密加工 | 机械零件 | 5000 | 4250 | 85.00 | 98.76 | 4250 |\\n| B线 - 自动化包装 | 消费电子 | 8000 | 6400 | 80.00 | 99.54 | 6400 |\\n| D线 - 测试验证 | 通信设备 | 3000 | 2250 | 75.00 | 99.87 | 2250 |\\n| E线 - 定制生产 | 医疗器械 | 1500 | 975 | 65.00 | 99.92 | 975 |\"" +
                "}";
    }
    
    private String createStep6Content() {
        return "{" +
                "\"type\": \"chart\"," +
                "\"title\": \"步骤6：今日生产线进度与质量对比图\"," +
                "\"step\": 6," +
                "\"chartType\": \"bar\"," +
                "\"data\": {" +
                "  \"xAxis\": [\"A线 - 高速组装\", \"B线 - 自动化包装\", \"C线 - 精密加工\", \"D线 - 测试验证\", \"E线 - 定制生产\"]," +
                "  \"series\": [" +
                "    {\"name\": \"完成率(%)\", \"data\": [88, 80, 85, 75, 65], \"color\": \"#5470c6\"}," +
                "    {\"name\": \"质量合格率(%)\", \"data\": [99.12, 99.54, 98.76, 99.87, 99.92], \"color\": \"#91cc75\"}" +
                "  ]" +
                "}" +
                "}";
    }

    /**
     * 简单的测试接口
     */
    @CrossOrigin("*")
    @GetMapping("/hello")
    public String hello() {
        return "Hello, ChatStream!";
    }

    /**
     * ChatAI字符流式输出API
     */
    @CrossOrigin("*")
    @GetMapping(value = "/chatai", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter streamChatAI(HttpServletResponse response) {
        log.info("ChatAI字符流式连接请求已接收");
        
        // 设置响应头
        response.setHeader("Cache-Control", "no-cache, no-store, max-age=0, must-revalidate");
        response.setHeader("Pragma", "no-cache");
        response.setHeader("X-Accel-Buffering", "no");
        response.setHeader("Access-Control-Allow-Origin", "*");
        
        // 创建SSE发射器
        var emitter = new SseEmitter(180000L); // 3分钟超时
        
        emitter.onCompletion(() -> log.info("ChatAI连接完成"));
        emitter.onTimeout(() -> log.info("ChatAI连接超时"));
        emitter.onError(ex -> log.error("ChatAI连接发生错误: {}", ex.getMessage()));
        
        // 异步处理，避免阻塞主线程
        executor.execute(() -> {
            try {
                log.info("开始发送ChatAI字符流事件");
                // 发送连接成功事件
                emitter.send(SseEmitter.event()
                        .name("connected")
                        .data("连接已建立!"));
                
                Thread.sleep(500);
                
                // 模拟回复内容
                String fullResponse = "您好！我是AI助手。非常高兴能够为您提供帮助。请问有什么问题我可以解答的吗？我可以回答关于技术、科学、文学、历史等多方面的问题。如果您需要编写代码、解决数学问题或者获取一些常识性的信息，都可以向我咨询。";
                
                // 字符流式发送
                for (int i = 0; i < fullResponse.length(); i++) {
                    // 发送单个字符
                    emitter.send(SseEmitter.event()
                            .name("token")
                            .data(String.valueOf(fullResponse.charAt(i))));
                    
                    // 模拟打字速度，随机延迟30-100毫秒
                    Thread.sleep((long) (Math.random() * 70) + 30);
                }
                
                Thread.sleep(500);
                
                // 发送完成事件
                emitter.send(SseEmitter.event()
                        .name("completed")
                        .data("回复已完成"));
                
                // 完成并关闭流
                emitter.complete();
                
            } catch (IOException e) {
                log.error("发送消息发生错误", e);
                emitter.completeWithError(e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("消息发送被中断", e);
                emitter.completeWithError(e);
            }
        });
        
        return emitter;
    }
} 