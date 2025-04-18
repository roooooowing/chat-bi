package tech.rowing.chatbi.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 测试控制器
 */
@Slf4j
@RestController
@RequestMapping("/api")
public class HelloController {

    /**
     * 测试接口
     */
    @GetMapping("/hello")
    public String hello() {
        log.debug("访问hello接口");
        return "Hello, Chat BI!";
    }
}
