package cn.gk.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author：HT
 * @Date：2022/6/23
 * @Description：
 */
@Data
@AllArgsConstructor
@NoArgsConstructor

public class JsonMapBean {

    private String database;
    private String table;
    private BeforeBean before;
    private AfterBean after;
    private String op;

}
