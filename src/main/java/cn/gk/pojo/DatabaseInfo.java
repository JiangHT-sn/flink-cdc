package cn.gk.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author：HT
 * @Date：2022/6/27
 * @Description：
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class DatabaseInfo {
    private String database;
    private String table;
}
