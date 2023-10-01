package com.inori.modle;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author : KaelvihN
 * @date : 2023/9/29 7:03
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Order {
    private Integer id;
    private String name;
    /**
     * 下单，付款，发货
     */
    private String state;
}
