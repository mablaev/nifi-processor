package hwx.processors.demo.model;

import lombok.*;

import java.time.LocalDate;

@Getter
@Setter
@ToString
@NoArgsConstructor
public class Order {
    private LocalDate tradeDate;
    private String routedOrderId;
    private LocalDate orderDate;
    private String executionId;
    private String lastMarket;
    private String executedQty;
    private String liquidityType;
}
