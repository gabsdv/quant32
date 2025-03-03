# OrderBook Implementation Exercise

This project provides a base OrderBook class and a comprehensive test suite. Your task is to implement the OrderBook functionality by creating a class that inherits from the base OrderBook class.

## Requirements

Your implementation should:
1. Complete the OrderBook class in orderbook.py
2. Implement all required methods:
   - submitOrder(order: Order) -> List[Trade]
   - cancelOrder(order_id: str) -> None
   - getPriceLevels() -> List[float]
   - getVolumeAtPrice(priceLevel: float) -> int

### Order Handling Rules
- Orders with duplicate order_ids should be ignored (existing order should be preserved)
- Orders with zero or negative quantities should be ignored
- Orders with zero or negative prices should be ignored
- All valid prices must be positive float values

## Testing Your Implementation

1. Create your implementation in orderbook.py
2. Run the tests using pytest: `pytest test_orderbook.py`