import pytest
from orderbook import Order, OrderBook, Side
import time

class TestOrderBook:
    @pytest.fixture
    def orderbook(self):
        return OrderBook()

    def test_submit_order(self, orderbook):
        # Test submitting a single order
        order = Order(
            price=100.0,
            quantity=10.0,
            side=Side.BUY,
            order_id="order1",
            timestamp=int(time.time())
        )
        orderbook.submitOrder(order)
        
        assert orderbook.getVolumeAtPrice(100.0) == 10
        assert orderbook.getPriceLevels() == [100.0]

    def test_submit_multiple_orders_same_price(self, orderbook):
        # Test submitting multiple orders at the same price level
        orders = [
            Order(price=100.0, quantity=10.0, side=Side.BUY, order_id=f"order{i}", 
                  timestamp=int(time.time()))
            for i in range(3)
        ]
        
        for order in orders:
            orderbook.submitOrder(order)
        
        assert orderbook.getVolumeAtPrice(100.0) == 30
        assert orderbook.getPriceLevels() == [100.0]

    def test_submit_multiple_orders_different_prices(self, orderbook):
        # Test submitting orders at different price levels
        orders = [
            Order(price=float(100 + i), quantity=10.0, side=Side.BUY, 
                  order_id=f"order{i}", timestamp=int(time.time()))
            for i in range(3)
        ]
        
        for order in orders:
            orderbook.submitOrder(order)
        
        assert orderbook.getPriceLevels() == [100.0, 101.0, 102.0]
        assert all(orderbook.getVolumeAtPrice(price) == 10 
                  for price in [100.0, 101.0, 102.0])

    def test_cancel_order(self, orderbook):
        # Test canceling an order
        order = Order(
            price=100.0,
            quantity=10.0,
            side=Side.BUY,
            order_id="order1",
            timestamp=int(time.time())
        )
        orderbook.submitOrder(order)
        orderbook.cancelOrder("order1")
        
        assert orderbook.getVolumeAtPrice(100.0) == 0
        assert orderbook.getPriceLevels() == []

    def test_cancel_nonexistent_order(self, orderbook):
        # Test canceling an order that doesn't exist
        orderbook.cancelOrder("nonexistent")  # Should not raise an exception
        assert orderbook.getPriceLevels() == []

    def test_get_volume_at_nonexistent_price(self, orderbook):
        # Test getting volume at a price level that doesn't exist
        assert orderbook.getVolumeAtPrice(999.9) == 0

    def test_mixed_buy_sell_orders(self, orderbook):
        # Test handling both buy and sell orders
        buy_order = Order(
            price=100.0,
            quantity=10.0,
            side=Side.BUY,
            order_id="buy1",
            timestamp=int(time.time())
        )
        sell_order = Order(
            price=101.0,
            quantity=15.0,
            side=Side.SELL,
            order_id="sell1",
            timestamp=int(time.time())
        )
        
        orderbook.submitOrder(buy_order)
        orderbook.submitOrder(sell_order)
        
        assert orderbook.getPriceLevels() == [100.0, 101.0]
        assert orderbook.getVolumeAtPrice(100.0) == 10
        assert orderbook.getVolumeAtPrice(101.0) == 15

    def test_cancel_partial_price_level(self, orderbook):
        # Test canceling one order out of multiple at the same price level
        orders = [
            Order(price=100.0, quantity=10.0, side=Side.BUY, 
                  order_id=f"order{i}", timestamp=int(time.time()))
            for i in range(3)
        ]
        
        for order in orders:
            orderbook.submitOrder(order)
            
        orderbook.cancelOrder("order1")
        
        assert orderbook.getVolumeAtPrice(100.0) == 20
        assert orderbook.getPriceLevels() == [100.0]

    def test_duplicate_order_id(self, orderbook):
        # Test submitting an order with an ID that already exists - should ignore the second order
        order1 = Order(
            price=100.0,
            quantity=10.0,
            side=Side.BUY,
            order_id="duplicate",
            timestamp=int(time.time())
        )
        order2 = Order(
            price=101.0,
            quantity=20.0,
            side=Side.BUY,
            order_id="duplicate",
            timestamp=int(time.time())
        )
        
        orderbook.submitOrder(order1)
        orderbook.submitOrder(order2)  # Should be ignored
        
        # First order should remain unchanged
        assert orderbook.getVolumeAtPrice(100.0) == 10
        assert orderbook.getVolumeAtPrice(101.0) == 0
        assert len(orderbook.getPriceLevels()) == 1
        assert orderbook.getPriceLevels() == [100.0]

    def test_zero_quantity_order(self, orderbook):
        # Test submitting an order with zero quantity - should be ignored
        order = Order(
            price=100.0,
            quantity=0.0,
            side=Side.BUY,
            order_id="zero_qty",
            timestamp=int(time.time())
        )
        orderbook.submitOrder(order)
        
        assert orderbook.getVolumeAtPrice(100.0) == 0
        assert orderbook.getPriceLevels() == []

    def test_negative_quantity_order(self, orderbook):
        # Test submitting an order with negative quantity - should be ignored
        order = Order(
            price=100.0,
            quantity=-10.0,
            side=Side.BUY,
            order_id="neg_qty",
            timestamp=int(time.time())
        )
        orderbook.submitOrder(order)
        
        assert orderbook.getVolumeAtPrice(100.0) == 0
        assert orderbook.getPriceLevels() == []

    def test_negative_price_order(self, orderbook):
        # Test submitting an order with negative price - should be ignored
        order = Order(
            price=-100.0,
            quantity=10.0,
            side=Side.BUY,
            order_id="neg_price",
            timestamp=int(time.time())
        )
        orderbook.submitOrder(order)
        
        assert orderbook.getVolumeAtPrice(-100.0) == 0
        assert orderbook.getPriceLevels() == []

    def test_zero_price_order(self, orderbook):
        # Test submitting an order with zero price - should be ignored
        order = Order(
            price=0.0,
            quantity=10.0,
            side=Side.BUY,
            order_id="zero_price",
            timestamp=int(time.time())
        )
        orderbook.submitOrder(order)
        
        assert orderbook.getVolumeAtPrice(0.0) == 0
        assert orderbook.getPriceLevels() == []

    def test_floating_point_precision(self, orderbook):
        # Test handling of floating point price levels
        order1 = Order(
            price=100.001,
            quantity=10.0,
            side=Side.BUY,
            order_id="precise1",
            timestamp=int(time.time())
        )
        order2 = Order(
            price=100.002,
            quantity=20.0,
            side=Side.BUY,
            order_id="precise2",
            timestamp=int(time.time())
        )
        
        orderbook.submitOrder(order1)
        orderbook.submitOrder(order2)
        
        assert orderbook.getPriceLevels() == [100.001, 100.002]
        assert orderbook.getVolumeAtPrice(100.001) == 10
        assert orderbook.getVolumeAtPrice(100.002) == 20

    def test_no_trades_different_sides(self, orderbook):
        # Test when orders don't match (buy price < sell price)
        buy_order = Order(
            price=100.0,
            quantity=10.0,
            side=Side.BUY,
            order_id="buy1",
            timestamp=int(time.time())
        )
        sell_order = Order(
            price=101.0,
            quantity=10.0,
            side=Side.SELL,
            order_id="sell1",
            timestamp=int(time.time())
        )
        
        trades = orderbook.submitOrder(buy_order)
        assert trades == []
        
        trades = orderbook.submitOrder(sell_order)
        assert trades == []

    def test_single_trade_full_match(self, orderbook):
        # Test when orders match completely
        buy_order = Order(
            price=100.0,
            quantity=10.0,
            side=Side.BUY,
            order_id="buy1",
            timestamp=int(time.time())
        )
        sell_order = Order(
            price=100.0,
            quantity=10.0,
            side=Side.SELL,
            order_id="sell1",
            timestamp=int(time.time())
        )
        
        trades = orderbook.submitOrder(buy_order)
        assert trades == []
        
        trades = orderbook.submitOrder(sell_order)
        assert len(trades) == 1
        assert trades[0].price == 100.0
        assert trades[0].quantity == 10.0

    def test_single_trade_partial_match(self, orderbook):
        # Test when orders match partially
        buy_order = Order(
            price=100.0,
            quantity=15.0,
            side=Side.BUY,
            order_id="buy1",
            timestamp=int(time.time())
        )
        sell_order = Order(
            price=100.0,
            quantity=10.0,
            side=Side.SELL,
            order_id="sell1",
            timestamp=int(time.time())
        )
        
        trades = orderbook.submitOrder(buy_order)
        assert trades == []
        
        trades = orderbook.submitOrder(sell_order)
        assert len(trades) == 1
        assert trades[0].price == 100.0
        assert trades[0].quantity == 10.0
        assert orderbook.getVolumeAtPrice(100.0) == 5  # Remaining buy order quantity

    def test_multiple_trades_different_prices(self, orderbook):
        # Test multiple trades at different price levels
        buy_orders = [
            Order(price=100.0, quantity=10.0, side=Side.BUY, 
                  order_id=f"buy{i}", timestamp=int(time.time()))
            for i in range(2)
        ]
        
        for order in buy_orders:
            trades = orderbook.submitOrder(order)
            assert trades == []
        
        sell_order = Order(
            price=99.0,
            quantity=15.0,
            side=Side.SELL,
            order_id="sell1",
            timestamp=int(time.time())
        )
        
        trades = orderbook.submitOrder(sell_order)
        assert len(trades) == 2
        assert trades[0].price == 100.0
        assert trades[0].quantity == 10.0
        assert trades[1].price == 100.0
        assert trades[1].quantity == 5.0
        assert orderbook.getVolumeAtPrice(100.0) == 5  # Remaining buy order quantity

    def test_no_trades_invalid_orders(self, orderbook):
        # Test that invalid orders don't generate trades
        invalid_orders = [
            Order(price=0.0, quantity=10.0, side=Side.BUY, 
                  order_id="zero_price", timestamp=int(time.time())),
            Order(price=-100.0, quantity=10.0, side=Side.BUY, 
                  order_id="neg_price", timestamp=int(time.time())),
            Order(price=100.0, quantity=0.0, side=Side.BUY, 
                  order_id="zero_qty", timestamp=int(time.time())),
            Order(price=100.0, quantity=-10.0, side=Side.BUY, 
                  order_id="neg_qty", timestamp=int(time.time()))
        ]
        
        for order in invalid_orders:
            trades = orderbook.submitOrder(order)
            assert trades == []
            assert orderbook.getPriceLevels() == []


def main():
    testOrderBook = TestOrderBook()
    orderBook = OrderBook()
    testOrderBook.test_submit_multiple_orders_different_prices(orderBook)

if __name__ == "__main__":
    main()

# tejavk2@illinois.edu
# anikak4@illinois.edu