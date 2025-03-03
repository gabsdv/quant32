
from enum import Enum
from dataclasses import dataclass
import heapq
from typing import List


class Side(Enum):
    BUY = "buy"
    SELL = "sell"

@dataclass
class Order:
    price: float
    quantity: float
    side: Side
    order_id: str
    timestamp: int


@dataclass
class Trade:
    price: float
    quantity: float

class OrderBook():

    def __init__(self):
        self.buyHeap = []
        self.sellHeap = []
        self.orderMap = {}
        self.priceLevels = {}
        heapq.heapify(self.buyHeap)
        heapq.heapify(self.sellHeap)

    def submitOrder(self, order: Order) -> List[Trade]:
        if order.order_id in self.orderMap:
            return []
        if order.quantity <= 0:
            return []
        if order.price <= 0 or not isinstance(order.price, float):
            return []

        quantityNeeded = order.quantity
        ordersMatched = []
        if order.side == Side.BUY:
            while quantityNeeded > 0:
                if len(self.sellHeap) == 0:
                    break
                _, lowest = self.sellHeap[0]
                if lowest.order_id not in self.orderMap:
                    heapq.heappop(self.sellHeap)
                    continue
                if order.price < lowest.price:
                    break
                
                # Making the trade:
                orderMatchQuantity = min(quantityNeeded, lowest.quantity)
                orderMatchPrice = (lowest.price + order.price)/2
                trade = Trade(orderMatchPrice, orderMatchQuantity)
                ordersMatched.append(trade)

                # Updating quantity
                quantityNeeded -= orderMatchQuantity
                lowest.quantity -= orderMatchQuantity
                if lowest.quantity <= 0:
                    heapq.heappop(self.sellHeap)
                    del self.orderMap[lowest.order_id]

                self.priceLevels[lowest.price] = self.priceLevels[lowest.price] - orderMatchQuantity
                if self.priceLevels[lowest.price] <= 0:
                    del self.priceLevels[lowest.price]

            if quantityNeeded <= 0:
                return ordersMatched
            
            order.quantity = quantityNeeded
            heapq.heappush(self.buyHeap, (-order.price, order))
            self.orderMap[order.order_id] = order
            if order.price in self.priceLevels:
                self.priceLevels[order.price] += order.quantity
            else:
                self.priceLevels[order.price] = order.quantity

        else:
            while quantityNeeded > 0:
                if len(self.buyHeap) == 0:
                    break
                _, highest = self.buyHeap[0]
                if highest.order_id not in self.orderMap:
                    heapq.heappop(self.buyHeap)
                    continue
                if order.price > highest.price:
                    break
                
                # Making the trade:
                orderMatchQuantity = min(quantityNeeded, highest.quantity)
                orderMatchPrice = (highest.price + order.price)/2
                trade = Trade(orderMatchPrice, orderMatchQuantity)
                ordersMatched.append(trade)

                # Updating quantity
                quantityNeeded -= orderMatchQuantity
                highest.quantity -= orderMatchQuantity
                if highest.quantity <= 0:
                    heapq.heappop(self.buyHeap)
                    del self.orderMap[highest.order_id]

                self.priceLevels[highest.price] = self.priceLevels[highest.price] - orderMatchQuantity
                if self.priceLevels[highest.price] <= 0:
                    del self.priceLevels[highest.price]

            if quantityNeeded <= 0:
                return ordersMatched
            
            order.quantity = quantityNeeded
            heapq.heappush(self.sellHeap, (order.price, order))
            self.orderMap[order.order_id] = order
            if order.price in self.priceLevels:
                self.priceLevels[order.price] += order.quantity
            else:
                self.priceLevels[order.price] = order.quantity

        return ordersMatched


    def cancelOrder(self, order_id: str) -> None:
        if order_id not in self.orderMap:
            return
        
        order = self.orderMap[order_id]
        
        self.priceLevels[order.price] -= order.quantity
        if self.priceLevels[order.price] <= 0:
            del self.priceLevels[order.price]
        del self.orderMap[order_id]

    def getPriceLevels(self) -> List[float]:
        return list(self.priceLevels.keys())
    
    def getVolumeAtPrice(self, priceLevel: float) -> int:
        return self.priceLevels.get(priceLevel, 0)


