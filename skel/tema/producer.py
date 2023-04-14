"""
Producer
"""

from threading import Thread
from time import sleep


class Producer(Thread):
    """
    Producer
    """

    def __init__(self, products, marketplace, republish_wait_time, **kwargs):
        super().__init__(**kwargs)
        self.prod_data = products
        self.market_place = marketplace
        self.wait_time = republish_wait_time

    def run(self):
        """
        Run Producer.

        Returns:
            Producers.
        """
        p_id = self.market_place.register_producer()

        while True:
            for prod, qty, prod_time in self.prod_data:
                sleep(prod_time)

                while not all(self.market_place.publish(p_id, prod) for _ in range(qty)):
                    sleep(self.wait_time)
